// Package gateway implements bounded per-request L7 routing, not a cache or a
// second ingestion owner. Upstream authentication and priority stay intact.
package gateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type backend struct {
	host                   string
	active                 atomic.Int64
	healthy                atomic.Bool
	proxy                  *httputil.ReverseProxy
	regular, field         chan struct{}
	waitRegular, waitField chan struct{}
}
type Gateway struct {
	connections                               atomic.Int64
	copyBuffers                               *copyPool
	mu                                        sync.RWMutex
	nodes                                     map[string]*backend
	transport                                 *http.Transport
	probeTransport                            *http.Transport
	backendRegular, backendField              int
	logger                                    *slog.Logger
	slots                                     chan struct{}
	regularSlots                              chan struct{}
	apiKey                                    string
	requests, rejected                        atomic.Uint64
	durationNanos, upstreamErrors             atomic.Uint64
	discoveryErrors, probeFailures, noBackend atomic.Uint64
	discovered                                time.Time
	sequence                                  atomic.Uint64
	draining                                  atomic.Bool
	queueWait                                 time.Duration
	classes                                   [2]classMetrics
}

func New(logger *slog.Logger, keys ...string) *Gateway {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1024
	transport.MaxIdleConnsPerHost = 128
	transport.MaxConnsPerHost = 256
	transport.DisableCompression = true // preserve upstream negotiated wire bytes
	transport.ResponseHeaderTimeout = 5 * time.Second
	probeTransport := transport.Clone()
	probeTransport.MaxConnsPerHost = 2
	probeTransport.MaxIdleConnsPerHost = 2
	g := &Gateway{nodes: map[string]*backend{}, transport: transport, probeTransport: probeTransport, backendRegular: 48, backendField: 16, logger: logger, slots: make(chan struct{}, 1024), regularSlots: make(chan struct{}, 768), queueWait: 100 * time.Millisecond}
	if len(keys) > 0 {
		g.apiKey = keys[0]
	}
	g.copyBuffers = newCopyPool()
	return g
}

// Startup-only budget derived from the backend response capacity and the fixed
// gateway replica count. Bound requests through body consumption, not sockets.
func (g *Gateway) ConfigureBackendBudget(responses, gateways int) error {
	if gateways < 1 || responses < gateways*4 {
		return fmt.Errorf("insufficient per-gateway Field and ordinary backend capacity")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.nodes) > 0 {
		return fmt.Errorf("backend budget is startup-only")
	}
	g.backendField = responses / 4 / gateways
	g.backendRegular = (responses - responses/4) / gateways
	return nil
}
func (g *Gateway) fieldRequest(r *http.Request) bool {
	return r.Header.Get("X-Request-Priority") == "field" && g.apiKey != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Aggregator-Key")), []byte(g.apiKey)) == 1
}

type budgetBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *budgetBody) Close() error { err := b.ReadCloser.Close(); b.once.Do(b.release); return err }

type upstreamBudget struct {
	gateway *Gateway
	backend *backend
}

func (t upstreamBudget) RoundTrip(r *http.Request) (*http.Response, error) {
	slots := t.backend.regular
	waiters := t.backend.waitRegular
	m := &t.gateway.classes[0]
	if t.gateway.fieldRequest(r) {
		slots = t.backend.field
		waiters = t.backend.waitField
		m = &t.gateway.classes[1]
	}
	began := time.Now()
	if err := acquire(r.Context(), slots, waiters, t.gateway.queueWait, m); err != nil {
		m.queue.observe(time.Since(began))
		return nil, err
	}
	m.queue.observe(time.Since(began))
	m.active.Add(1)
	release := func() { <-slots; m.active.Add(-1) }
	began = time.Now()
	response, err := t.gateway.transport.RoundTrip(r)
	m.headers.observe(time.Since(began))
	if err != nil {
		release()
		return nil, err
	}
	bodyBegan := time.Now()
	response.Body = &observedBody{budgetBody: budgetBody{ReadCloser: response.Body, release: func() {
		release()
		m.body.observe(time.Since(bodyBegan))
	}}, metrics: m}
	return response, nil
}

func (g *Gateway) newBackend(host string) *backend {
	b := &backend{host: host, regular: make(chan struct{}, g.backendRegular), field: make(chan struct{}, g.backendField), waitRegular: make(chan struct{}, g.backendRegular), waitField: make(chan struct{}, g.backendField)}
	u, _ := url.Parse("http://" + host)
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.Transport = upstreamBudget{g, b}
	proxy.BufferPool = g.copyBuffers
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, errQueueFull) || errors.Is(err, errQueueTimeout) {
			g.rejected.Add(1)
			w.Header().Set("Retry-After", "1")
			failure(w, http.StatusTooManyRequests, "gateway_queue_busy")
			return // Local overload is NOT a backend health failure.
		}
		if r.Context().Err() != nil {
			failure(w, http.StatusServiceUnavailable, "client_canceled")
			return // Disconnected callers must not poison endpoint health.
		}
		b.healthy.Store(false)
		g.logger.Warn("gateway_upstream_failed", "correlation_id", r.Header.Get("X-Correlation-ID"), "backend", host, "error", err)
		failure(w, http.StatusServiceUnavailable, "backend_unavailable")
	}
	b.proxy = proxy
	return b
}

// Refresh drops DNS-removed endpoints before probing. A failed discovery is
// fail-closed; successful old health must not persist indefinitely.
func (g *Gateway) Refresh(ctx context.Context, hosts []string) {
	if len(hosts) > 32 {
		hosts = nil
	}
	g.mu.Lock()
	next := make(map[string]*backend, len(hosts))
	for _, host := range hosts {
		b := g.nodes[host]
		if b == nil {
			b = g.newBackend(host)
		}
		next[host] = b
	}
	g.nodes = next
	g.discovered = time.Now()
	g.mu.Unlock()
	g.probe(ctx, next)
}

func (g *Gateway) probe(ctx context.Context, nodes map[string]*backend) {
	var group sync.WaitGroup
	// DNS endpoint count and health work are bounded, including surge pods.
	for _, b := range nodes {
		group.Go(func() {
			op, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(op, "GET", "http://"+b.host+"/ready", nil)
			response, err := g.probeTransport.RoundTrip(req)
			healthy := err == nil && response.StatusCode == 200
			if response != nil {
				io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				response.Body.Close()
			}
			if !healthy {
				g.probeFailures.Add(1)
				status := 0
				if response != nil {
					status = response.StatusCode
				}
				g.logger.Warn("gateway_probe_failed", "backend", b.host, "status", status, "error", err)
			}
			b.healthy.Store(healthy)
		})
	}
	group.Wait()
}

// A failed DNS lookup is not an authoritative empty endpoint set. Retain only
// the previous snapshot's ORIGINAL bounded lifetime, while actively probing it.
// This neither renews discovery freshness nor keeps a failed backend eligible.
func (g *Gateway) discoveryFailed(ctx context.Context, err error) {
	g.discoveryErrors.Add(1)
	g.logger.Warn("gateway_discovery_failed", "error", err)
	g.mu.RLock()
	previous := make(map[string]*backend, len(g.nodes))
	for host, b := range g.nodes {
		previous[host] = b
	}
	g.mu.RUnlock()
	g.probe(ctx, previous)
}

func (g *Gateway) Discover(ctx context.Context, name string) {
	for {
		op, cancel := context.WithTimeout(ctx, time.Second)
		ips, err := net.DefaultResolver.LookupHost(op, name)
		cancel()
		hosts := []string{}
		if err == nil && len(ips) <= 32 {
			for _, ip := range ips {
				hosts = append(hosts, net.JoinHostPort(ip, "8083"))
			}
		}
		if err != nil {
			g.discoveryFailed(ctx, err)
		} else {
			g.Refresh(ctx, hosts)
		}
		select {
		case <-ctx.Done():
			g.transport.CloseIdleConnections()
			g.probeTransport.CloseIdleConnections()
			return
		case <-time.After(time.Second):
		}
	}
}

func (g *Gateway) pick() *backend {
	return g.pickClass(nil)
}

// A full ordinary queue must not divert Field traffic or attract more ordinary
// work while another healthy endpoint has class-specific capacity available.
func (g *Gateway) pickClass(field *bool) *backend {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if time.Since(g.discovered) > 3*time.Second {
		return nil
	}
	// Random tie-breaking avoids keeping all idle traffic on the first pod.
	var chosen *backend
	ties := 0
	for _, b := range g.nodes {
		if !b.healthy.Load() {
			continue
		}
		load := func(node *backend) float64 {
			if field == nil {
				return float64(node.active.Load())
			}
			slots, waiters := node.regular, node.waitRegular
			if *field {
				slots, waiters = node.field, node.waitField
			}
			return float64(len(slots)+len(waiters)) / float64(cap(slots))
		}
		if chosen == nil || load(b) < load(chosen) {
			chosen = b
			ties = 1
		} else if load(b) == load(chosen) {
			ties++
			if rand.IntN(ties) == 0 {
				chosen = b
			}
		}
	}
	if chosen != nil {
		chosen.active.Add(1)
	}
	return chosen
}

func failure(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":{"code":%q,"message":%q}}`, message, message)
}

type responseStatus struct {
	http.ResponseWriter
	status int
}

func (w *responseStatus) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *responseStatus) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *responseStatus) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		w.Write([]byte(`{"status":"ok"}`))
		return
	}
	if r.URL.Path == "/ready" {
		if g.draining.Load() {
			failure(w, 503, "gateway_draining")
			return
		}
		b := g.pick()
		if b == nil {
			failure(w, 503, "no_ready_backend")
		} else {
			b.active.Add(-1)
			w.Write([]byte(`{"status":"ready"}`))
		}
		return
	}
	if r.URL.Path == "/metrics" {
		fmt.Fprintf(w, "gateway_requests_total %d\ngateway_rejected_total %d\ngateway_upstream_errors_total %d\ngateway_request_duration_seconds_sum %g\ngateway_request_duration_seconds_count %d\n", g.requests.Load(), g.rejected.Load(), g.upstreamErrors.Load(), float64(g.durationNanos.Load())/1e9, g.requests.Load())
		fmt.Fprintf(w, "gateway_discovery_errors_total %d\ngateway_probe_failures_total %d\ngateway_no_backend_total %d\n", g.discoveryErrors.Load(), g.probeFailures.Load(), g.noBackend.Load())
		g.writeMetrics(w)
		return
	}
	if g.draining.Load() {
		failure(w, 503, "gateway_draining")
		return
	}
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		g.rejected.Add(1)
		w.Header().Set("Retry-After", "1")
		failure(w, 429, "gateway_busy")
		return
	}
	priority := g.fieldRequest(r)
	if !priority {
		select {
		case g.regularSlots <- struct{}{}:
			defer func() { <-g.regularSlots }()
		default:
			g.rejected.Add(1)
			w.Header().Set("Retry-After", "1")
			failure(w, 429, "gateway_busy")
			return
		}
	}
	b := g.pickClass(&priority)
	if b == nil {
		g.rejected.Add(1)
		g.noBackend.Add(1)
		failure(w, 503, "no_ready_backend")
		return
	}
	defer b.active.Add(-1)
	if r.Header.Get("X-Correlation-ID") == "" {
		r.Header.Set("X-Correlation-ID", fmt.Sprintf("gateway-%d-%d", time.Now().UnixNano(), g.sequence.Add(1)))
	}
	w.Header().Set("X-Aggregator-Gateway", "least-active")
	began := time.Now()
	out := &responseStatus{ResponseWriter: w}
	b.proxy.ServeHTTP(out, r)
	g.requests.Add(1)
	g.durationNanos.Add(uint64(time.Since(began)))
	if out.status >= 500 && r.Context().Err() == nil {
		g.upstreamErrors.Add(1)
	}
	g.logger.Info("gateway_request", "correlation_id", r.Header.Get("X-Correlation-ID"), "backend", b.host, "status", out.status, "duration_ms", float64(time.Since(began))/float64(time.Millisecond))
}
