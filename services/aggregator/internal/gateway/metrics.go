package gateway

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

var latencyBounds = [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, time.Second, 5 * time.Second}

// Fixed labels only: no URLs, credentials, correlation IDs or backend IPs.
type latency struct {
	buckets      [len(latencyBounds)]atomic.Uint64
	count, nanos atomic.Uint64
}

func (h *latency) observe(d time.Duration) {
	h.count.Add(1)
	h.nanos.Add(uint64(d))
	for i, bound := range latencyBounds {
		if d <= bound {
			h.buckets[i].Add(1)
		}
	}
}
func (h *latency) write(w io.Writer, name, class string) {
	for i, bound := range latencyBounds {
		fmt.Fprintf(w, "%s_bucket{class=%q,le=%q} %d\n", name, class, fmt.Sprint(bound.Seconds()), h.buckets[i].Load())
	}
	fmt.Fprintf(w, "%s_bucket{class=%q,le=\"+Inf\"} %d\n%s_count{class=%q} %d\n%s_sum{class=%q} %g\n", name, class, h.count.Load(), name, class, h.count.Load(), name, class, float64(h.nanos.Load())/1e9)
}

type classMetrics struct {
	queue, headers, body                   latency
	active, pending                        atomic.Int64
	queued, full, expired, canceled, bytes atomic.Uint64
}
type observedBody struct {
	budgetBody
	metrics *classMetrics
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.metrics.bytes.Add(uint64(n))
	return n, err
}
func (g *Gateway) writeMetrics(w io.Writer) {
	fmt.Fprintf(w, "gateway_connections_open %d\n", g.connections.Load())
	for _, name := range []string{"gateway_queue_wait_seconds", "gateway_upstream_headers_seconds", "gateway_body_lifetime_seconds"} {
		fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	}
	for i, class := range []string{"ordinary", "field"} {
		m := &g.classes[i]
		m.queue.write(w, "gateway_queue_wait_seconds", class)
		m.headers.write(w, "gateway_upstream_headers_seconds", class)
		m.body.write(w, "gateway_body_lifetime_seconds", class)
		fmt.Fprintf(w, "gateway_upstream_active{class=%q} %d\ngateway_queue_pending{class=%q} %d\n", class, m.active.Load(), class, m.pending.Load())
		for _, value := range []struct {
			name  string
			count uint64
		}{
			{"queued", m.queued.Load()}, {"queue_full", m.full.Load()}, {"queue_expired", m.expired.Load()}, {"queue_canceled", m.canceled.Load()}, {"body_bytes", m.bytes.Load()},
		} {
			fmt.Fprintf(w, "gateway_%s_total{class=%q} %d\n", value.name, class, value.count)
		}
	}
}

// HTTP active-response admission does not bound persistent sockets. Keep their
// footprint visible without a per-connection map or a high-cardinality label.
func (g *Gateway) ConnectionState(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		g.connections.Add(1)
	case http.StateClosed, http.StateHijacked:
		g.connections.Add(-1)
	}
}
