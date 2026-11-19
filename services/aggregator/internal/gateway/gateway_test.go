package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testGateway() *Gateway { return New(slog.New(slog.NewTextHandler(io.Discard, nil)), "trusted") }
func TestPerRequestSelectionPrefersIdleAndDropsUnready(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()
	g := testGateway()
	a := strings.TrimPrefix(good.URL, "http://")
	b := strings.TrimPrefix(bad.URL, "http://")
	g.Refresh(t.Context(), []string{a, b})
	selected := g.pick()
	if selected == nil || selected.host != a {
		t.Fatal("unhealthy endpoint selected")
	}
	selected.active.Add(-1)
	g.Refresh(t.Context(), nil)
	if g.pick() != nil {
		t.Fatal("removed endpoint retained")
	}
}
func TestProxyPreservesBodyCredentialsAndCorrelationWithoutReplay(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			return
		}
		calls++
		if r.Header.Get("X-Aggregator-Key") != "trusted" || r.Header.Get("X-Correlation-ID") != "same" {
			t.Error("identity headers changed")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "payload" {
			t.Error("body changed")
		}
		w.WriteHeader(429)
		w.Write([]byte("rejected"))
	}))
	defer server.Close()
	g := testGateway()
	g.Refresh(context.Background(), []string{strings.TrimPrefix(server.URL, "http://")})
	req := httptest.NewRequest("POST", "/internal/import/events", strings.NewReader("payload"))
	req.Header.Set("X-Aggregator-Key", "trusted")
	req.Header.Set("X-Correlation-ID", "same")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if calls != 1 || w.Code != 429 || w.Body.String() != "rejected" {
		t.Fatal("status/body changed or rejection retried")
	}
}
func TestGatewayReservesFieldCapacityOnlyForTrustedPriority(t *testing.T) {
	g := testGateway()
	for range cap(g.regularSlots) {
		g.regularSlots <- struct{}{}
	}
	req := httptest.NewRequest("GET", "/internal/events", nil)
	req.Header.Set("X-Request-Priority", "field")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if !strings.Contains(w.Body.String(), "gateway_busy") || w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatal("untrusted priority bypassed reserve")
	}
	req.Header.Set("X-Aggregator-Key", "trusted")
	w = httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if !strings.Contains(w.Body.String(), "no_ready_backend") {
		t.Fatal("trusted Field was stopped by ordinary budget")
	}
	if len(g.slots) != 0 {
		t.Fatal("response budget leaked")
	}
}

func TestLeastActiveSelectionAndDiscoveryExpiry(t *testing.T) {
	g := testGateway()
	a := &backend{host: "a"}
	b := &backend{host: "b"}
	a.healthy.Store(true)
	b.healthy.Store(true)
	a.active.Store(12)
	g.nodes = map[string]*backend{"a": a, "b": b}
	g.discovered = time.Now()
	picked := g.pick()
	if picked != b {
		t.Fatal("busy backend chosen")
	}
	picked.active.Add(-1)
	g.discovered = time.Now().Add(-4 * time.Second)
	if g.pick() != nil {
		t.Fatal("stale health reused after discovery stopped")
	}
}

func TestTransientDiscoveryFailurePreservesOriginalLifetimeNotFailedHealth(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(int(status.Load())) }))
	defer server.Close()
	g := testGateway()
	host := strings.TrimPrefix(server.URL, "http://")
	g.Refresh(t.Context(), []string{host})
	discovered := g.discovered
	g.discoveryFailed(t.Context(), errors.New("temporary DNS failure"))
	if g.discovered != discovered {
		t.Fatal("failure renewed snapshot lifetime")
	}
	b := g.pick()
	if b == nil {
		t.Fatal("transient DNS error discarded a fresh healthy endpoint")
	}
	b.active.Add(-1)
	status.Store(http.StatusServiceUnavailable)
	g.discoveryFailed(t.Context(), errors.New("DNS still unavailable"))
	if g.pick() != nil {
		t.Fatal("retained snapshot bypassed active failed health")
	}
	status.Store(http.StatusOK)
	g.discovered = time.Now().Add(-4 * time.Second)
	g.discoveryFailed(t.Context(), errors.New("expired"))
	if g.pick() != nil {
		t.Fatal("expired discovery was revived by successful probe")
	}
	g.Refresh(t.Context(), []string{host})
	g.Refresh(t.Context(), nil)
	g.discoveryFailed(t.Context(), errors.New("after authoritative empty result"))
	if g.pick() != nil {
		t.Fatal("authoritative removal was undone")
	}
}

func TestUpstreamBudgetHoldsUntilBodyCloseAndProtectsField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("payload")) }))
	defer server.Close()
	g := testGateway()
	if err := g.ConfigureBackendBudget(128, 2); err != nil {
		t.Fatal(err)
	}
	b := g.newBackend(strings.TrimPrefix(server.URL, "http://"))
	b.regular = make(chan struct{}, 1)
	b.field = make(chan struct{}, 1)
	transport := upstreamBudget{g, b}
	ordinary, _ := http.NewRequestWithContext(t.Context(), "GET", server.URL, nil)
	response, err := transport.RoundTrip(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if len(b.regular) != 1 {
		t.Fatal("budget released before response body")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	waiting, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	if _, err = transport.RoundTrip(waiting); err == nil {
		t.Fatal("ordinary request exceeded upstream budget")
	}
	field, _ := http.NewRequestWithContext(t.Context(), "GET", server.URL, nil)
	field.Header.Set("X-Aggregator-Key", "trusted")
	field.Header.Set("X-Request-Priority", "field")
	protected, err := transport.RoundTrip(field)
	if err != nil {
		t.Fatalf("Field waited behind ordinary budget: %v", err)
	}
	protected.Body.Close()
	response.Body.Close()
	if len(b.regular) != 0 || len(b.field) != 0 {
		t.Fatal("upstream budget leaked after close")
	}
}

func TestClientCancellationDoesNotMarkBackendUnhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()
	g := testGateway()
	g.Refresh(t.Context(), []string{strings.TrimPrefix(server.URL, "http://")})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/internal/events", nil).WithContext(ctx)
	g.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable || g.upstreamErrors.Load() != 0 {
		t.Fatal("cancellation was counted as a success or upstream failure")
	}
	for _, b := range g.nodes {
		if !b.healthy.Load() {
			t.Fatal("client cancellation poisoned backend health")
		}
	}
}
