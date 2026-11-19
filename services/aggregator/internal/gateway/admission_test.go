package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestQueueIsBoundedAndTimeoutIsNotBackendFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			t.Error("queued request must not reach upstream")
		}
	}))
	defer server.Close()
	g := testGateway()
	g.queueWait = 10 * time.Millisecond
	host := strings.TrimPrefix(server.URL, "http://")
	g.Refresh(t.Context(), []string{host})
	b := g.nodes[host]
	for range cap(b.regular) {
		b.regular <- struct{}{}
	}
	for _, full := range []bool{false, true} {
		if full {
			for range cap(b.waitRegular) {
				b.waitRegular <- struct{}{}
			}
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest("GET", "/internal/events", nil))
		if w.Code != 429 || w.Header().Get("Retry-After") != "1" || !b.healthy.Load() {
			t.Fatalf("local overload changed status or backend health: %d", w.Code)
		}
	}
	m := &g.classes[0]
	if m.expired.Load() != 1 || m.full.Load() != 1 || m.pending.Load() != 0 || m.active.Load() != 0 {
		t.Fatal("queue accounting incorrect")
	}
}

func TestQueueCancellationReleasesPendingAndKeepsFieldIndependent(t *testing.T) {
	slots, waiters := make(chan struct{}, 1), make(chan struct{}, 1)
	slots <- struct{}{}
	m := &classMetrics{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- acquire(ctx, slots, waiters, time.Second, m) }()
	deadline := time.After(time.Second)
	for m.pending.Load() != 1 {
		select {
		case <-deadline:
			t.Fatal("request never queued")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := acquire(t.Context(), slots, waiters, time.Second, m); !errors.Is(err, errQueueFull) {
		t.Fatal(err)
	}
	field := &classMetrics{}
	if err := acquire(t.Context(), make(chan struct{}, 1), make(chan struct{}, 1), time.Second, field); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if m.pending.Load() != 0 || len(waiters) != 0 || m.canceled.Load() != 1 {
		t.Fatal("canceled request leaked queue capacity")
	}
}

func TestSelectionUsesClassCapacityNotOtherClassActivity(t *testing.T) {
	g := testGateway()
	a, b := g.newBackend("a"), g.newBackend("b")
	a.healthy.Store(true)
	b.healthy.Store(true)
	for range cap(a.regular) {
		a.regular <- struct{}{}
	}
	for range cap(b.field) {
		b.field <- struct{}{}
	}
	g.nodes = map[string]*backend{"a": a, "b": b}
	g.discovered = time.Now()
	for _, field := range []bool{false, true} {
		selected := g.pickClass(&field)
		if (!field && selected != b) || (field && selected != a) {
			t.Fatal("priority class capacity ignored")
		}
		selected.active.Add(-1)
	}
}

func TestDrainingWithdrawsReadinessButKeepsHealthAndMetrics(t *testing.T) {
	g := testGateway()
	g.BeginDrain()
	for path, want := range map[string]int{"/health": 200, "/metrics": 200, "/ready": 503, "/internal/events": 503} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s: %d", path, w.Code)
		}
		if path == "/metrics" && strings.Count(w.Body.String(), "# TYPE gateway_queue_wait_seconds histogram") != 1 {
			t.Fatal("invalid duplicate histogram metadata")
		}
	}
}

func TestConnectionGaugeTracksSocketLifetimeNotRequestCount(t *testing.T) {
	g := testGateway()
	g.ConnectionState(nil, http.StateNew)
	for range 5 {
		g.ConnectionState(nil, http.StateActive)
		g.ConnectionState(nil, http.StateIdle)
	}
	if g.connections.Load() != 1 {
		t.Fatal("keepalive requests changed connection count")
	}
	g.ConnectionState(nil, http.StateClosed)
	g.ConnectionState(nil, http.StateNew)
	g.ConnectionState(nil, http.StateHijacked)
	if g.connections.Load() != 0 {
		t.Fatal("terminal connection states leaked gauge")
	}
}

func TestBodyMetricsAndBudgetAreReleasedExactlyOnce(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("body")) }))
	defer server.Close()
	g := testGateway()
	g.Refresh(t.Context(), []string{strings.TrimPrefix(server.URL, "http://")})
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/events", nil))
	m := &g.classes[0]
	if w.Code != 200 || m.bytes.Load() != 4 || m.body.count.Load() != 1 || m.headers.count.Load() != 1 || m.queue.count.Load() != 1 || m.active.Load() != 0 {
		t.Fatal("response accounting or budget leaked")
	}
}
