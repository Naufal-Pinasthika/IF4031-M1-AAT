package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadDialFailureRetriesOnceAndRetainsIdentityAndBudgets(t *testing.T) {
	var calls atomic.Int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Correlation-ID") != "failover-id" || r.Header.Get("X-Aggregator-Key") != "trusted" || r.URL.RawQuery != "limit=1" {
			t.Error("retry changed request identity/query")
		}
		w.Write([]byte("survivor"))
	}))
	defer good.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	g := testGateway()
	a := g.newBackend(strings.TrimPrefix(dead.URL, "http://"))
	b := g.newBackend(strings.TrimPrefix(good.URL, "http://"))
	a.healthy.Store(true)
	b.healthy.Store(true)
	g.nodes = map[string]*backend{a.host: a, b.host: b}
	g.discovered = time.Now()
	r, _ := http.NewRequestWithContext(t.Context(), "GET", dead.URL+"/internal/events?limit=1", nil)
	r.Header.Set("X-Correlation-ID", "failover-id")
	r.Header.Set("X-Aggregator-Key", "trusted")
	r.Header.Set("X-Request-Priority", "field")
	response, err := (upstreamBudget{g, a}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	if b.active.Load() != 1 || len(b.field) != 1 {
		t.Fatal("fallback body escaped active-response budget")
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	response.Body.Close()
	if string(body) != "survivor" || calls.Load() != 1 || g.readFailovers.Load() != 1 || a.healthy.Load() {
		t.Fatal("read did not fail over exactly once")
	}
	if b.active.Load() != 0 || len(b.field) != 0 || len(a.field) != 0 {
		t.Fatal("fallback leaked response capacity")
	}
}

func TestDialFailoverNeverReplaysWritesOrGETBodies(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "PATCH", "GET"} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			good := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
			defer good.Close()
			dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			dead.Close()
			g := testGateway()
			a := g.newBackend(strings.TrimPrefix(dead.URL, "http://"))
			b := g.newBackend(strings.TrimPrefix(good.URL, "http://"))
			a.healthy.Store(true)
			b.healthy.Store(true)
			g.nodes = map[string]*backend{a.host: a, b.host: b}
			g.discovered = time.Now()
			r, _ := http.NewRequestWithContext(t.Context(), method, dead.URL+"/internal/events", strings.NewReader("body"))
			if _, err := (upstreamBudget{g, a}).RoundTrip(r); err == nil {
				t.Fatal("dead backend returned success")
			}
			if calls.Load() != 0 || g.readFailovers.Load() != 0 {
				t.Fatal("request with body was replayed")
			}
		})
	}
}

func TestReadFailoverPreservesApplication503WithoutReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	g := testGateway()
	b := g.newBackend(strings.TrimPrefix(server.URL, "http://"))
	r, _ := http.NewRequestWithContext(t.Context(), "GET", server.URL, nil)
	response, err := (upstreamBudget{g, b}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 503 || g.readFailovers.Load() != 0 {
		t.Fatal("application response was hidden/retried")
	}
}

func TestReadHeaderTimeoutFailsOverBeforeClientDeadline(t *testing.T) {
	var calls atomic.Int32
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer silent.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Correlation-ID") != "partition-read" {
			t.Error("lost request correlation during failover")
		}
		w.Write([]byte("survivor"))
	}))
	defer good.Close()
	g := testGateway()
	a := g.newBackend(strings.TrimPrefix(silent.URL, "http://"))
	b := g.newBackend(strings.TrimPrefix(good.URL, "http://"))
	a.healthy.Store(true)
	b.healthy.Store(true)
	g.nodes = map[string]*backend{a.host: a, b.host: b}
	g.discovered = time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 1900*time.Millisecond)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "GET", silent.URL+"/internal/events", nil)
	r.Header.Set("X-Correlation-ID", "partition-read")
	response, err := (upstreamBudget{g, a}).RoundTrip(r)
	if err != nil {
		t.Fatalf("established but silent backend prevented read failover: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(body) != "survivor" || calls.Load() != 1 || g.readFailovers.Load() != 1 || a.healthy.Load() {
		t.Fatalf("unsafe/unsuccessful failover: body=%q err=%v", body, err)
	}
	if b.active.Load() != 0 || len(a.regular) != 0 || len(b.regular) != 0 {
		t.Fatal("header-timeout retry leaked response budgets")
	}
}

func TestReadSurvivesTwoSilentPeersWithoutLeakingBudgets(t *testing.T) {
	g := testGateway()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("survivor")) }))
	defer good.Close()
	c := g.newBackend(strings.TrimPrefix(good.URL, "http://"))
	c.healthy.Store(true)
	silent := func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
	first := httptest.NewServer(http.HandlerFunc(silent))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.nodes[c.host] = c
		g.mu.Unlock()
		silent(w, r)
	}))
	defer second.Close()
	a := g.newBackend(strings.TrimPrefix(first.URL, "http://"))
	b := g.newBackend(strings.TrimPrefix(second.URL, "http://"))
	a.healthy.Store(true)
	b.healthy.Store(true)
	g.nodes = map[string]*backend{a.host: a, b.host: b}
	g.discovered = time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "GET", first.URL+"/internal/events", nil)
	response, err := (upstreamBudget{g, a}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(body) != "survivor" || g.readFailovers.Load() != 2 {
		t.Fatalf("third reachable peer was not used: %q %v", body, err)
	}
	for _, node := range []*backend{a, b, c} {
		if node.active.Load() != 0 || len(node.regular) != 0 {
			t.Fatal("safe-read failover leaked active-response capacity")
		}
	}
}
