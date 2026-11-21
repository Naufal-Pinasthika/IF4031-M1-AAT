package gateway

import (
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
