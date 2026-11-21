package gateway

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecentProbeKeepsKnownEndpointDuringDNSTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer server.Close()
	g := testGateway()
	g.Refresh(t.Context(), []string{strings.TrimPrefix(server.URL, "http://")})
	g.discovered = time.Now().Add(-4 * time.Second)
	original := g.discovered
	g.discoveryFailed(t.Context(), errors.New("DNS deadline exceeded"))
	r := httptest.NewRequest("GET", "/internal/events", nil)
	r.Header.Set("X-Aggregator-Key", "trusted")
	r.Header.Set("X-Request-Priority", "field")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 200 || g.noBackend.Load() != 0 || g.discovered != original {
		t.Fatalf("fresh probe lost to transient discovery failure: HTTP %d", w.Code)
	}
}

func TestNoBackendRejectionRetainsCorrelationLog(t *testing.T) {
	var logs bytes.Buffer
	g := New(slog.New(slog.NewJSONHandler(&logs, nil)), "trusted")
	r := httptest.NewRequest("GET", "/internal/events", nil)
	r.Header.Set("X-Correlation-ID", "unavailable-field")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 503 || !strings.Contains(logs.String(), `"correlation_id":"unavailable-field"`) ||
		!strings.Contains(logs.String(), `"error":"no_ready_backend"`) {
		t.Fatalf("unavailable routing decision has no correlated failure log: %s", logs.String())
	}
}

func TestExpiredProbeDoesNotUseDiscoveryGrace(t *testing.T) {
	g := testGateway()
	b := g.newBackend("127.0.0.1:1")
	b.healthy.Store(true)
	expired := time.Now().Add(-probeMaxAge - time.Second)
	b.healthy.checked.Store(&expired)
	g.nodes[b.host] = b
	g.discovered = time.Now()
	if g.pick() != nil || b.healthy.Load() {
		t.Fatal("fresh discovery admitted a backend whose probe stopped")
	}
	b.healthy.Store(true)
	if !b.healthy.Load() {
		t.Fatal("new successful probe did not restore eligibility")
	}
	b.healthy.Store(false)
	if b.healthy.Load() {
		t.Fatal("failed probe did not withdraw eligibility immediately")
	}
}
