package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsCountResponsesWithoutURLLabels(t *testing.T) {
	h := Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }), false)
	for i := 0; i < 100; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/arbitrary-secret-id", nil))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	s := w.Body.String()
	if !strings.Contains(s, "aat_http_requests_total{status=\"429\"} 100") || strings.Contains(s, "arbitrary") {
		t.Fatal(s)
	}
}
