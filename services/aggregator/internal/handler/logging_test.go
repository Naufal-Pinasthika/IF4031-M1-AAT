package handler

import (
	metric "github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/metrics"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricRouteDoesNotContainUnboundedIDs(t *testing.T) {
	previous := metric.Default
	metric.Default = metric.New()
	defer func() { metric.Default = previous }()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/events/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := Logging(slog.New(slog.NewTextHandler(io.Discard, nil)), mux)
	for _, path := range []string{"/internal/events/unique-one", "/internal/events/unique-two", "/unknown-unique"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	var output strings.Builder
	metric.Default.WritePrometheus(&output)
	if strings.Contains(output.String(), "unique") || !strings.Contains(output.String(), `path="GET /internal/events/{id}"`) {
		t.Fatalf("unbounded or missing route metric: %s", output.String())
	}
}
