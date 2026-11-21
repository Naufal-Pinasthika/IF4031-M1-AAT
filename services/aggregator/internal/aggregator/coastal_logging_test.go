package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCoastalRequestLogsHTTPAndBodyFailuresExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		short  bool
	}{
		{"success", 200, false},
		{"unavailable", 503, false},
		{"incomplete_body", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.short {
					w.Header().Set("Content-Length", "20")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("{}"))
			}))
			defer server.Close()
			var output bytes.Buffer
			client := NewCoastalClient(time.Second, 3, time.Second, slog.New(slog.NewJSONHandler(&output, nil)))
			_, err := client.get(context.Background(), server.URL, "test-credential", "request-trace", "observations")
			failed := tc.status != 200 || tc.short
			if (err != nil) != failed {
				t.Fatalf("err=%v failed=%v", err, failed)
			}
			lines := strings.Split(strings.TrimSpace(output.String()), "\n")
			if len(lines) != 1 {
				t.Fatalf("expected one request record, got %d", len(lines))
			}
			var row map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
				t.Fatal(err)
			}
			message := "coastal_request_completed"
			if failed {
				message = "coastal_request_failed"
			}
			if row["msg"] != message || row["status"] != float64(tc.status) || row["correlation_id"] != "request-trace" || row["latency_ms"] == nil {
				t.Fatalf("incomplete request record: %v", row)
			}
			if failed && row["error"] == nil {
				t.Fatal("failure reason is missing")
			}
			if strings.Contains(output.String(), "test-credential") {
				t.Fatal("credential leaked into request log")
			}
		})
	}
}
