package handler

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	metric "github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/metrics"
)

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func Logging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := r.Header.Get("X-Correlation-ID")
		if correlationID == "" {
			correlationID = newCorrelationID()
		}
		w.Header().Set("X-Correlation-ID", correlationID)
		ctx := correlation.WithID(r.Context(), correlationID)
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		startedAt := time.Now()
		observed := r.WithContext(ctx)
		next.ServeHTTP(wrapped, observed)
		duration := time.Since(startedAt)
		// Request IDs belong in logs/traces, never in metric labels. Arbitrary
		// /events/{id} and unmatched URLs otherwise grow this map forever.
		route := observed.Pattern
		if route == "" {
			route = "unmatched"
		}
		method := r.Method
		switch method {
		case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS":
		default:
			method = "OTHER"
		}
		metric.Default.HTTP(method, route, wrapped.status, duration)
		logger.Info(
			"http_request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"correlation_id", correlationID,
			"latency_ms", duration.Milliseconds(),
		)
	})
}

func newCorrelationID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(value)
}
