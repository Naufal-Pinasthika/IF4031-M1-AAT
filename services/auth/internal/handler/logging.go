package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/correlation"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
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
		started := time.Now()
		next.ServeHTTP(wrapped, r.WithContext(ctx))
		logger.Info("http_request", "method", r.Method, "path", r.URL.Path, "status", wrapped.status, "correlation_id", correlationID, "latency_ms", time.Since(started).Milliseconds())
	})
}

func Limit(maxInFlight int, next http.Handler) http.Handler {
	slots := make(chan struct{}, maxInFlight)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "server_busy", "too many active requests")
		}
	})
}

func CorrelationID(ctx context.Context) string {
	return correlation.ID(ctx)
}

func newCorrelationID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(value)
}
