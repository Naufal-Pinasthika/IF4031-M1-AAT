package middleware

import (
	cryptoRand "crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"
)

const CorrelationIDHeader = "X-Correlation-ID"

// RequestLogger records structured request logs and preserves correlation IDs.
func RequestLogger(serviceName string) func(http.Handler) http.Handler {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			correlationID := r.Header.Get(CorrelationIDHeader)
			if correlationID == "" {
				correlationID = newCorrelationID()
			}

			w.Header().Set(CorrelationIDHeader, correlationID)
			startedAt := time.Now()
			recorder := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(recorder, r)

			logger.Info(
				"http_request",
				"service", serviceName,
				"correlation_id", correlationID,
				"method", r.Method,
				"path", r.URL.Path,
				"status", recorder.statusCode,
				"latency_ms", time.Since(startedAt).Milliseconds(),
			)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusRecorder) WriteHeader(statusCode int) {
	if w.statusCode != 0 {
		return
	}

	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusRecorder) Write(body []byte) (int, error) {
	if w.statusCode == 0 {
		w.WriteHeader(http.StatusOK)
	}

	return w.ResponseWriter.Write(body)
}

func newCorrelationID() string {
	var rawID [16]byte

	if _, err := cryptoRand.Read(rawID[:]); err == nil {
		return hex.EncodeToString(rawID[:])
	}

	return strconv.FormatInt(time.Now().UnixNano(), 10)
}
