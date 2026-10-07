package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/dashboard-consumer/internal/store"
)

type BrokerProbe interface {
	Probe(context.Context) error
}

type Server struct {
	store         *store.Store
	probe         BrokerProbe
	logger        *slog.Logger
	healthTimeout time.Duration
}

func New(eventStore *store.Store, probe BrokerProbe, logger *slog.Logger, healthTimeout time.Duration) http.Handler {
	server := &Server{store: eventStore, probe: probe, logger: logger, healthTimeout: healthTimeout}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.health)
	mux.HandleFunc("GET /events", server.events)
	mux.HandleFunc("GET /stats", server.stats)
	return server.logging(mux)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.healthTimeout)
	defer cancel()
	started := time.Now()
	err := s.probe.Probe(ctx)
	s.logger.Info("kafka_health_probe", "correlation_id", correlationID(r.Context()), "latency_ms", time.Since(started).Milliseconds(), "error", err)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "dependency": "kafka"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) events(w http.ResponseWriter, _ *http.Request) {
	items, err := s.store.Events()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "read dashboard state"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": items})
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	stats, err := s.store.Stats()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "read dashboard stats"})
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

type correlationKey struct{}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(payload []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(payload)
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if id == "" {
			id = newCorrelationID()
		}
		w.Header().Set("X-Correlation-ID", id)
		wrapped := &statusWriter{ResponseWriter: w}
		started := time.Now()
		next.ServeHTTP(wrapped, r.WithContext(context.WithValue(r.Context(), correlationKey{}, id)))
		s.logger.Info("http_request", "correlation_id", id, "method", r.Method, "path", r.URL.Path, "status", wrapped.status, "latency_ms", time.Since(started).Milliseconds())
	})
}

func correlationID(ctx context.Context) string {
	value, _ := ctx.Value(correlationKey{}).(string)
	return value
}

func newCorrelationID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(value)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
