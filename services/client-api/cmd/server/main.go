package main

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	client        *http.Client
	logger        *slog.Logger
	aggregatorURL string
	aggregatorKey string
	clientKey     string
	slots         chan struct{}
}

func main() {
	aggregatorURL := os.Getenv("AGGREGATOR_URL")
	aggregatorKey := os.Getenv("AGGREGATOR_API_KEY")
	clientKey := os.Getenv("CLIENT_API_KEY")
	if aggregatorURL == "" || aggregatorKey == "" || clientKey == "" {
		log.Fatal("AGGREGATOR_URL, AGGREGATOR_API_KEY, and CLIENT_API_KEY are required")
	}

	maxInFlight := 64
	if raw := os.Getenv("CLIENT_API_MAX_IN_FLIGHT"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			log.Fatal("CLIENT_API_MAX_IN_FLIGHT must be a positive integer")
		}
		maxInFlight = value
	}

	server := &Server{
		client:        newAggregatorClient(maxInFlight),
		logger:        slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		aggregatorURL: strings.TrimRight(aggregatorURL, "/"),
		aggregatorKey: aggregatorKey,
		clientKey:     clientKey,
		slots:         make(chan struct{}, maxInFlight),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /events", server.protect(func(w http.ResponseWriter, r *http.Request) {
		server.forward(w, r, "/internal/events")
	}))
	mux.HandleFunc("GET /events/{id}", server.protect(func(w http.ResponseWriter, r *http.Request) {
		server.forward(w, r, "/internal/events/"+url.PathEscape(r.PathValue("id")))
	}))
	mux.HandleFunc("GET /source-status", server.protect(func(w http.ResponseWriter, r *http.Request) {
		server.forward(w, r, "/internal/source-status")
	}))

	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Println("Client API server running at :8080")
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func newAggregatorClient(maxInFlight int) *http.Client {
	// keep idle sockets for all active requests
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxInFlight
	transport.MaxIdleConnsPerHost = maxInFlight
	transport.MaxConnsPerHost = maxInFlight
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

func (s *Server) protect(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Client-Key")), []byte(s.clientKey)) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
			next(w, r)
		default:
			// reject fast if all slots busy
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "server busy")
		}
	}
}

func (s *Server) forward(w http.ResponseWriter, r *http.Request, path string) {
	target := s.aggregatorURL + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}
	request.Header.Set("X-Aggregator-Key", s.aggregatorKey)
	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = time.Now().UTC().Format("20060102T150405.000000000Z07:00")
	}
	request.Header.Set("X-Correlation-ID", correlationID)

	startedAt := time.Now()
	response, err := s.client.Do(request)
	if err != nil {
		s.logger.Error("aggregator_request_failed", "error", err, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
		writeError(w, http.StatusServiceUnavailable, "aggregator unavailable")
		return
	}
	defer response.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	if retryAfter := response.Header.Get("Retry-After"); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(response.StatusCode)
	if _, err := io.Copy(w, response.Body); err != nil {
		s.logger.Error("aggregator_response_failed", "error", err, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
		return
	}
	s.logger.Info("aggregator_request_done", "status", response.StatusCode, "correlation_id", correlationID, "latency_ms", time.Since(startedAt).Milliseconds())
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
