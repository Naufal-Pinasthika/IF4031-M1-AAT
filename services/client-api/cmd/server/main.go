package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxAggregatorResponseBytes = 16 << 20

type contextKey string

const (
	claimsContextKey      contextKey = "access_claims"
	correlationContextKey contextKey = "correlation_id"
)

type Server struct {
	client        *http.Client
	logger        *slog.Logger
	aggregatorURL string
	aggregatorKey string
	verifier      *JWTVerifier
	slots         chan struct{}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "client-api")
	aggregatorURL := os.Getenv("AGGREGATOR_URL")
	aggregatorKey := os.Getenv("AGGREGATOR_API_KEY")
	jwksURL := os.Getenv("AUTH_JWKS_URL")
	issuer := envOrDefault("AUTH_JWT_ISSUER", "bnpb-auth-service")
	audience := envOrDefault("AUTH_JWT_AUDIENCE", "bnpb-client-api")
	if aggregatorURL == "" || aggregatorKey == "" || jwksURL == "" {
		logger.Error("configuration_invalid", "error", "AGGREGATOR_URL, AGGREGATOR_API_KEY, and AUTH_JWKS_URL are required")
		os.Exit(1)
	}

	maxInFlight := 64
	if raw := os.Getenv("CLIENT_API_MAX_IN_FLIGHT"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			logger.Error("configuration_invalid", "error", "CLIENT_API_MAX_IN_FLIGHT must be a positive integer")
			os.Exit(1)
		}
		maxInFlight = value
	}

	verifier, err := NewJWTVerifier(context.Background(), jwksURL, issuer, audience, logger)
	if err != nil {
		logger.Error("jwt_verifier_initialization_failed", "error", err)
		os.Exit(1)
	}
	server := &Server{
		client:        newAggregatorClient(maxInFlight),
		logger:        logger,
		aggregatorURL: strings.TrimRight(aggregatorURL, "/"),
		aggregatorKey: aggregatorKey,
		verifier:      verifier,
		slots:         make(chan struct{}, maxInFlight),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /events", server.protect(func(w http.ResponseWriter, r *http.Request) {
		server.forwardEvents(w, r, "/internal/events", false)
	}))
	mux.HandleFunc("GET /events/{id}", server.protect(func(w http.ResponseWriter, r *http.Request) {
		server.forwardEvents(w, r, "/internal/events/"+url.PathEscape(r.PathValue("id")), true)
	}))
	mux.HandleFunc("GET /source-status", server.protect(func(w http.ResponseWriter, r *http.Request) {
		if !hasScope(claimsFromContext(r.Context()).Scope, scopeSummary) {
			writeBearerError(w, http.StatusForbidden, "insufficient_scope", "summary scope is required", scopeSummary)
			return
		}
		server.forwardRaw(w, r, "/internal/source-status")
	}))

	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           loggingMiddleware(logger, mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	logger.Info("client_api_server_started", "address", ":8080")
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("client_api_server_stopped", "error", err)
		os.Exit(1)
	}
}

func newAggregatorClient(maxInFlight int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxInFlight
	transport.MaxIdleConnsPerHost = maxInFlight
	transport.MaxConnsPerHost = maxInFlight
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

func (s *Server) protect(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rawToken, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeBearerError(w, http.StatusUnauthorized, "invalid_token", "a Bearer access token is required", "")
			return
		}
		claims, err := s.verifier.Verify(r.Context(), rawToken, correlationID(r.Context()))
		if err != nil {
			s.logger.Warn("access_token_rejected", "error", err, "correlation_id", correlationID(r.Context()))
			writeBearerError(w, http.StatusUnauthorized, "invalid_token", "access token is invalid or expired", "")
			return
		}
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
			ctx := context.WithValue(r.Context(), claimsContextKey, claims)
			next(w, r.WithContext(ctx))
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "server busy")
		}
	}
}

func (s *Server) forwardRaw(w http.ResponseWriter, r *http.Request, path string) {
	response, startedAt, err := s.callAggregator(r, path)
	if err != nil {
		s.writeAggregatorFailure(w, r, startedAt, err)
		return
	}
	defer response.Body.Close()
	if err := copyAggregatorResponse(w, response); err != nil {
		s.logAggregatorResponseFailure(r, response.StatusCode, startedAt, err)
		return
	}
	s.logAggregatorDone(r, response.StatusCode, startedAt)
}

func (s *Server) forwardEvents(w http.ResponseWriter, r *http.Request, path string, single bool) {
	claims := claimsFromContext(r.Context())
	fields, err := selectFields(r.URL.Query().Get("fields"), claims.Scope)
	if err != nil {
		if err == errInsufficientRawScope {
			writeBearerError(w, http.StatusForbidden, "insufficient_scope", "raw hazard fields require hazards:read:raw", scopeRaw)
			return
		}
		if err == errInsufficientSummaryScope {
			writeBearerError(w, http.StatusForbidden, "insufficient_scope", "summary scope is required", scopeSummary)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	response, startedAt, err := s.callAggregator(r, path)
	if err != nil {
		s.writeAggregatorFailure(w, r, startedAt, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if err := copyAggregatorResponse(w, response); err != nil {
			s.logAggregatorResponseFailure(r, response.StatusCode, startedAt, err)
			return
		}
		s.logAggregatorDone(r, response.StatusCode, startedAt)
		return
	}

	limited := io.LimitReader(response.Body, maxAggregatorResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		s.logAggregatorResponseFailure(r, response.StatusCode, startedAt, err)
		writeError(w, http.StatusBadGateway, "invalid aggregator response")
		return
	}
	if len(data) > maxAggregatorResponseBytes {
		s.logAggregatorResponseFailure(r, response.StatusCode, startedAt, errors.New("aggregator response exceeds size limit"))
		writeError(w, http.StatusBadGateway, "invalid aggregator response")
		return
	}
	if single {
		var envelope EventEnvelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			s.logAggregatorResponseFailure(r, response.StatusCode, startedAt, err)
			writeError(w, http.StatusBadGateway, "invalid aggregator response")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"event":         projectEvent(envelope.Event, fields),
			"source_status": envelope.SourceStatus,
		})
	} else {
		var envelope EventsEnvelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			s.logAggregatorResponseFailure(r, response.StatusCode, startedAt, err)
			writeError(w, http.StatusBadGateway, "invalid aggregator response")
			return
		}
		projected := make([]map[string]any, 0, len(envelope.Events))
		for _, event := range envelope.Events {
			projected = append(projected, projectEvent(event, fields))
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": projected, "sources": envelope.Sources})
	}
	s.logAggregatorDone(r, response.StatusCode, startedAt)
}

func (s *Server) callAggregator(r *http.Request, path string) (*http.Response, time.Time, error) {
	target := s.aggregatorURL + path
	query := r.URL.Query()
	query.Del("fields")
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
	if err != nil {
		return nil, time.Now(), err
	}
	request.Header.Set("X-Aggregator-Key", s.aggregatorKey)
	request.Header.Set("X-Correlation-ID", correlationID(r.Context()))
	startedAt := time.Now()
	response, err := s.client.Do(request)
	return response, startedAt, err
}

func (s *Server) writeAggregatorFailure(w http.ResponseWriter, r *http.Request, startedAt time.Time, err error) {
	s.logger.Error("aggregator_request_failed", "error", err, "correlation_id", correlationID(r.Context()), "latency_ms", time.Since(startedAt).Milliseconds())
	writeError(w, http.StatusServiceUnavailable, "aggregator unavailable")
}

func (s *Server) logAggregatorDone(r *http.Request, status int, startedAt time.Time) {
	s.logger.Info("aggregator_request_done", "status", status, "correlation_id", correlationID(r.Context()), "latency_ms", time.Since(startedAt).Milliseconds())
}

func (s *Server) logAggregatorResponseFailure(r *http.Request, status int, startedAt time.Time, err error) {
	s.logger.Error("aggregator_response_failed", "error", err, "status", status, "correlation_id", correlationID(r.Context()), "latency_ms", time.Since(startedAt).Milliseconds())
}

func copyAggregatorResponse(w http.ResponseWriter, response *http.Response) error {
	w.Header().Set("Content-Type", "application/json")
	if retryAfter := response.Header.Get("Retry-After"); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(response.StatusCode)
	_, err := io.Copy(w, io.LimitReader(response.Body, maxAggregatorResponseBytes))
	return err
}

func bearerToken(value string) (string, bool) {
	scheme, tokenValue, ok := strings.Cut(strings.TrimSpace(value), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(tokenValue) == "" {
		return "", false
	}
	return strings.TrimSpace(tokenValue), true
}

func claimsFromContext(ctx context.Context) *AccessClaims {
	claims, _ := ctx.Value(claimsContextKey).(*AccessClaims)
	return claims
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if id == "" {
			id = newCorrelationID()
		}
		w.Header().Set("X-Correlation-ID", id)
		ctx := context.WithValue(r.Context(), correlationContextKey, id)
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		started := time.Now()
		next.ServeHTTP(wrapped, r.WithContext(ctx))
		logger.Info("http_request", "method", r.Method, "path", r.URL.Path, "status", wrapped.status, "correlation_id", id, "latency_ms", time.Since(started).Milliseconds())
	})
}

func correlationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationContextKey).(string)
	return id
}

func newCorrelationID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(value)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeBearerError(w http.ResponseWriter, status int, code, description, scope string) {
	header := `Bearer error="` + code + `", error_description="` + description + `"`
	if scope != "" {
		header += `, scope="` + scope + `"`
	}
	w.Header().Set("WWW-Authenticate", header)
	writeJSON(w, status, map[string]string{"error": code, "error_description": description})
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
