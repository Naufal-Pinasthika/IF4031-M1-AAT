package main

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
)

func (s *Server) forwardIngress(w http.ResponseWriter, r *http.Request, path string, opsOnly bool) {
	claims := claimsFromContext(r.Context())
	ops := hasScope(claims.Scope, scopeOps)
	if opsOnly && !ops {
		writeBearerError(w, 403, "insufficient_scope", "internal operations only", scopeOps)
		return
	}
	if !opsOnly && !ops && claims.ClientID != s.fieldClientID {
		writeBearerError(w, 403, "insufficient_scope", "field identity required", scopeRaw)
		return
	}
	var body []byte
	if r.Method == http.MethodPost {
		var err error
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
		if err != nil {
			writeError(w, 413, "request exceeds 64 KiB")
			return
		}
	}
	target := s.aggregatorURL + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	request, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(body))
	if err != nil {
		writeError(w, 400, "invalid request")
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Aggregator-Key", s.aggregatorKey)
	request.Header.Set("X-Correlation-ID", correlationID(r.Context()))
	request.Header.Set("X-Client-ID", claims.ClientID)
	role := "field"
	if ops {
		role = "ops"
	}
	request.Header.Set("X-Client-Role", role)
	request.Header.Set("Idempotency-Key", r.Header.Get("Idempotency-Key"))
	response, err := s.client.Do(request)
	if err != nil {
		writeError(w, 503, "ingress unavailable; retry using the same idempotency key")
		return
	}
	defer response.Body.Close()
	_ = copyAggregatorResponse(w, response)
}
func (s *Server) registerIngress(mux *http.ServeMux) {
	mux.HandleFunc("POST /field-reports", s.protect(func(w http.ResponseWriter, r *http.Request) { s.forwardIngress(w, r, "/internal/field-reports", false) }))
	mux.HandleFunc("GET /field-reports/{id}", s.protect(func(w http.ResponseWriter, r *http.Request) {
		s.forwardIngress(w, r, "/internal/field-reports/"+url.PathEscape(r.PathValue("id")), false)
	}))
	mux.HandleFunc("GET /unprocessed/{id}", s.protect(func(w http.ResponseWriter, r *http.Request) {
		s.forwardIngress(w, r, "/internal/unprocessed/"+url.PathEscape(r.PathValue("id")), true)
	}))
	mux.HandleFunc("POST /unprocessed/replay", s.protect(func(w http.ResponseWriter, r *http.Request) {
		s.forwardIngress(w, r, "/internal/unprocessed/replay", true)
	}))
	mux.HandleFunc("GET /satellite-status", s.protect(func(w http.ResponseWriter, r *http.Request) {
		s.forwardIngress(w, r, "/internal/satellite-status", true)
	}))
}
