package main

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard.html
var dashboardHTML []byte

// p1
func serveDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(dashboardHTML)
}

// p1
func (s *Server) forwardAnalytics(w http.ResponseWriter, r *http.Request) {
	if !hasScope(claimsFromContext(r.Context()).Scope, scopeSummary) {
		writeBearerError(w, http.StatusForbidden, "insufficient_scope", "summary scope is required", scopeSummary)
		return
	}
	s.forwardRaw(w, r, "/internal/analytics")
}
