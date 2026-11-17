package handler

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

// p1 canonical store private to Aggregator
type P1Handler struct {
	store      *store.Store
	auditToken string
}

func NewP1(s *store.Store, token string) *P1Handler { return &P1Handler{store: s, auditToken: token} }

func auditError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func (h *P1Handler) AuditAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || h.auditToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(h.auditToken)) != 1 {
			auditError(w, 401, "unauthorized", "token tidak valid")
			return
		}
		next(w, r)
	}
}

func (h *P1Handler) GetAuditEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.EventFilter{Source: q.Get("source"), Cursor: q.Get("cursor"), Limit: 100, Audit: true}
	var err error
	if raw := q.Get("limit"); raw != "" {
		f.Limit, err = strconv.Atoi(raw)
		if err != nil || f.Limit < 1 || f.Limit > 1000 {
			auditError(w, 400, "invalid_parameter", "limit must be between 1 and 1000")
			return
		}
	}
	if raw := q.Get("since"); raw != "" {
		f.Since, err = parseTime(raw)
		if err != nil {
			auditError(w, 400, "invalid_parameter", "since must be RFC3339")
			return
		}
	}
	page, err := h.store.ListEvents(r.Context(), f)
	if err != nil {
		if errors.Is(err, store.ErrInvalidCursor) {
			auditError(w, 400, "invalid_cursor", "cursor tidak valid")
		} else {
			auditError(w, 503, "unavailable", "audit unavailable")
		}
		return
	}
	writeJSON(w, 200, map[string]any{"items": page.Events, "next_cursor": page.NextCursor, "partial": page.Partial, "unavailable_units": page.UnavailableUnits})
}

func (h *P1Handler) GetAuditChecksum(w http.ResponseWriter, r *http.Request) {
	result, err := h.store.AuditChecksum(r.Context(), r.URL.Query().Get("source"))
	if err != nil {
		auditError(w, 503, "unavailable", "audit unavailable")
		return
	}
	writeJSON(w, 200, result)
}

func (h *P1Handler) GetAuditLocation(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("hazard_id")
	if id == "" {
		auditError(w, 400, "invalid_parameter", "hazard_id is required")
		return
	}
	result, err := h.store.AuditLocation(r.Context(), id)
	if err != nil {
		auditError(w, 503, "unavailable", "audit unavailable")
		return
	}
	if len(result.Locations) == 0 && !result.Partial {
		auditError(w, 404, "not_found", "hazard event not found")
		return
	}
	writeJSON(w, 200, result)
}
