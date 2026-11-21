package handler

import (
	"net/http"
	"strconv"
)

// Internal operational evidence, not an addition to the assistant Audit API.
func (h *P1Handler) GetPolling(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 1000 {
			auditError(w, 400, "invalid_parameter", "limit must be between 1 and 1000")
			return
		}
		limit = value
	}
	items, err := h.coordinator.ListPollAttempts(r.Context(), r.URL.Query().Get("source"), limit)
	if err != nil {
		auditError(w, 503, "unavailable", "poll history unavailable")
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "order": "poll_id_desc", "retention_per_source": 10000})
}
