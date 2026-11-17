package handler

import (
	"net/http"
	"time"
)

// p1
func (h *P1Handler) GetAnalytics(w http.ResponseWriter, r *http.Request) {
	from, err := parseOptionalTime(r.URL.Query().Get("from"))
	if err != nil {
		writeError(w, 400, "from must be RFC3339")
		return
	}
	to, err := parseOptionalTime(r.URL.Query().Get("to"))
	if err != nil {
		writeError(w, 400, "to must be RFC3339")
		return
	}
	if from != nil && to != nil && from.After(*to) {
		writeError(w, 400, "from must precede to")
		return
	}
	result, err := h.store.Analytics(r.Context(), from, to)
	if err != nil {
		writeError(w, 503, "analytics unavailable")
		return
	}
	writeJSON(w, 200, result)
}

// p1
func (h *P1Handler) GetStorageStats(w http.ResponseWriter, r *http.Request) {
	result, err := h.store.StorageStats(r.Context())
	if err != nil {
		writeError(w, 503, "storage stats unavailable")
		return
	}
	writeJSON(w, 200, result)
}

// p1
func parseOptionalTime(raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	return parseTime(raw)
}
