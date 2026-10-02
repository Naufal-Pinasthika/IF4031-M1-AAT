package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/status"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

type EventReader interface {
	ListEvents(context.Context, store.EventFilter) ([]model.HazardEvent, error)
	GetEventByID(context.Context, string) (*model.HazardEvent, error)
}

type Handler struct {
	reader  EventReader
	tracker *status.Tracker
}

func New(reader EventReader, tracker *status.Tracker) *Handler {
	return &Handler{reader: reader, tracker: tracker}
}

func GetHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) GetEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	filter := store.EventFilter{
		Source:     query.Get("source"),
		HazardType: query.Get("hazard_type"),
		Severity:   query.Get("severity"),
		Limit:      50,
	}

	var err error
	if raw := query.Get("from"); raw != "" {
		filter.From, err = parseTime(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "from must be RFC3339")
			return
		}
	}
	if raw := query.Get("to"); raw != "" {
		filter.To, err = parseTime(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "to must be RFC3339")
			return
		}
	}
	if raw := query.Get("limit"); raw != "" {
		filter.Limit, err = strconv.Atoi(raw)
		if err != nil || filter.Limit < 1 || filter.Limit > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
	}
	if raw := query.Get("offset"); raw != "" {
		filter.Offset, err = strconv.Atoi(raw)
		if err != nil || filter.Offset < 0 {
			writeError(w, http.StatusBadRequest, "offset must be zero or greater")
			return
		}
	}

	events, err := h.reader.ListEvents(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load hazard events")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events":  events,
		"sources": h.sourceStatuses(filter.Source),
	})
}

func (h *Handler) GetEventByID(w http.ResponseWriter, r *http.Request) {
	event, err := h.reader.GetEventByID(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load hazard event")
		return
	}
	if event == nil {
		writeError(w, http.StatusNotFound, "hazard event not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event":         event,
		"source_status": h.tracker.Snapshot(string(event.Source)),
	})
}

func (h *Handler) GetSourceStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"sources": h.sourceStatuses("")})
}

func (h *Handler) sourceStatuses(source string) map[string]status.SourceStatus {
	result := make(map[string]status.SourceStatus)
	if source == "" || source == "BMKG" {
		result["BMKG"] = h.tracker.Snapshot("BMKG")
	}
	if source == "" || source == "PVMBG" {
		result["PVMBG"] = h.tracker.Snapshot("PVMBG")
	}
	return result
}

func parseTime(value string) (*time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
