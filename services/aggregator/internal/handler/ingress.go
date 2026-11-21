package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/aggregator"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
	"github.com/jackc/pgx/v5"
)

func OperationsOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Client-Role") != "ops" {
			auditError(w, 403, "forbidden", "internal operations only")
			return
		}
		next(w, r)
	}
}

func (h *P1Handler) PostFieldReport(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Client-Role") != "field" && r.Header.Get("X-Client-Role") != "ops" {
		auditError(w, 403, "forbidden", "field identity required")
		return
	}
	client := r.Header.Get("X-Client-ID")
	if client == "" {
		auditError(w, 403, "forbidden", "signed client identity required")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		auditError(w, 413, "payload_too_large", "field reports are limited to 64 KiB")
		return
	}
	if err = aggregator.ValidateJSON(raw); err != nil {
		auditError(w, 400, "invalid_report", err.Error())
		return
	}
	var report struct {
		ID          string `json:"report_id"`
		Description string `json:"description"`
	}
	if err = json.Unmarshal(raw, &report); err != nil || !aggregator.ValidReportID(report.ID) || report.Description == "" {
		auditError(w, 400, "invalid_report", "report_id and description are required")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = report.ID
	}
	if !aggregator.ValidReportID(key) {
		auditError(w, 400, "invalid_report", "invalid idempotency key")
		return
	}
	// Canonical JSON makes harmless whitespace/key-order retries equivalent.
	var document any
	_ = json.Unmarshal(raw, &document)
	canonical, _ := json.Marshal(document)
	body, _ := json.Marshal(map[string]any{"client_id": client, "data": json.RawMessage(canonical)})
	id := store.InboxID("FIELD_REPORT", []byte(client+"\x00"+key))
	status, err := h.coordinator.StageInbox(r.Context(), id, "FIELD_REPORT", client, "field-report", store.InboxID("FIELD_REPORT", body), bytes.NewReader(body), store.InboxLease{CorrelationID: correlation.ID(r.Context())})
	if errors.Is(err, store.ErrIdempotencyConflict) {
		auditError(w, 409, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		auditError(w, 503, "unavailable", "report was not acknowledged; retry with the same key")
		return
	}
	writeJSON(w, 202, map[string]any{"report_id": id, "status": status})
}

func (h *P1Handler) GetFieldReport(w http.ResponseWriter, r *http.Request) {
	result, err := h.coordinator.FieldReport(r.Context(), r.PathValue("id"), r.Header.Get("X-Client-ID"), r.Header.Get("X-Client-Role") == "ops")
	if errors.Is(err, pgx.ErrNoRows) {
		auditError(w, 404, "not_found", "report not found")
		return
	}
	if err != nil {
		auditError(w, 503, "unavailable", "report unavailable")
		return
	}
	writeJSON(w, 200, result)
}
func (h *P1Handler) GetInboxDetails(w http.ResponseWriter, r *http.Request) {
	chunk, err := strconv.Atoi(r.URL.Query().Get("chunk"))
	if r.URL.Query().Get("chunk") == "" {
		chunk = 0
		err = nil
	}
	if err != nil || chunk < 0 {
		auditError(w, 400, "invalid_chunk", "chunk must be nonnegative")
		return
	}
	result, err := h.coordinator.InboxDetails(r.Context(), r.PathValue("id"), chunk)
	if errors.Is(err, pgx.ErrNoRows) {
		auditError(w, 404, "not_found", "message not found")
		return
	}
	if err != nil {
		auditError(w, 503, "unavailable", "message unavailable")
		return
	}
	writeJSON(w, 200, result)
}
func (h *P1Handler) ReplayInbox(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Source string `json:"source"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&body) != nil || body.Source != "PVMBG" {
		auditError(w, 400, "invalid_source", "only PVMBG messages can be replayed")
		return
	}
	count, err := h.coordinator.ReplayInbox(r.Context(), body.Source)
	if err != nil {
		auditError(w, 503, "unavailable", "replay unavailable")
		return
	}
	writeJSON(w, 202, map[string]any{"requeued": count})
}
func (h *P1Handler) GetSatelliteStats(w http.ResponseWriter, r *http.Request) {
	result, err := h.coordinator.SatelliteStats(r.Context())
	if err != nil {
		auditError(w, 503, "unavailable", "stream state unavailable")
		return
	}
	writeJSON(w, 200, result)
}
