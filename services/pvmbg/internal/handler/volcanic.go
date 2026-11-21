package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/data"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

type VolcanicHandler struct {
	State *state.ServiceState
}

func (v *VolcanicHandler) GetReports(w http.ResponseWriter, r *http.Request) {
	// if IsOutage() return err
	if v.State.IsOutage() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"PVMBG unavailable"}`))
		return
	}

	// if qparams since "", return err
	querySince := r.URL.Query().Get("since")
	if querySince == "" {
		http.Error(w, `{"error":"missing query parameter"}`, http.StatusBadRequest)
		return
	}

	since, err := time.Parse(time.RFC3339, querySince)
	if err != nil {
		http.Error(w, `{"error":"invalid timestamp"}`, http.StatusBadRequest)
		return
	}

	result := make([]model.VolcanicReport, 0)

	for _, report := range data.ListVolcanicReports(since) {
		copyReport := report

		if v.State.IsSchemaV2() {
			// PLACEHOLDER
			confidence := 0.92
			copyReport.ConfidenceLevel = &confidence
		}

		result = append(result, copyReport)
	}
	w.Header().Set("Content-Type", "application/json")
	mode := v.State.SchemaMode()
	switch mode {
	case "truncated":
		_, _ = w.Write([]byte(`[{"report_id":`))
		return
	case "deep":
		_, _ = w.Write([]byte(strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100)))
		return
	case "invalid-utf8":
		_, _ = w.Write([]byte{'[', '"', 0xff, '"', ']'})
		return
	case "oversized":
		_, _ = w.Write([]byte(`["`))
		for range 32 {
			_, _ = w.Write([]byte(strings.Repeat("x", 64<<10)))
		}
		_, _ = w.Write([]byte(`"]`))
		return
	case "rename", "type-change", "missing":
		rows := make([]map[string]any, 0, len(result))
		for _, report := range result {
			raw, _ := json.Marshal(report)
			var fields map[string]any
			_ = json.Unmarshal(raw, &fields)
			if mode == "rename" {
				fields["report_ref"] = fields["report_id"]
				delete(fields, "report_id")
			}
			if mode == "type-change" {
				fields["eruption_count_24h"] = strconv.FormatInt(report.EruptionCount24H, 10)
			}
			if mode == "missing" {
				delete(fields, "alert_level")
			}
			rows = append(rows, fields)
		}
		_ = json.NewEncoder(w).Encode(rows)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
