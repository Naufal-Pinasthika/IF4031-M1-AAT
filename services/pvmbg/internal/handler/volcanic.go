package handler

import (
	"encoding/json"
	"net/http"
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

	for _, report := range data.VolcanicReports {
		if report.ReportedAt.Before(since) {
			continue
		}

		copyReport := report

		if v.State.IsSchemaV2() {
			// PLACEHOLDER
			confidence := 0.92
			copyReport.ConfidenceLevel = &confidence
		}

		result = append(result, copyReport)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
