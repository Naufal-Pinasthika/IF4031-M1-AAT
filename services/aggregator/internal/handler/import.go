package handler

import (
	"encoding/json"
	"io"
	"math"
	"net/http"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

// p1 canonical JSON only format conversion belongs to the actual dataset adapter
func (h *P1Handler) ImportEvents(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var events []model.HazardEvent
	if err := decoder.Decode(&events); err != nil {
		writeError(w, 400, "invalid canonical event array (maximum 2 MiB)")
		return
	}
	if len(events) == 0 || len(events) > 100 {
		writeError(w, 400, "batch must contain 1 to 100 events")
		return
	}
	if decoder.Decode(new(any)) != io.EOF {
		writeError(w, 400, "expected exactly one JSON array")
		return
	}
	for _, e := range events {
		if !validImport(e) {
			writeError(w, 400, "event requires canonical source, source_ref_id, hazard_type, severity, area_name, occurred_at and valid coordinates")
			return
		}
	}
	accepted := 0
	failed := 0
	for i := range events {
		if err := h.store.Upsert(r.Context(), &events[i]); err != nil {
			failed++
		} else {
			accepted++
		}
	}
	status := http.StatusOK
	if failed > 0 {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]int{"accepted": accepted, "failed": failed})
}

func validImport(e model.HazardEvent) bool {
	if e.SourceRefID == "" || e.AreaName == "" || e.OccuredAt.IsZero() || math.IsNaN(e.Latitude) || math.IsNaN(e.Longitude) || e.Latitude < -90 || e.Latitude > 90 || e.Longitude < -180 || e.Longitude > 180 {
		return false
	}
	switch e.Source {
	case model.SourceBMKG, model.SourcePVMBG, model.SourceSATELIT, model.SourceSENSORPANTAI:
	default:
		return false
	}
	switch e.HazardType {
	case model.HazardTypeSEISMIC, model.HazardTypeTSUNAMI, model.HazardTypeVOLCANIC, model.HazardTypeWILDFIRE, model.HazardTypeCOASTAL:
	default:
		return false
	}
	switch e.Severity {
	case model.SeverityNORMAL, model.SeverityWASPADA, model.SeveritySIAGA, model.SeverityAWAS:
	default:
		return false
	}
	return true
}
