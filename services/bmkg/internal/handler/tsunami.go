package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/data"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/model"
)

// GET /volcanic-reports?since=<timestamp>
func GetTsunamiWarnings(w http.ResponseWriter, r* http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// grab query paraameter `since`
	querySince := r.URL.Query().Get("since") 
	// assumptions: if null return error
	if querySince == "" {
		http.Error(w, `{"error":"invalid timestamp"}`, http.StatusBadRequest)
		return
	}

	// parse 
	dataSince, err := time.Parse(time.RFC3339, querySince)
	if err != nil {
		http.Error(w, `{"error":"invalid timestamp"}`, http.StatusBadRequest)
		return
	}

	// SeismicEvent.EventID -> SeismicEvent.OccuredAt
	eventOccuredAt := make(map[string]time.Time)

	for _, seismicEvent := range data.SeismicEvents {
		eventOccuredAt[seismicEvent.EventID] = seismicEvent.OccurredAt
	}

	result := make([]model.TsunamiWarning, 0)

	for _, warning := range data.TsunamiWarnings {
		occuredAt, exist := eventOccuredAt[warning.RelatedEventID]
		
		// collect SesimicEvent data DURING and AFTER `since`
		// append to result
		if !exist {
			continue
		}

		if occuredAt.Before(dataSince) {
			continue
		}
		result = append(result, warning)
	}

	// convert to json
	json.NewEncoder(w).Encode(result)
}	
