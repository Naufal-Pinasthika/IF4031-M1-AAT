package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/data"
)

// GET /seismic-events?since=<timestamp>
func GetSesimicEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// grab query paraameter `since`
	querySince := r.URL.Query().Get("since") 
	
	// assumptions: if null return error
	if querySince == "" {
		http.Error(w, `{"error":"missing query parameter"}`, http.StatusBadRequest)
		return
	}

	// parse 
	dataSince, err := time.Parse(time.RFC3339, querySince)
	if err != nil {
		http.Error(w, `{"error":"invalid timestamp"}`, http.StatusBadRequest)
		return
	}

	result := data.ListSeismicEvents(dataSince)
	w.Header().Set("Content-Type", "application/json")
	// convert to json
	json.NewEncoder(w).Encode(result)
}	
