package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/data"
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

	result := data.ListTsunamiWarnings(dataSince)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}	
