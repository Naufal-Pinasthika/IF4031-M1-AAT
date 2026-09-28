package handler

import "net/http"

// GET /health
func GetHealth(w http.ResponseWriter, r* http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}