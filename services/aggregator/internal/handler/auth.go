package handler

import (
	"crypto/subtle"
	"net/http"
	"os"
)

// Aggregator auth using AGGREGATOR_API_KEY provided in env
func AggregatorAuth(next http.HandlerFunc) http.HandlerFunc {
	expectedKey := os.Getenv("AGGREGATOR_API_KEY")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providedKey := r.Header.Get("X-Aggregator-Key")
		if expectedKey == "" {
			http.Error(w, `{"error":"missing AGGREGATOR_API_KEY"}`, http.StatusInternalServerError)
			return
		}
		if providedKey == "" {
			http.Error(w, `{"error":"missing API key"}`, http.StatusUnauthorized)
			return
		}
		if subtle.ConstantTimeCompare([]byte(providedKey), []byte(expectedKey)) != 1 {
			http.Error(w, `{"error":"invalid API key"}`, http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)

	})

}
