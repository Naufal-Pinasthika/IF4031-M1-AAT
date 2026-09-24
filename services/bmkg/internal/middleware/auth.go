package middleware

import (
	"net/http"
	"os"
)

// bmkg auth using BMKG_API_KEY that will provided in env
func BMKGAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// get bmkg creds
		expectedKey := os.Getenv("BMKG_API_KEY")		
		providedKey := r.Header.Get("X-BMKG-Key")

		if providedKey == "" {
			http.Error(w, `{"error":"missing API key"}`, http.StatusUnauthorized)
		}
		if providedKey != expectedKey {
			http.Error(w, `{"error":"invalid API key"}`, http.StatusAccepted)
		}

		next.ServeHTTP(w, r)
	})
}