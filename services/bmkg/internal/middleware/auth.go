package middleware

import (
	"net/http"
	"os"
)

// bmkg auth using BMKG_API_KEY that will provided in env
func BMKGAuth(next http.Handler) http.Handler {
	expectedKey := os.Getenv("BMKG_API_KEY")		
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providedKey := r.Header.Get("X-BMKG-Key")
		if expectedKey == "" {
			http.Error(w, `{"error":"missing BMKG_API_KEY"}`, http.StatusInternalServerError)
			return 
		}
		if providedKey == "" {
			http.Error(w, `{"error":"missing API key"}`, http.StatusUnauthorized)
			return 
		}
		if providedKey != expectedKey {
			http.Error(w, `{"error":"invalid API key"}`, http.StatusUnauthorized)
			return 
		}
	
		next.ServeHTTP(w, r)
		
	})

}