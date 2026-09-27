package middleware

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func PVMBGAuth(expectedToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(authHeader, prefix) {
				w.Header().Set("WWW-Authenticate", `Bearer`)
				http.Error(w, `{"error":"invalid authorization format"}`, http.StatusUnauthorized)
				return
			}

			providedToken := strings.TrimPrefix(authHeader, prefix)

			if providedToken == "" {
				http.Error(w, `{"error":"missing bearer token"}`, http.StatusUnauthorized)
				return
			}

			if subtle.ConstantTimeCompare([]byte(providedToken), []byte(expectedToken)) != 1 {
				http.Error(w, `{"error":"invalid token"}`, http.StatusUnauthorized)
				return
			}
			
			next.ServeHTTP(w, r)
		})
	}
}