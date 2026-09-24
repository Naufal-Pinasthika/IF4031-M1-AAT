package middleware

import (
	"math/rand"
	"net/http"
	"time"
)

// apply 50--150ms delay
func ApplyDelay(next http.Handler) http.Handler {
	return  http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delay := time.Duration(50+rand.Intn(101)) * time.Millisecond
		time.Sleep(delay)

		next.ServeHTTP(w, r)
	})
}