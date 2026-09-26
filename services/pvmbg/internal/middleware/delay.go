package middleware

import (
	"math/rand"
	"net/http"
	"time"
)

func SimulatedDelay(
	minDelay time.Duration,
	maxDelay time.Duration,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			delayRange := maxDelay.Milliseconds() - minDelay.Milliseconds()

			delay := minDelay

			if delayRange > 0 {
				delay += time.Duration(
					rand.Int63n(delayRange+1),
				) * time.Millisecond
			}

			timer := time.NewTimer(delay)
			defer timer.Stop()

			select {
			case <-timer.C:
				next.ServeHTTP(w, r)

			case <-r.Context().Done():
				return
			}
		})
	}
}