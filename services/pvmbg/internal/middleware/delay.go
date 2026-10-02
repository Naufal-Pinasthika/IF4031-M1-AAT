package middleware

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

func SimulatedDelay(
	serviceState *state.ServiceState,
	minDelay time.Duration,
	maxDelay time.Duration,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// skip wait if outage already on
			if serviceState.IsOutage() {
				next.ServeHTTP(w, r)
				return
			}

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
