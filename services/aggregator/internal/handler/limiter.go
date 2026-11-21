package handler

import (
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
	"net/http"
	"sync"
)

type Limiter struct {
	mu                                  sync.Mutex
	maximum, regularMax, total, regular int
}

func NewLimiter(maxConcurrent int) *Limiter {
	return NewPriorityLimiter(maxConcurrent, 0)
}

func NewPriorityLimiter(maxConcurrent, fieldReserve int) *Limiter {
	if maxConcurrent < 1 || fieldReserve < 0 || fieldReserve >= maxConcurrent {
		panic("invalid admission limits")
	}
	return &Limiter{maximum: maxConcurrent, regularMax: maxConcurrent - fieldReserve}
}

func (l *Limiter) acquire(priority bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.maximum || !priority && l.regular >= l.regularMax {
		return false
	}
	l.total++
	if !priority {
		l.regular++
	}
	return true
}

func (l *Limiter) release(priority bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total--
	if !priority {
		l.regular--
	}
}

func (l *Limiter) Wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// This header is accepted only AFTER AggregatorAuth. Client API derives
		// it from verified claims, never from an untrusted incoming header.
		priority := r.Header.Get("X-Request-Priority") == "field"
		if !l.acquire(priority) {
			// reject fast if all slots busy
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "server busy")
			return
		}
		defer l.release(priority)
		next(w, r.WithContext(store.WithReadPriority(r.Context(), priority)))
	}
}
