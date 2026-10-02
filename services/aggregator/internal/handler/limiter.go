package handler

import "net/http"

type Limiter struct {
	slots chan struct{}
}

func NewLimiter(maxConcurrent int) *Limiter {
	return &Limiter{slots: make(chan struct{}, maxConcurrent)}
}

func (l *Limiter) Wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case l.slots <- struct{}{}:
			defer func() { <-l.slots }()
			next(w, r)
		default:
			// reject fast if all slots busy
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "server busy")
		}
	}
}
