package handler

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"time"
)

const fieldAdmissionWait = time.Second

func acquireField(ctx context.Context, active, waiting chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case active <- struct{}{}:
		return true
	default:
	}
	select {
	case waiting <- struct{}{}:
		defer func() { <-waiting }()
	default:
		return false
	}
	timer := time.NewTimer(fieldAdmissionWait)
	defer timer.Stop()
	select {
	case active <- struct{}{}:
		if ctx.Err() != nil {
			<-active
			return false
		}
		return true
	case <-ctx.Done():
	case <-timer.C:
	}
	return false
}

// Admission classification does not replace normal database authentication or
// refresh-token validation. Checking BOTH configured credentials prevents a
// claimed client ID or spoofed priority header from stealing reserved slots.
func LimitField(maximum, reserve int, fieldID, fieldSecret string, next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(fieldSecret))
	enabled := fieldID != "" && fieldSecret != ""
	active := make(chan struct{}, maximum)
	regular := make(chan struct{}, maximum-reserve)
	waiting := make(chan struct{}, maximum)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, secret, basic := r.BasicAuth()
		provided := sha256.Sum256([]byte(secret))
		priority := basic && enabled && id == fieldID &&
			subtle.ConstantTimeCompare(expected[:], provided[:]) == 1 && r.Method == http.MethodPost &&
			(r.URL.Path == "/v1/token" || r.URL.Path == "/v1/token/refresh")
		allowed := false
		if priority {
			allowed = acquireField(r.Context(), active, waiting)
		} else {
			select {
			case regular <- struct{}{}:
				select {
				case active <- struct{}{}:
					allowed = true
				default:
					<-regular
				}
			default:
			}
		}
		if !allowed {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "server_busy", "too many active requests")
			return
		}
		defer func() {
			<-active
			if !priority {
				<-regular
			}
		}()
		next.ServeHTTP(w, r)
	})
}
