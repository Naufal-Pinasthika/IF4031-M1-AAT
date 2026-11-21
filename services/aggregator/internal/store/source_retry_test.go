package store

import (
	"testing"
	"time"
)

func TestSourceRetryIsBoundedAndDoesNotAcceleratePolling(t *testing.T) {
	for _, base := range []time.Duration{time.Millisecond, 500 * time.Millisecond, 5 * time.Second} {
		for failures := 1; failures <= 30; failures++ {
			for range 20 {
				delay := sourceRetryDelay(base, failures)
				if delay < base || delay > max(base, 2*time.Second) {
					t.Fatalf("base %s failures %d produced %s", base, failures, delay)
				}
			}
		}
	}
}
