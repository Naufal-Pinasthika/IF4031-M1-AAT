package status

import (
	"context"
	"sync"
	"time"
)

type SourceStatus struct {
	State                     string     `json:"state"`
	LastSuccessAt             *time.Time `json:"last_success_at,omitempty"`
	StaleSince                *time.Time `json:"stale_since,omitempty"`
	CircuitState              string     `json:"circuit_state,omitempty"`
	HealthMetadataUnavailable bool       `json:"health_metadata_unavailable,omitempty"`
}

type Tracker struct {
	mu             sync.RWMutex
	sources        map[string]SourceStatus
	shared         map[string]SourceStatus
	sharedEnabled  bool
	sharedAt       time.Time
	sharedMaxAge   time.Duration
	sharedFailedAt *time.Time
}

func NewTracker() *Tracker {
	return &Tracker{sources: make(map[string]SourceStatus)}
}

func (t *Tracker) Success(source string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now = now.UTC()
	current := t.sources[source]
	current.State = "UP"
	current.LastSuccessAt = &now
	current.StaleSince = nil
	t.sources[source] = current
}

func (t *Tracker) Standby(source string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	current := t.sources[source]
	current.State = "STANDBY"
	t.sources[source] = current
}

func (t *Tracker) Circuit(source, state string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	current := t.sources[source]
	current.CircuitState = state
	t.sources[source] = current
}

func (t *Tracker) Failure(source string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.sources[source]
	if current.StaleSince == nil {
		now = now.UTC()
		current.StaleSince = &now
	}
	if current.LastSuccessAt == nil {
		current.State = "UNAVAILABLE"
	} else {
		current.State = "STALE"
	}
	t.sources[source] = current
}

func (t *Tracker) Snapshot(source string) SourceStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.sharedEnabled {
		current, ok := t.shared[source]
		if !ok {
			current = SourceStatus{State: "UNKNOWN"}
		}
		if t.sharedFailedAt != nil || t.sharedAt.IsZero() || time.Since(t.sharedAt) > t.sharedMaxAge {
			current.HealthMetadataUnavailable = true
			current.State = "UNAVAILABLE"
			if current.LastSuccessAt != nil {
				current.State = "STALE"
			}
			if current.StaleSince == nil {
				if t.sharedFailedAt != nil {
					current.StaleSince = t.sharedFailedAt
				} else if !t.sharedAt.IsZero() {
					lastObservation := t.sharedAt
					current.StaleSince = &lastObservation
				}
			}
		}
		return current
	}
	current, ok := t.sources[source]
	if !ok {
		return SourceStatus{State: "UNKNOWN"}
	}
	return current
}

type HealthReader interface {
	SourceHealth(context.Context) (map[string]SourceStatus, error)
}

func (t *Tracker) EnableSharedHealth(maxAge time.Duration) {
	if maxAge <= 0 {
		panic("shared source-health age must be positive")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sharedEnabled, t.sharedMaxAge = true, maxAge
}

// RunSharedHealth keeps the read path independent of coordinator latency.
// Failed/stalled refreshes cannot leave a cached UP status indefinitely.
// Local leader/standby state continues to be tracked separately for polling.
func (t *Tracker) RunSharedHealth(ctx context.Context, reader HealthReader, interval time.Duration) {
	if interval <= 0 {
		panic("source-health refresh interval must be positive")
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			values, err := reader.SourceHealth(ctx)
			now := time.Now()
			t.mu.Lock()
			if err == nil {
				t.shared, t.sharedAt, t.sharedFailedAt = values, now, nil
			} else if t.sharedFailedAt == nil {
				t.sharedFailedAt = &now
			}
			t.mu.Unlock()
			timer.Reset(interval)
		}
	}
}
