package status

import (
	"sync"
	"time"
)

type SourceStatus struct {
	State         string     `json:"state"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	StaleSince    *time.Time `json:"stale_since,omitempty"`
}

type Tracker struct {
	mu      sync.RWMutex
	sources map[string]SourceStatus
}

func NewTracker() *Tracker {
	return &Tracker{sources: make(map[string]SourceStatus)}
}

func (t *Tracker) Success(source string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now = now.UTC()
	t.sources[source] = SourceStatus{State: "UP", LastSuccessAt: &now}
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

	current, ok := t.sources[source]
	if !ok {
		return SourceStatus{State: "UNKNOWN"}
	}
	return current
}
