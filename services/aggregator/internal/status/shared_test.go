package status

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type healthReader struct {
	calls   atomic.Int32
	fail    atomic.Bool
	success time.Time
}

func (r *healthReader) SourceHealth(context.Context) (map[string]SourceStatus, error) {
	r.calls.Add(1)
	if r.fail.Load() {
		return nil, errors.New("coordinator unavailable")
	}
	return map[string]SourceStatus{"PVMBG": {State: "STALE", LastSuccessAt: &r.success, StaleSince: &r.success}}, nil
}

func TestSharedHealthOverridesStandbyAndDoesNotQueryOnReads(t *testing.T) {
	tracker := NewTracker()
	tracker.EnableSharedHealth(time.Second)
	reader := &healthReader{success: time.Now().UTC()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); tracker.RunSharedHealth(ctx, reader, time.Millisecond) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(time.Second)
	for tracker.Snapshot("PVMBG").LastSuccessAt == nil {
		if time.Now().After(deadline) {
			t.Fatal("no shared snapshot")
		}
		time.Sleep(time.Millisecond)
	}
	tracker.Standby("PVMBG")
	tracker.Success("PVMBG", time.Now()) // local state cannot hide the shared outage
	got := tracker.Snapshot("PVMBG")
	if got.State != "STALE" || got.StaleSince == nil {
		t.Fatalf("standby lied about freshness: %+v", got)
	}
	cancel()
	<-done
	before := reader.calls.Load()
	for range 10000 {
		_ = tracker.Snapshot("PVMBG")
	}
	if reader.calls.Load() != before {
		t.Fatal("client reads query coordinator")
	}
}

func TestSharedHealthDegradesOnRefreshFailureOrAge(t *testing.T) {
	tracker := NewTracker()
	tracker.EnableSharedHealth(time.Second)
	reader := &healthReader{success: time.Now().UTC()}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); tracker.RunSharedHealth(ctx, reader, time.Millisecond) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(time.Second)
	for reader.calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("refresh not running")
		}
		time.Sleep(time.Millisecond)
	}
	reader.fail.Store(true)
	for !tracker.Snapshot("PVMBG").HealthMetadataUnavailable {
		if time.Now().After(deadline) {
			t.Fatal("refresh failure hidden")
		}
		time.Sleep(time.Millisecond)
	}
	got := tracker.Snapshot("PVMBG")
	if got.State != "STALE" || got.LastSuccessAt == nil {
		t.Fatalf("last known state lost: %+v", got)
	}
	cancel()
	<-done
	tracker.mu.Lock()
	tracker.sharedFailedAt = nil
	tracker.sharedAt = time.Now().Add(-2 * time.Second)
	tracker.shared["PVMBG"] = SourceStatus{State: "UP", LastSuccessAt: &reader.success}
	tracker.mu.Unlock()
	if got := tracker.Snapshot("PVMBG"); got.State != "STALE" || !got.HealthMetadataUnavailable || got.StaleSince == nil {
		t.Fatalf("aged healthy cache still shown fresh: %+v", got)
	}
}
