package handler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

type readerFunc func(context.Context, store.EventFilter) (store.EventPage, error)

func (f readerFunc) ListEvents(ctx context.Context, filter store.EventFilter) (store.EventPage, error) {
	return f(ctx, filter)
}
func (f readerFunc) GetEventByID(context.Context, string) (store.EventLookup, error) {
	return store.EventLookup{}, nil
}

func TestReadSideCoalescesArbitraryFiltersAndExpires(t *testing.T) {
	var calls atomic.Int32
	upstream := readerFunc(func(context.Context, store.EventFilter) (store.EventPage, error) {
		calls.Add(1)
		return store.EventPage{}, nil
	})
	c := NewCachedReader(upstream, 30*time.Millisecond, time.Second)
	filter := store.EventFilter{Source: "arbitrary-source", Severity: "AWAS", Limit: 17, Cursor: "opaque-cursor"}
	var workers sync.WaitGroup
	for range 24 {
		workers.Go(func() {
			if _, err := c.ListEvents(t.Context(), filter); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatalf("identical concurrent reads were not coalesced: %d", calls.Load())
	}
	filter.Severity = "SIAGA"
	if _, err := c.ListEvents(t.Context(), filter); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("different filters shared a cached answer")
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := c.ListEvents(t.Context(), filter); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatal("expired snapshot reused")
	}
}

func TestReadSideDoesNotRetainPartialErrorsOrAudit(t *testing.T) {
	for _, kind := range []string{"partial", "error", "audit"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			c := NewCachedReader(readerFunc(func(context.Context, store.EventFilter) (store.EventPage, error) {
				calls++
				if kind == "error" {
					return store.EventPage{}, store.ErrStorageBusy
				}
				return store.EventPage{ReadStatus: store.ReadStatus{Partial: kind == "partial"}}, nil
			}), time.Second, time.Second)
			for range 2 {
				_, _ = c.ListEvents(t.Context(), store.EventFilter{Audit: kind == "audit"})
			}
			if calls != 2 {
				t.Fatalf("%s answer was cached", kind)
			}
		})
	}
}

func TestReadSideCancellationIsPerWaiter(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	c := NewCachedReader(readerFunc(func(ctx context.Context, _ store.EventFilter) (store.EventPage, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return store.EventPage{}, nil
		case <-ctx.Done():
			return store.EventPage{}, ctx.Err()
		}
	}), time.Second, time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := c.ListEvents(ctx, store.EventFilter{}); done <- err }()
	<-started
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("canceled waiter did not return")
	}
	close(release)
	if _, err := c.ListEvents(t.Context(), store.EventFilter{}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("one waiter canceled a shared fill")
	}
}

func TestReadSideFillHasIndependentDeadline(t *testing.T) {
	c := NewCachedReader(readerFunc(func(ctx context.Context, _ store.EventFilter) (store.EventPage, error) {
		<-ctx.Done()
		return store.EventPage{}, ctx.Err()
	}), time.Second, 10*time.Millisecond)
	if _, err := c.ListEvents(t.Context(), store.EventFilter{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fill deadline not enforced: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.flights) != 0 || len(c.entries) != 0 {
		t.Fatal("failed fill leaked state")
	}
}

func TestFieldFillDoesNotWaitForRegularFlight(t *testing.T) {
	regularStarted, release := make(chan struct{}), make(chan struct{})
	c := NewCachedReader(readerFunc(func(ctx context.Context, _ store.EventFilter) (store.EventPage, error) {
		if store.ReadPriority(ctx) {
			return store.EventPage{}, nil
		}
		close(regularStarted)
		select {
		case <-release:
			return store.EventPage{}, nil
		case <-ctx.Done():
			return store.EventPage{}, ctx.Err()
		}
	}), time.Second, time.Second)
	done := make(chan error, 1)
	go func() { _, err := c.ListEvents(t.Context(), store.EventFilter{}); done <- err }()
	<-regularStarted
	ctx, cancel := context.WithTimeout(store.WithReadPriority(t.Context(), true), 100*time.Millisecond)
	defer cancel()
	_, err := c.ListEvents(ctx, store.EventFilter{})
	close(release)
	regularErr := <-done
	if err != nil || regularErr != nil {
		t.Fatalf("field joined regular fill: field=%v regular=%v", err, regularErr)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	actual := 0
	for _, entry := range c.entries {
		actual += entry.size
	}
	if c.bytes != actual || c.regularFlights != 0 {
		t.Fatalf("cache accounting leaked: bytes=%d actual=%d regular=%d", c.bytes, actual, c.regularFlights)
	}
}

func TestRegularCacheFlightsCannotConsumeFieldReserve(t *testing.T) {
	started, release := make(chan struct{}, 48), make(chan struct{})
	c := NewCachedReader(readerFunc(func(ctx context.Context, _ store.EventFilter) (store.EventPage, error) {
		if store.ReadPriority(ctx) {
			return store.EventPage{}, nil
		}
		started <- struct{}{}
		select {
		case <-release:
			return store.EventPage{}, nil
		case <-ctx.Done():
			return store.EventPage{}, ctx.Err()
		}
	}), time.Second, 2*time.Second)
	var workers sync.WaitGroup
	for limit := 1; limit <= 48; limit++ {
		workers.Go(func() { _, _ = c.ListEvents(t.Context(), store.EventFilter{Limit: limit}) })
	}
	for range 48 {
		<-started
	}
	_, regularErr := c.ListEvents(t.Context(), store.EventFilter{Limit: 99})
	_, fieldErr := c.ListEvents(store.WithReadPriority(t.Context(), true), store.EventFilter{Limit: 99})
	close(release)
	workers.Wait()
	if !errors.Is(regularErr, store.ErrStorageBusy) || fieldErr != nil {
		t.Fatalf("cache reserve failed: regular=%v field=%v", regularErr, fieldErr)
	}
}
