package aggregator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/broker"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

type projectionStore struct {
	EventStore
	apply func(context.Context, *model.HazardEvent, int64) error
}

func (s projectionStore) UpsertAccepted(ctx context.Context, e *model.HazardEvent, v int64) error {
	return s.apply(ctx, e, v)
}

func (s projectionStore) UpsertAcceptedTarget(ctx context.Context, e *model.HazardEvent, v int64, _ string) error {
	return s.apply(ctx, e, v)
}

type publisherFunc func(context.Context, broker.EventEnvelope) error

func (f publisherFunc) Publish(ctx context.Context, e broker.EventEnvelope) error { return f(ctx, e) }

func TestProjectionFailureDoesNotSuppressOtherDestination(t *testing.T) {
	for _, failed := range []string{"canonical", "broker"} {
		t.Run(failed, func(t *testing.T) {
			storeCalled, publishCalled := false, false
			failure := errors.New("destination unavailable")
			a := &Aggregator{logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
				store: projectionStore{apply: func(context.Context, *model.HazardEvent, int64) error {
					storeCalled = true
					if failed == "canonical" {
						return failure
					}
					return nil
				}},
				publisher: publisherFunc(func(context.Context, broker.EventEnvelope) error {
					publishCalled = true
					if failed == "broker" {
						return failure
					}
					return nil
				})}
			target := "canonical:store-a"
			if failed == "broker" {
				target = "broker"
			}
			if err := a.deliverAccepted(t.Context(), store.AcceptedEvent{Version: 7, Target: target}); !errors.Is(err, failure) {
				t.Fatalf("lost retry cause: %v", err)
			}
			other := "broker"
			if failed == "broker" {
				other = "canonical:store-a"
			}
			if err := a.deliverAccepted(t.Context(), store.AcceptedEvent{Version: 7, Target: other}); err != nil {
				t.Fatal(err)
			}
			if !storeCalled || !publishCalled {
				t.Fatal("one failed destination suppressed the other")
			}
		})
	}
}

func TestSlowCanonicalDoesNotDelayBrokerProjection(t *testing.T) {
	release, published := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	a := &Aggregator{logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		store: projectionStore{apply: func(context.Context, *model.HazardEvent, int64) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}},
		publisher: publisherFunc(func(context.Context, broker.EventEnvelope) error { close(published); return nil })}
	done := make(chan error, 1)
	go func() { done <- a.deliverAccepted(ctx, store.AcceptedEvent{Version: 9, Target: "canonical:store-a"}) }()
	if err := a.deliverAccepted(ctx, store.AcceptedEvent{Version: 9, Target: "broker"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-published:
	case <-ctx.Done():
		t.Fatal("notification waited for canonical storage")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
