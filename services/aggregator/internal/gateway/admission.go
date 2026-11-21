package gateway

import (
	"context"
	"errors"
	"time"
)

var errQueueFull = errors.New("gateway backend queue full")
var errQueueTimeout = errors.New("gateway backend queue wait expired")

// At most one waiting request per active response slot, separately for each
// trusted class. Waiting cannot consume the complete upstream/client deadline.
// There is no replay, and expiry never marks a healthy backend unavailable.
func acquire(ctx context.Context, slots, waiters chan struct{}, wait time.Duration, m *classMetrics) error {
	if err := ctx.Err(); err != nil {
		m.canceled.Add(1)
		return err
	}
	select {
	case slots <- struct{}{}:
		return nil
	default:
	}
	select {
	case waiters <- struct{}{}:
		defer func() { <-waiters; m.pending.Add(-1) }()
		m.pending.Add(1)
		m.queued.Add(1)
	default:
		m.full.Add(1)
		return errQueueFull
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		m.canceled.Add(1)
		return ctx.Err()
	case <-timer.C:
		m.expired.Add(1)
		return errQueueTimeout
	}
}

// BeginDrain withdraws readiness and refuses new business work; responses that
// have already been admitted continue until HTTP Shutdown's finite deadline.
func (g *Gateway) BeginDrain() { g.draining.Store(true) }
