package handler

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

// Fixed-cardinality counters: never use filters, cursors or identities as labels.
var readMetrics struct {
	hits, misses, joins, fills, fillBusy, projectionFallback atomic.Uint64
	activeFills                                              atomic.Int64
	fillNanos                                                atomic.Uint64
}

func WriteReadMetrics(w io.Writer) {
	for _, item := range []struct {
		name  string
		value uint64
	}{
		{"cache_hits", readMetrics.hits.Load()}, {"cache_misses", readMetrics.misses.Load()},
		{"cache_joined", readMetrics.joins.Load()}, {"fills", readMetrics.fills.Load()},
		{"fill_rejected", readMetrics.fillBusy.Load()}, {"projection_fallback", readMetrics.projectionFallback.Load()},
	} {
		fmt.Fprintf(w, "# TYPE aggregator_read_%s_total counter\naggregator_read_%s_total %d\n", item.name, item.name, item.value)
	}
	fmt.Fprintf(w, "# TYPE aggregator_read_active_fills gauge\naggregator_read_active_fills %d\n", readMetrics.activeFills.Load())
	fmt.Fprintf(w, "# TYPE aggregator_read_fill_seconds_sum counter\naggregator_read_fill_seconds_sum %g\n", float64(readMetrics.fillNanos.Load())/1e9)
}

// LimitedReader budgets actual fills, not all clients sharing a cached answer.
// The outer HTTP limiter independently bounds waiting clients and slow writers.
type LimitedReader struct {
	upstream EventReader
	slots    *Limiter
}

func NewLimitedReader(upstream EventReader, slots, reserve int) *LimitedReader {
	return &LimitedReader{upstream, NewPriorityLimiter(slots, reserve)}
}

func (r *LimitedReader) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	priority := store.ReadPriority(ctx)
	if !r.slots.acquire(priority) {
		readMetrics.fillBusy.Add(1)
		return nil, store.ErrStorageBusy
	}
	readMetrics.activeFills.Add(1)
	readMetrics.fills.Add(1)
	began := time.Now()
	return func() {
		readMetrics.fillNanos.Add(uint64(time.Since(began)))
		readMetrics.activeFills.Add(-1)
		r.slots.release(priority)
	}, nil
}

func (r *LimitedReader) ListEvents(ctx context.Context, f store.EventFilter) (store.EventPage, error) {
	release, err := r.acquire(ctx)
	if err != nil {
		return store.EventPage{}, err
	}
	defer release()
	return r.upstream.ListEvents(ctx, f)
}
func (r *LimitedReader) GetEventByID(ctx context.Context, id string) (store.EventLookup, error) {
	release, err := r.acquire(ctx)
	if err != nil {
		return store.EventLookup{}, err
	}
	defer release()
	return r.upstream.GetEventByID(ctx, id)
}
