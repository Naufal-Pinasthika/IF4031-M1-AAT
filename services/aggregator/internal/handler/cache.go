package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

// CachedReader is the bounded-staleness read side, never the audit/ingestion
// path. Keys include every exported EventFilter field, not a benchmark URL.
// Partial pages and failures are never retained. Returned pages are immutable.
type CachedReader struct {
	upstream         EventReader
	ttl, fillTimeout time.Duration
	mu               sync.Mutex
	entries          map[string]cachedPage
	flights          map[string]*pageFlight
	bytes            int
	regularFlights   int
	logger           *slog.Logger
	projection       interface {
		ReadPage(context.Context, string, time.Duration, func(context.Context) (store.EventPage, error)) (store.EventPage, time.Time, error)
	}
}

// SetProjection is startup-only. The projection shares certified query results,
// never source health, credentials, HTTP headers or Audit answers.
func (c *CachedReader) SetProjection(p interface {
	ReadPage(context.Context, string, time.Duration, func(context.Context) (store.EventPage, error)) (store.EventPage, time.Time, error)
}) { c.projection = p }

type cachedPage struct {
	page    store.EventPage
	wire    []byte // immutable JSON page fields, without live source health
	expires time.Time
	size    int
}

type pageFlight struct {
	done    chan struct{}
	page    store.EventPage
	wire    []byte
	err     error
	id      string
	waiters int // protected by CachedReader.mu until done closes
}

func NewCachedReader(upstream EventReader, ttl, fillTimeout time.Duration, loggers ...*slog.Logger) *CachedReader {
	c := &CachedReader{upstream: upstream, ttl: ttl, fillTimeout: fillTimeout,
		entries: make(map[string]cachedPage), flights: make(map[string]*pageFlight)}
	if len(loggers) > 0 {
		c.logger = loggers[0]
	}
	return c
}

func (c *CachedReader) GetEventByID(ctx context.Context, id string) (store.EventLookup, error) {
	return c.upstream.GetEventByID(ctx, id)
}

func (c *CachedReader) ListEvents(ctx context.Context, filter store.EventFilter) (store.EventPage, error) {
	page, _, err := c.listEventsEncoded(ctx, filter)
	return page, err
}

func (c *CachedReader) listEventsEncoded(ctx context.Context, filter store.EventFilter) (store.EventPage, []byte, error) {
	if c.ttl <= 0 || filter.Audit {
		page, err := c.upstream.ListEvents(ctx, filter)
		return page, nil, err
	}
	if err := ctx.Err(); err != nil {
		return store.EventPage{}, nil, err
	}
	raw, err := json.Marshal(filter)
	if err != nil {
		return store.EventPage{}, nil, err
	}
	if len(raw) > 4096 {
		page, err := c.upstream.ListEvents(ctx, filter)
		return page, nil, err
	}
	key := string(raw)
	flightKey := key
	priority := store.ReadPriority(ctx)
	if priority {
		flightKey = "field:" + key
	}
	now := time.Now()
	c.mu.Lock()
	for k, entry := range c.entries {
		if !now.Before(entry.expires) {
			c.bytes -= entry.size
			delete(c.entries, k)
		}
	}
	if entry, ok := c.entries[key]; ok {
		readMetrics.hits.Add(1)
		c.mu.Unlock()
		return entry.page, entry.wire, nil
	}
	flight, exists := c.flights[flightKey]
	readMetrics.misses.Add(1)
	if exists {
		readMetrics.joins.Add(1)
	}
	if !exists {
		if len(c.flights) >= 64 || !priority && c.regularFlights >= 48 {
			c.mu.Unlock()
			return store.EventPage{}, nil, store.ErrStorageBusy
		}
		id := correlation.ID(ctx)
		if id == "" {
			id = newCorrelationID()
		}
		flight = &pageFlight{done: make(chan struct{}), id: id}
		c.flights[flightKey] = flight
		if !priority {
			c.regularFlights++
		}
		// One disconnected requester cannot cancel a shared read for others.
		// The fill still has its own finite I/O deadline and concurrency budget.
		fillCtx, cancel := context.WithTimeout(correlation.WithID(context.WithoutCancel(ctx), flight.id), c.fillTimeout)
		go func() {
			defer cancel()
			expires := now.Add(c.ttl)
			var page store.EventPage
			var err error
			if c.projection != nil {
				page, expires, err = c.projection.ReadPage(fillCtx, key, c.ttl, func(ctx context.Context) (store.EventPage, error) { return c.upstream.ListEvents(ctx, filter) })
			} else {
				page, err = c.upstream.ListEvents(fillCtx, filter)
			}
			encoded, encodeErr := marshalEventPage(page)
			c.mu.Lock()
			defer c.mu.Unlock()
			flight.page, flight.err = page, err
			if encodeErr == nil {
				flight.wire = encoded
			}
			if err == nil && encodeErr == nil && !page.Partial && len(encoded) <= 1<<20 && len(c.entries) < 128 && c.bytes+len(encoded) <= 8<<20 {
				// Expiry starts at read initiation, not completion. A slow query
				// must not make its old snapshot look fresh for another full TTL.
				if time.Now().Before(expires) {
					if old, ok := c.entries[key]; ok {
						c.bytes -= old.size
					}
					c.entries[key] = cachedPage{page: page, wire: encoded, expires: expires, size: len(encoded)}
					c.bytes += len(encoded)
				}
			}
			delete(c.flights, flightKey)
			if !priority {
				c.regularFlights--
			}
			close(flight.done)
		}()
	}
	flight.waiters++
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return store.EventPage{}, nil, ctx.Err()
	case <-flight.done:
		if c.logger != nil && (flight.err != nil || flight.page.Partial) {
			c.logger.Warn("read_fill_result", "correlation_id", correlation.ID(ctx),
				"fill_id", flight.id, "waiters", flight.waiters, "partial", flight.page.Partial,
				"unavailable_units", flight.page.UnavailableUnits,
				"replication_degraded", flight.page.ReplicationDegraded,
				"latency_ms", float64(time.Since(now))/float64(time.Millisecond), "error", flight.err)
		}
		return flight.page, flight.wire, flight.err
	}
}
