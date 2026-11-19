package handler

import (
	"context"
	"encoding/json"
	"sync"
	"time"

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
}

type cachedPage struct {
	page    store.EventPage
	expires time.Time
	size    int
}

type pageFlight struct {
	done chan struct{}
	page store.EventPage
	err  error
}

func NewCachedReader(upstream EventReader, ttl, fillTimeout time.Duration) *CachedReader {
	return &CachedReader{upstream: upstream, ttl: ttl, fillTimeout: fillTimeout,
		entries: make(map[string]cachedPage), flights: make(map[string]*pageFlight)}
}

func (c *CachedReader) GetEventByID(ctx context.Context, id string) (store.EventLookup, error) {
	return c.upstream.GetEventByID(ctx, id)
}

func (c *CachedReader) ListEvents(ctx context.Context, filter store.EventFilter) (store.EventPage, error) {
	if c.ttl <= 0 || filter.Audit {
		return c.upstream.ListEvents(ctx, filter)
	}
	if err := ctx.Err(); err != nil {
		return store.EventPage{}, err
	}
	raw, err := json.Marshal(filter)
	if err != nil {
		return store.EventPage{}, err
	}
	if len(raw) > 4096 {
		return c.upstream.ListEvents(ctx, filter)
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
		c.mu.Unlock()
		return entry.page, nil
	}
	flight, exists := c.flights[flightKey]
	if !exists {
		if len(c.flights) >= 64 || !priority && c.regularFlights >= 48 {
			c.mu.Unlock()
			return store.EventPage{}, store.ErrStorageBusy
		}
		flight = &pageFlight{done: make(chan struct{})}
		c.flights[flightKey] = flight
		if !priority {
			c.regularFlights++
		}
		// One disconnected requester cannot cancel a shared read for others.
		// The fill still has its own finite I/O deadline and concurrency budget.
		fillCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.fillTimeout)
		go func() {
			defer cancel()
			page, err := c.upstream.ListEvents(fillCtx, filter)
			encoded, encodeErr := json.Marshal(page)
			c.mu.Lock()
			defer c.mu.Unlock()
			flight.page, flight.err = page, err
			if err == nil && encodeErr == nil && !page.Partial && len(encoded) <= 1<<20 && len(c.entries) < 128 && c.bytes+len(encoded) <= 8<<20 {
				// Expiry starts at read initiation, not completion. A slow query
				// must not make its old snapshot look fresh for another full TTL.
				if time.Now().Before(now.Add(c.ttl)) {
					if old, ok := c.entries[key]; ok {
						c.bytes -= old.size
					}
					c.entries[key] = cachedPage{page: page, expires: now.Add(c.ttl), size: len(encoded)}
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
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return store.EventPage{}, ctx.Err()
	case <-flight.done:
		return flight.page, flight.err
	}
}
