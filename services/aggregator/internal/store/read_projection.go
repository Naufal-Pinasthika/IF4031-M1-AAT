package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadProjection is a disposable, bounded materialized query view, NOT another
// canonical store. At most 32 pages of <=1MiB live in the existing owner DB.
// Audit never uses it. No background dataset preload or full-dataset replica.
// Dedicated pools keep cache traffic away from source leases/delivery workers.
type ReadProjection struct {
	regular, field        *pgxpool.Pool
	hits, fills, fallback atomic.Uint64
}

func NewReadProjection(ctx context.Context, url string) (*ReadProjection, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 2
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 100 * time.Millisecond
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "100"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "1500"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	fc := cfg.Copy()
	fc.MaxConns = 1
	f, err := pgxpool.NewWithConfig(ctx, fc)
	if err != nil {
		p.Close()
		return nil, err
	}
	result := &ReadProjection{regular: p, field: f}
	op, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := p.Begin(op)
	if err != nil {
		result.Close()
		return nil, err
	}
	defer tx.Rollback(op)
	// Schema initialization has its own bounded budget. The 100 ms runtime
	// cache-query timeout must not also truncate transactional DDL under load.
	_, err = tx.Exec(op, `SET LOCAL statement_timeout='2000'; CREATE TABLE IF NOT EXISTS read_page_projection (
 slot SMALLINT PRIMARY KEY CHECK(slot>=0 AND slot<32),
 cache_key TEXT NOT NULL CHECK(length(cache_key)<=64),
 payload BYTEA NOT NULL CHECK(octet_length(payload)<=1048576),
 expires_at TIMESTAMPTZ NOT NULL)`)
	if err != nil {
		_ = tx.Rollback(op)
		result.Close()
		return nil, err
	}
	if err := tx.Commit(op); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

func (p *ReadProjection) Close() { p.regular.Close(); p.field.Close() }
func (p *ReadProjection) WriteMetrics(w io.Writer) {
	fmt.Fprintf(w, "aggregator_projection_hits_total %d\naggregator_projection_fills_total %d\naggregator_projection_fallback_total %d\n", p.hits.Load(), p.fills.Load(), p.fallback.Load())
}

type projectionDB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func projectionLookup(ctx context.Context, db projectionDB, slot int, key string) (EventPage, time.Time, error) {
	began := time.Now()
	var payload []byte
	var remaining float64
	err := db.QueryRow(ctx, `SELECT payload,extract(epoch FROM (expires_at-clock_timestamp()))::double precision
 FROM read_page_projection WHERE slot=$1 AND cache_key=$2 AND expires_at>clock_timestamp()`, slot, key).Scan(&payload, &remaining)
	var page EventPage
	if err != nil {
		return page, time.Time{}, err
	}
	// Age before the round trip is conservative. Never restart a full local TTL
	// when another pod's materialized page is read.
	until := began.Add(time.Duration(remaining * float64(time.Second)))
	if err = json.Unmarshal(payload, &page); err != nil || page.Partial || !time.Now().Before(until) {
		return EventPage{}, time.Time{}, pgx.ErrNoRows
	}
	return page, until, nil
}

func (p *ReadProjection) ReadPage(ctx context.Context, key string, ttl time.Duration, fill func(context.Context) (EventPage, error)) (EventPage, time.Time, error) {
	pool := p.regular
	namespace := int32(40310201)
	if ReadPriority(ctx) {
		pool = p.field
		namespace++
	} // Field never queues behind an ordinary fill.
	digest := sha256.Sum256([]byte(key))
	hash := hex.EncodeToString(digest[:])
	slot := int(digest[0] % 32)
	fallback := func() (EventPage, time.Time, error) {
		p.fallback.Add(1)
		began := time.Now()
		page, err := fill(ctx)
		return page, began.Add(ttl), err
	}
	// Fail open to the existing certified canonical read, NOT to stale data.
	acquire, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	conn, err := pool.Acquire(acquire)
	cancel()
	if err != nil {
		return fallback()
	}
	defer conn.Release()
	// Optional shared-cache I/O must not consume the canonical read deadline.
	// The short budget covers lookup/locking, not the canonical fill itself.
	cacheCtx, cacheCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cacheCancel()
	if page, until, err := projectionLookup(cacheCtx, conn, slot, hash); err == nil {
		p.hits.Add(1)
		return page, until, nil
	}
	// Cross-pod request coalescing is bounded. Hash collisions only reduce hits.
	waitUntil := time.Now().Add(20 * time.Millisecond)
	for {
		tx, err := conn.Begin(cacheCtx)
		if err != nil {
			return fallback()
		}
		var won bool
		err = tx.QueryRow(cacheCtx, `SELECT pg_try_advisory_xact_lock($1,$2)`, namespace, int32(slot)).Scan(&won)
		if err != nil {
			tx.Rollback(cacheCtx)
			return fallback()
		}
		if won {
			// Recheck after locking: another pod may have just published the result.
			if page, until, err := projectionLookup(cacheCtx, tx, slot, hash); err == nil {
				tx.Rollback(cacheCtx)
				p.hits.Add(1)
				return page, until, nil
			}
			var beganDB time.Time
			began := time.Now()
			if err = tx.QueryRow(cacheCtx, `SELECT clock_timestamp()`).Scan(&beganDB); err != nil {
				tx.Rollback(cacheCtx)
				return fallback()
			}
			p.fills.Add(1)
			cacheCancel()
			page, fillErr := fill(ctx)
			publishCtx, publishCancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer publishCancel()
			until := began.Add(ttl)
			payload, encodeErr := json.Marshal(page)
			if fillErr == nil && encodeErr == nil && !page.Partial && len(payload) <= 1<<20 && time.Now().Before(until) {
				_, err = tx.Exec(publishCtx, `INSERT INTO read_page_projection(slot,cache_key,payload,expires_at) VALUES($1,$2,$3,$4)
    ON CONFLICT(slot) DO UPDATE SET cache_key=excluded.cache_key,payload=excluded.payload,expires_at=excluded.expires_at
    WHERE read_page_projection.expires_at<excluded.expires_at`, slot, hash, payload, beganDB.Add(ttl))
				if err == nil {
					err = tx.Commit(publishCtx)
				} else {
					tx.Rollback(publishCtx)
				}
				// A failed publication does not erase a successful canonical result.
				if err != nil {
					p.fallback.Add(1)
				}
			} else {
				tx.Rollback(publishCtx)
			}
			return page, until, fillErr
		}
		tx.Rollback(cacheCtx)
		if page, until, err := projectionLookup(cacheCtx, conn, slot, hash); err == nil {
			p.hits.Add(1)
			return page, until, nil
		}
		if !time.Now().Before(waitUntil) || cacheCtx.Err() != nil {
			return fallback()
		}
		select {
		case <-ctx.Done():
			return EventPage{}, time.Time{}, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}
