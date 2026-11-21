package store

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrSourcePaused = errors.New("coastal circuit is open")

func (c *Coordinator) initSourceRetry(ctx context.Context) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var exists bool
	if err := c.pool.QueryRow(op, `SELECT to_regclass('source_retry_state') IS NOT NULL`).Scan(&exists); err != nil || exists {
		return err
	}
	tx, err := c.pool.Begin(op)
	if err != nil {
		return err
	}
	defer tx.Rollback(op)
	if _, err = tx.Exec(op, `SELECT pg_advisory_xact_lock(40310501)`); err != nil {
		return err
	}
	_, err = tx.Exec(op, `CREATE TABLE IF NOT EXISTS source_retry_state (
 source TEXT PRIMARY KEY, failures INTEGER NOT NULL DEFAULT 0)`)
	if err != nil {
		return err
	}
	return tx.Commit(op)
}

// The bounded retry cadence is persisted with FinishPoll, not with a pod's
// local breaker. Lease handover therefore cannot restart a burst of retries.
func sourceRetryDelay(interval time.Duration, failures int) time.Duration {
	capDelay := max(interval, 2*time.Second)
	delay := interval
	for n := 1; n < failures && delay < capDelay; n++ {
		delay = min(capDelay, delay*2)
	}
	if delay >= capDelay || delay <= 0 {
		return capDelay
	}
	// Add jitter rather than reducing the minimum contractual poll interval.
	return min(capDelay, delay+time.Duration(rand.Int64N(max(1, int64(delay/4)))))
}

func finishSourceRetry(ctx context.Context, tx pgx.Tx, attempt PollAttempt, interval time.Duration, pollErr error) (time.Duration, error) {
	if attempt.Source != "SENSOR_PANTAI" || errors.Is(pollErr, ErrSourcePaused) {
		return interval, nil
	}
	var failures int
	err := tx.QueryRow(ctx, `INSERT INTO source_retry_state(source,failures) VALUES($1,CASE WHEN $2 THEN 1 ELSE 0 END)
 ON CONFLICT(source) DO UPDATE SET failures=CASE WHEN $2 THEN LEAST(source_retry_state.failures+1,30) ELSE 0 END
 RETURNING failures`, attempt.Source, pollErr != nil).Scan(&failures)
	if err != nil || pollErr == nil {
		return interval, err
	}
	return sourceRetryDelay(interval, failures), nil
}
