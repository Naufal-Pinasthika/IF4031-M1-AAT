package store

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// New pods can start before Service routing or the database is reachable.
// Retry only transient initialization failures; do not mask invalid credentials,
// permission errors, or malformed migrations with an endless restart loop.
// Each database attempt retains its own existing finite I/O timeout.
func retryStartup(ctx context.Context, logger *slog.Logger, operation func() error) error {
	const attempts = 6
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = operation(); err == nil {
			return nil
		}
		var pgerr *pgconn.PgError
		transient := availabilityError(err)
		if errors.As(err, &pgerr) {
			transient = pgerr.Code == "40001" || pgerr.Code == "40P01" || pgerr.Code == "55P03" || pgerr.Code == "57014" || pgerr.Code == "57P03" || len(pgerr.Code) >= 2 && pgerr.Code[:2] == "08"
		}
		if !transient || attempt == attempts-1 {
			return err
		}
		delay := 100 * time.Millisecond * time.Duration(1<<attempt)
		logger.Warn("coordination_startup_retry", "attempt", attempt+1, "delay_ms", delay.Milliseconds(), "error", err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
