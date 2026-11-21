package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type maintenanceKey struct{}

func maintenanceContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, maintenanceKey{}, true)
}

func isMaintenance(ctx context.Context) bool {
	value, _ := ctx.Value(maintenanceKey{}).(bool)
	return value
}

func (u *unit) operationPool(ctx context.Context) *pgxpool.Pool {
	if isMaintenance(ctx) && u.maintenance != nil {
		return u.maintenance
	}
	if ReadPriority(ctx) && u.priority != nil {
		return u.priority
	}
	return u.pool
}

// Ordinary replica reads fan out. Bound actual SQL work, not the number of
// clients sharing a cache fill. Keep a regular connection available for live
// projection; never enqueue an unbounded number of reads behind a repair.
// Explicit one-connection configurations cannot reserve a second connection.
func (u *unit) admitRead(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if u.readSlots == nil || isMaintenance(ctx) || ReadPriority(ctx) {
		return func() {}, nil
	}
	select {
	case u.readSlots <- struct{}{}:
		return func() { <-u.readSlots }, nil
	default:
		return nil, ErrStorageBusy
	}
}
