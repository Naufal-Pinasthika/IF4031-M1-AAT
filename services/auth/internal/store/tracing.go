package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/correlation"
)

type queryStartedAtKey struct{}

type postgresTracer struct {
	logger *slog.Logger
}

func (t *postgresTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, queryStartedAtKey{}, time.Now())
}

func (t *postgresTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	startedAt, _ := ctx.Value(queryStartedAtKey{}).(time.Time)
	correlationID := correlation.ID(ctx)
	if correlationID == "" {
		correlationID = "unscoped"
	}
	arguments := []any{
		"command", data.CommandTag.String(),
		"correlation_id", correlationID,
		"latency_ms", time.Since(startedAt).Milliseconds(),
	}
	if data.Err != nil {
		t.logger.Error("postgres_query_failed", append(arguments, "error", data.Err)...)
		return
	}
	t.logger.Info("postgres_query_completed", arguments...)
}
