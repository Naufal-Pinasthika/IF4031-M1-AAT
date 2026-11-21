package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
)

type queryStartedAtKey struct{}
type batchTraceKey struct{}
type batchTrace struct {
	started time.Time
	queries int
}

type postgresTracer struct {
	logger *slog.Logger
}

var _ pgx.BatchTracer = (*postgresTracer)(nil)

func (t *postgresTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	return context.WithValue(ctx, batchTraceKey{}, batchTrace{started: time.Now(), queries: data.Batch.Len()})
}

func (t *postgresTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (t *postgresTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	trace, _ := ctx.Value(batchTraceKey{}).(batchTrace)
	id := correlation.ID(ctx)
	if id == "" {
		id = "unscoped"
	}
	args := []any{"correlation_id", id, "queries", trace.queries, "latency_ms", time.Since(trace.started).Milliseconds()}
	if data.Err != nil {
		t.logger.Error("postgres_batch_failed", append(args, "error", data.Err)...)
	} else if id == "unscoped" {
		t.logger.Debug("postgres_batch_completed", args...)
	} else {
		t.logger.Info("postgres_batch_completed", args...)
	}
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
	if correlationID == "unscoped" {
		// Empty outbox/lease maintenance is not an end-user trace. Keep errors
		// visible, but avoid hundreds of repetitive INFO records per second.
		t.logger.Debug("postgres_query_completed", arguments...)
	} else {
		t.logger.Info("postgres_query_completed", arguments...)
	}
}
