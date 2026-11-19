package store

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/jackc/pgx/v5"
)

func TestBatchTracePreservesCorrelationAndDoesNotLogArguments(t *testing.T) {
	var output bytes.Buffer
	tracer := &postgresTracer{logger: slog.New(slog.NewJSONHandler(&output, nil))}
	batch := &pgx.Batch{}
	batch.Queue("SELECT $1", "sensitive-payload-must-not-be-logged")
	ctx := tracer.TraceBatchStart(correlation.WithID(context.Background(), "trace-regression"), nil, pgx.TraceBatchStartData{Batch: batch})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["correlation_id"] != "trace-regression" || entry["queries"] != float64(1) || entry["latency_ms"] == nil || strings.Contains(output.String(), "sensitive-payload") {
		t.Fatalf("invalid batch trace: %s", output.String())
	}
}
