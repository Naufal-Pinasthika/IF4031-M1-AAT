package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
)

func TestReadBudgetRejectionIsNotLoggedAsElapsedTimeout(t *testing.T) {
	if readFailureReason(fmt.Errorf("busy: %w", ErrStorageBusy)) != "query_budget_exhausted" {
		t.Fatal("admission rejection mislabeled as query timeout")
	}
	if readFailureReason(context.DeadlineExceeded) != "query_timeout" || readFailureReason(nil) != "unit_unavailable" {
		t.Fatal("existing failure labels changed")
	}
}

func TestReadPartialDiagnosticsPreserveReasonAndCorrelation(t *testing.T) {
	var output bytes.Buffer
	s := &Store{logger: slog.New(slog.NewJSONHandler(&output, nil))}
	s.logReadPartial(correlation.WithID(t.Context(), "fill-17"), "coverage_unproven", "coverage", "store-b", 2, 42, nil)
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["correlation_id"] != "fill-17" || entry["reason"] != "coverage_unproven" || entry["storage_unit"] != "store-b" || entry["partition"] != float64(2) || entry["fetch_limit"] != float64(42) {
		t.Fatalf("invalid partial reason: %+v", entry)
	}
}
