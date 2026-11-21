package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

func TestPostgresCriticalProjectionBypassesGeneralFailureAndReplaysSafely(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	lease, won, err := c.AcquireOrRenewLease(ctx, "SENSOR_PANTAI", "owner", 15*time.Second)
	if err != nil || !won {
		t.Fatalf("lease: %v", err)
	}
	event := model.HazardEvent{Source: model.SourceSENSORPANTAI, SourceRefID: "opaque-critical",
		HazardType: model.HazardTypeCOASTAL, Severity: model.SeverityAWAS, Attributes: map[string]any{"critical": true}}
	if err := c.AcceptEvent(ctx, "SENSOR_PANTAI", "owner", lease.Token, &event, "critical-test"); err != nil {
		t.Fatal(err)
	}
	if err := c.EnableCriticalLane(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeliverNext(ctx, "", "broker", func(context.Context, AcceptedEvent) error { return errors.New("general lane unavailable") }); err == nil {
		t.Fatal("failure was hidden")
	}
	var first int64
	worked, err := c.DeliverNext(ctx, "", "broker:critical", func(_ context.Context, item AcceptedEvent) error {
		first = item.Version
		return errors.New("ambiguous ack")
	})
	if !worked || err == nil || first == 0 {
		t.Fatalf("critical delivery: %v %v", worked, err)
	}
	if _, err := c.pool.Exec(ctx, `UPDATE ingestion_projections SET next_attempt_at=clock_timestamp() WHERE target='broker:critical'`); err != nil {
		t.Fatal(err)
	}
	worked, err = c.DeliverNext(ctx, "", "broker:critical", func(_ context.Context, item AcceptedEvent) error {
		if item.Version != first {
			t.Fatal("replay changed the ingestion version")
		}
		return nil
	})
	if !worked || err != nil {
		t.Fatalf("replay: %v %v", worked, err)
	}
	if err := c.AcceptEvent(ctx, "SENSOR_PANTAI", "owner", lease.Token, &event, "duplicate"); err != nil {
		t.Fatal(err)
	}
	worked, err = c.DeliverNext(ctx, "", "broker:critical", func(context.Context, AcceptedEvent) error { t.Fatal("duplicated critical intent"); return nil })
	if worked || err != nil {
		t.Fatalf("duplicate: %v %v", worked, err)
	}
}

func TestPostgresSourceRetrySurvivesOwnerChange(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	lease, won, err := c.AcquireOrRenewLease(ctx, "SENSOR_PANTAI", "old", 15*time.Second)
	if err != nil || !won {
		t.Fatalf("lease: %v", err)
	}
	for failure := 1; failure <= 3; failure++ {
		attempt, run, err := c.BeginPoll(ctx, "SENSOR_PANTAI", "old", lease.Token, 500*time.Millisecond)
		if err != nil || !run {
			t.Fatalf("begin: %v %v", run, err)
		}
		if err := c.FinishPoll(ctx, attempt, 500*time.Millisecond, errors.New("unavailable")); err != nil {
			t.Fatal(err)
		}
		if failure < 3 {
			if _, err := c.pool.Exec(ctx, `UPDATE source_poll_heads SET next_poll_at=clock_timestamp()`); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := c.ReleaseLease(ctx, "SENSOR_PANTAI", "old", lease.Token); err != nil {
		t.Fatal(err)
	}
	next, won, err := c.AcquireOrRenewLease(ctx, "SENSOR_PANTAI", "replacement", 15*time.Second)
	if err != nil || !won {
		t.Fatalf("new lease: %v", err)
	}
	attempt, run, err := c.BeginPoll(ctx, "SENSOR_PANTAI", "replacement", next.Token, 500*time.Millisecond)
	if err != nil || run || attempt.RetryAfter < time.Second {
		t.Fatalf("cooldown reset by takeover: run=%v delay=%s err=%v", run, attempt.RetryAfter, err)
	}
}
