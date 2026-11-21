package store

import (
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

func TestPostgresAdmissionFingerprintSurvivesTimestampRoundTrip(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "single"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			c, _ := integrationCoordinator(t)
			ctx := t.Context()
			lease, won, err := c.AcquireOrRenewLease(ctx, "BMKG", "precision-owner", time.Minute)
			if err != nil || !won {
				t.Fatalf("lease: %v", err)
			}
			event := model.HazardEvent{Source: model.SourceBMKG, SourceRefID: "warning-precision", HazardType: model.HazardTypeTSUNAMI,
				OccuredAt: time.Date(2026, 11, 21, 20, 0, 0, 527368491, time.UTC), Severity: model.SeverityAWAS,
				Attributes: map[string]any{"related_event_id": "quake-precision"}}
			admit := func() error {
				if batch {
					return c.AcceptBatch(ctx, "BMKG", "precision-owner", lease.Token, []model.HazardEvent{event}, "precision-test")
				}
				return c.AcceptEvent(ctx, "BMKG", "precision-owner", lease.Token, &event, "precision-test")
			}
			if err := admit(); err != nil {
				t.Fatal(err)
			}
			// A warning initially uses the in-memory parent quake. On a later
			// poll its parent comes from PostgreSQL's microsecond timestamp.
			if err := c.pool.QueryRow(ctx, `SELECT $1::timestamptz`, event.OccuredAt).Scan(&event.OccuredAt); err != nil {
				t.Fatal(err)
			}
			if err := admit(); err != nil {
				t.Fatal(err)
			}
			var versions int
			if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox WHERE hazard_id=$1`, eventID("BMKG", event.SourceRefID)).Scan(&versions); err != nil {
				t.Fatal(err)
			}
			if versions != 1 {
				t.Fatalf("unchanged warning generated %d admission versions after timestamp round trip", versions)
			}
			event.OccuredAt = event.OccuredAt.Add(time.Microsecond)
			if err := admit(); err != nil {
				t.Fatal(err)
			}
			if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox WHERE hazard_id=$1`, eventID("BMKG", event.SourceRefID)).Scan(&versions); err != nil {
				t.Fatal(err)
			}
			if versions != 2 {
				t.Fatalf("a real microsecond-level timestamp change was hidden: versions=%d", versions)
			}
		})
	}
}
