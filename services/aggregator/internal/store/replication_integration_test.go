package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

func integrationReplicas(t *testing.T) (*Store, []*Coordinator, []config.StorageUnit) {
	t.Helper()
	var settings []config.StorageUnit
	var databases []*Coordinator
	for i := range 3 {
		db, url := integrationCoordinator(t)
		databases = append(databases, db)
		settings = append(settings, config.StorageUnit{Name: fmt.Sprintf("store-%c", 'a'+i), DatabaseURL: url})
	}
	s, err := New(t.Context(), settings, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, databases, settings
}

func TestPostgresReadyReplicaGapIsVisibleAndAutomaticallyRepaired(t *testing.T) {
	s, _, settings := integrationReplicas(t)
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	partition := partitionFor(string(e.Source), e.SourceRefID)
	pair := s.destinations(partition)
	// The preferred replica remains empty AND reports ready, reproducing the
	// actual P2 failure without manufacturing a health failure.
	if err := s.UpsertAcceptedTarget(t.Context(), &e, 10, "canonical:"+settings[pair[1]].Name); err != nil {
		t.Fatal(err)
	}
	reader, err := New(t.Context(), settings, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, err := reader.GetEventByID(t.Context(), e.HazardID)
	if err != nil || got.Event == nil || got.Partial || !got.ReplicationDegraded {
		t.Fatalf("empty ready primary hid existing peer data: %+v %v", got, err)
	}
	page, err := reader.ListEvents(t.Context(), EventFilter{Limit: 10, Audit: true})
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("audit dropped gap: %+v %v", page, err)
	}
	reader.reconcileReady(t.Context())
	got, err = s.GetEventByID(t.Context(), e.HazardID)
	if err != nil || got.Event == nil || got.Partial {
		t.Fatalf("automatic anti-entropy did not close ready gap: %+v %v", got, err)
	}
}

func TestPostgresReplicaUnionPaginatesAndUsesNewestFilteredVersion(t *testing.T) {
	s, _, settings := integrationReplicas(t)
	ctx := t.Context()
	for i := range 17 {
		e := testHazard()
		e.SourceRefID = fmt.Sprintf("pagination-%02d", i)
		e.OccuredAt = e.OccuredAt.Add(time.Duration(i) * time.Minute)
		e.IngestedAt = e.OccuredAt
		pair := s.destinations(partitionFor(string(e.Source), e.SourceRefID))
		if err := s.UpsertAcceptedTarget(ctx, &e, int64(i+1), "canonical:"+settings[pair[i%2]].Name); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	cursor := ""
	for round := 0; ; round++ {
		if round > 20 {
			t.Fatal("pagination did not terminate")
		}
		page, err := s.ListEvents(ctx, EventFilter{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Events {
			if seen[e.HazardID] {
				t.Fatal("duplicate page identity")
			}
			seen[e.HazardID] = true
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if len(seen) != 17 {
		t.Fatalf("union pagination lost rows: %d", len(seen))
	}
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	pair := s.destinations(partitionFor(string(e.Source), e.SourceRefID))
	if err := s.UpsertAcceptedTarget(ctx, &e, 30, "canonical:"+settings[pair[0]].Name); err != nil {
		t.Fatal(err)
	}
	e.Severity = model.SeverityAWAS
	if err := s.UpsertAcceptedTarget(ctx, &e, 40, "canonical:"+settings[pair[1]].Name); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetEventByID(ctx, e.HazardID)
	if err != nil || got.Event == nil || got.Event.Severity != model.SeverityAWAS {
		t.Fatalf("read chose stale version: %+v %v", got, err)
	}
	page, err := s.ListEvents(ctx, EventFilter{id: e.HazardID, Severity: "WASPADA", Limit: 10})
	if err != nil || len(page.Events) != 0 || page.Partial || !page.ReplicationDegraded {
		t.Fatalf("stale matching version resurfaced through filter: %+v %v", page, err)
	}
	s.reconcileReady(ctx)
	got, err = s.GetEventByID(ctx, e.HazardID)
	if err != nil || got.Event == nil || got.Partial || got.Event.Severity != model.SeverityAWAS {
		t.Fatalf("version-aware anti-entropy regressed data: %+v %v", got, err)
	}
}

func TestPostgresProjectionAcknowledgementsSurviveAmbiguousCommitAndRestart(t *testing.T) {
	c, url := integrationCoordinator(t)
	s, _, settings := integrationReplicas(t)
	lease := acquire(t, c, "owner")
	e := testHazard()
	if err := c.AcceptEvent(t.Context(), "BMKG", "owner", lease.Token, &e, "ambiguous-commit"); err != nil {
		t.Fatal(err)
	}
	targets := c.projectionTargets(e)
	brokerCalls := 0
	worked, err := c.DeliverNext(t.Context(), "BMKG", "broker", func(context.Context, AcceptedEvent) error { brokerCalls++; return nil })
	if !worked || err != nil {
		t.Fatal(worked, err)
	}
	var version int64
	for i, target := range targets[1:] {
		worked, err := c.DeliverNext(t.Context(), "BMKG", target, func(ctx context.Context, item AcceptedEvent) error {
			version = item.Version
			if err := s.UpsertAcceptedTarget(ctx, &item.Event, item.Version, item.Target); err != nil {
				return err
			}
			if i == 1 {
				return context.DeadlineExceeded
			} // remote COMMIT succeeded; its ack was lost
			return nil
		})
		if !worked || i == 0 && err != nil || i == 1 && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(worked, err)
		}
	}
	var pending, payloads int
	if err := c.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM ingestion_projections WHERE delivered_at IS NULL),(SELECT count(*) FROM ingestion_outbox WHERE payload IS NOT NULL)`).Scan(&pending, &payloads); err != nil || pending != 1 || payloads != 1 {
		t.Fatalf("repair obligation was lost: %d %d %v", pending, payloads, err)
	}
	next, err := NewCoordinator(t.Context(), url, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	reader, err := New(t.Context(), settings, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := next.pool.Exec(t.Context(), `UPDATE ingestion_projections SET next_attempt_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	worked, err = next.DeliverNext(t.Context(), "BMKG", targets[2], func(ctx context.Context, item AcceptedEvent) error {
		if item.Version != version {
			t.Fatal("retry changed identity/version")
		}
		return reader.UpsertAcceptedTarget(ctx, &item.Event, item.Version, item.Target)
	})
	if !worked || err != nil {
		t.Fatal(worked, err)
	}
	worked, err = next.DeliverNext(t.Context(), "BMKG", "broker", func(context.Context, AcceptedEvent) error { brokerCalls++; return nil })
	if worked || err != nil || brokerCalls != 1 {
		t.Fatal("replica retry republished acknowledged Kafka projection")
	}
	if err := next.pool.QueryRow(t.Context(), `SELECT count(*) FROM ingestion_outbox WHERE payload IS NOT NULL OR delivered_at IS NULL`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("completed payload not cleared: %d %v", pending, err)
	}
	got, err := reader.GetEventByID(t.Context(), e.HazardID)
	if err != nil || got.Event == nil || got.Partial {
		t.Fatalf("idempotent retry failed: %+v %v", got, err)
	}
}

func TestPostgresUnavailableReplicaDoesNotBlockBrokerOrOtherReplica(t *testing.T) {
	c, _ := integrationCoordinator(t)
	s, databases, _ := integrationReplicas(t)
	lease := acquire(t, c, "owner")
	e := testHazard()
	if err := c.AcceptEvent(t.Context(), "BMKG", "owner", lease.Token, &e, "outage"); err != nil {
		t.Fatal(err)
	}
	pair := s.destinations(partitionFor(string(e.Source), e.SourceRefID))
	lock, err := databases[pair[0]].pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(t.Context(), `LOCK TABLE hazard_events IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	s.timeout = 100 * time.Millisecond
	targets := c.projectionTargets(e)
	worked, err := c.DeliverNext(t.Context(), "BMKG", targets[1], func(ctx context.Context, item AcceptedEvent) error {
		return s.UpsertAcceptedTarget(ctx, &item.Event, item.Version, item.Target)
	})
	if !worked || err == nil {
		t.Fatal("blocked target was falsely acknowledged")
	}
	for _, target := range []string{"broker", targets[2]} {
		worked, err := c.DeliverNext(t.Context(), "BMKG", target, func(ctx context.Context, item AcceptedEvent) error {
			if item.Target == "broker" {
				return nil
			}
			return s.UpsertAcceptedTarget(ctx, &item.Event, item.Version, item.Target)
		})
		if !worked || err != nil {
			t.Fatalf("independent target blocked: %s %v", target, err)
		}
	}
	if err := lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(t.Context(), `UPDATE ingestion_projections SET next_attempt_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	s.timeout = 2 * time.Second
	worked, err = c.DeliverNext(t.Context(), "BMKG", targets[1], func(ctx context.Context, item AcceptedEvent) error {
		return s.UpsertAcceptedTarget(ctx, &item.Event, item.Version, item.Target)
	})
	if !worked || err != nil {
		t.Fatalf("repair did not resume: %v %v", worked, err)
	}
}

func TestPostgresConcurrentFinalAcknowledgementsClearPayload(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "owner")
	e := testHazard()
	if err := c.AcceptEvent(t.Context(), "BMKG", "owner", lease.Token, &e, "concurrent"); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	ready := make(chan struct{}, 3)
	release := make(chan struct{})
	for _, target := range c.projectionTargets(e) {
		group.Go(func() {
			worked, err := c.DeliverNext(t.Context(), "BMKG", target, func(context.Context, AcceptedEvent) error { ready <- struct{}{}; <-release; return nil })
			if !worked || err != nil {
				t.Errorf("concurrent ack %s: %v %v", target, worked, err)
			}
		})
	}
	for range 3 {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			close(release)
			group.Wait()
			t.Fatal("projection targets serialized remote work")
		}
	}
	close(release)
	group.Wait()
	var retained int
	if err := c.pool.QueryRow(t.Context(), `SELECT count(*) FROM ingestion_outbox WHERE payload IS NOT NULL OR delivered_at IS NULL`).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("concurrent acks lost cleanup: %d %v", retained, err)
	}
}

func TestPostgresProjectionOrderingIsPerDestinationNotGlobal(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "owner")
	e := testHazard()
	if err := c.AcceptEvent(t.Context(), "BMKG", "owner", lease.Token, &e, "first"); err != nil {
		t.Fatal(err)
	}
	e.Severity = model.SeverityAWAS
	if err := c.AcceptEvent(t.Context(), "BMKG", "owner", lease.Token, &e, "second"); err != nil {
		t.Fatal(err)
	}
	var versions []int64
	for range 2 {
		worked, err := c.DeliverNext(t.Context(), "BMKG", "broker", func(_ context.Context, item AcceptedEvent) error {
			versions = append(versions, item.Version)
			return nil
		})
		if !worked || err != nil {
			t.Fatalf("broker waited for canonical: %v %v", worked, err)
		}
	}
	if versions[0] >= versions[1] {
		t.Fatal("broker identity order regressed")
	}
	target := c.projectionTargets(e)[1]
	worked, err := c.DeliverNext(t.Context(), "BMKG", target, func(_ context.Context, item AcceptedEvent) error {
		if item.Version != versions[0] {
			t.Fatal("canonical skipped oldest unacknowledged version")
		}
		return context.DeadlineExceeded
	})
	if !worked || err == nil {
		t.Fatal("fault not retained")
	}
	worked, err = c.DeliverNext(t.Context(), "BMKG", target, func(context.Context, AcceptedEvent) error {
		t.Fatal("newer version bypassed pending predecessor")
		return nil
	})
	if worked || err != nil {
		t.Fatal(worked, err)
	}
}

func TestPostgresV2ProjectionMigrationPreservesPendingPayload(t *testing.T) {
	c, url := integrationCoordinator(t)
	lease := acquire(t, c, "owner")
	e := testHazard()
	if err := c.AcceptEvent(t.Context(), "BMKG", "owner", lease.Token, &e, "v2-pending"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(t.Context(), `DROP TABLE ingestion_projections;UPDATE coordination_schema SET version=2`); err != nil {
		t.Fatal(err)
	}
	next, err := NewCoordinator(t.Context(), url, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	for _, target := range []string{"broker", "canonical"} {
		worked, err := next.DeliverNext(t.Context(), "BMKG", target, func(_ context.Context, item AcceptedEvent) error {
			if item.Event.SourceRefID != e.SourceRefID || item.CorrelationID != "v2-pending" {
				t.Fatal("legacy payload changed")
			}
			return nil
		})
		if !worked || err != nil {
			t.Fatalf("legacy task %s: %v %v", target, worked, err)
		}
	}
	var retained int
	if err := c.pool.QueryRow(t.Context(), `SELECT count(*) FROM ingestion_outbox WHERE payload IS NOT NULL OR delivered_at IS NULL`).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("legacy completion: %d %v", retained, err)
	}
}
