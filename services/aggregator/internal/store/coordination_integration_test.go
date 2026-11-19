package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

type retrySignal chan struct{}

func TestPostgresGracefulReleaseCannotRevokeReplacementEpoch(t *testing.T) {
	c, _ := integrationCoordinator(t)
	old, won, err := c.AcquireOrRenewLease(t.Context(), "BMKG", "old", 15*time.Second)
	if err != nil || !won {
		t.Fatalf("acquire old: %v", err)
	}
	if err := c.ReleaseLease(t.Context(), "BMKG", "old", old.Token); err != nil {
		t.Fatal(err)
	}
	next, won, err := c.AcquireOrRenewLease(t.Context(), "BMKG", "new", 15*time.Second)
	if err != nil || !won || next.Token <= old.Token {
		t.Fatalf("replacement epoch: %+v %v", next, err)
	}
	if err := c.ReleaseLease(t.Context(), "BMKG", "old", old.Token); err != nil {
		t.Fatal(err)
	}
	if err := c.ValidateLease(t.Context(), "BMKG", "new", next.Token); err != nil {
		t.Fatalf("stale release revoked replacement: %v", err)
	}
	if err := c.ValidateLease(t.Context(), "BMKG", "old", old.Token); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("released epoch valid: %v", err)
	}
}

func TestPostgresFieldReadBypassesSaturatedRegularPool(t *testing.T) {
	_, url := integrationCoordinator(t)
	s, err := New(t.Context(), []config.StorageUnit{{Name: "test", DatabaseURL: url}}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var held []*pgxpool.Conn
	defer func() {
		for _, conn := range held {
			conn.Release()
		}
	}()
	for range s.units[0].pool.Config().MaxConns {
		conn, err := s.units[0].pool.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	ctx, cancel := context.WithTimeout(WithReadPriority(t.Context(), true), 500*time.Millisecond)
	defer cancel()
	if _, err := s.ListEvents(ctx, EventFilter{Limit: 1}); err != nil {
		t.Fatalf("field read waited for regular connections: %v", err)
	}
}

// A PostgreSQL bootstrap may create metadata before the Aggregator's schema
// migration completes. Reachable metadata does not prove the event table exists.
func TestPostgresRecoveryInitializesMetadataOnlyUnit(t *testing.T) {
	c, _ := integrationCoordinator(t)
	_, err := c.pool.Exec(t.Context(), `CREATE TABLE p1_metadata (
 singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
 unit_name TEXT NOT NULL,node_name TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'new',
 checkpoint TEXT NOT NULL DEFAULT '',copied BIGINT NOT NULL DEFAULT 0);
 INSERT INTO p1_metadata(singleton,unit_name) VALUES(true,'metadata-only')`)
	if err != nil {
		t.Fatal(err)
	}
	u := &unit{name: "metadata-only", pool: c.pool, probe: c.pool, online: true, ready: false}
	s := &Store{units: []*unit{u}, timeout: 2 * time.Second,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.recoverCycle(t.Context())
	var schemaExists bool
	if err := c.pool.QueryRow(t.Context(), `SELECT to_regclass('hazard_events') IS NOT NULL`).Scan(&schemaExists); err != nil {
		t.Fatal(err)
	}
	if !schemaExists || !u.ready {
		t.Fatalf("recovery did not initialize the canonical schema: schema=%v ready=%v", schemaExists, u.ready)
	}
	event := testHazard()
	if err := s.Upsert(t.Context(), &event); err != nil {
		t.Fatalf("recovered store cannot accept events: %v", err)
	}
}

func (s retrySignal) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "coordination_startup_retry") {
		select {
		case s <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}

func TestPostgresCoordinatorRetriesTransientStartupContention(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	// Simulate an older, unversioned database that actually needs migration.
	if _, err := c.pool.Exec(t.Context(), `DROP TABLE IF EXISTS coordination_schema`); err != nil {
		t.Fatal(err)
	}
	tx, err := c.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock(40310005)`); err != nil {
		t.Fatal(err)
	}
	retried := make(retrySignal, 1)
	finished := make(chan error, 1)
	go func() {
		next, err := NewCoordinator(t.Context(), databaseURL, 200*time.Millisecond,
			slog.New(slog.NewTextHandler(retried, nil)))
		if next != nil {
			next.Close()
		}
		finished <- err
	}()
	select {
	case <-retried:
	case err := <-finished:
		t.Fatalf("startup ended without retry while migration was locked: %v", err)
	case <-time.After(5 * time.Second):
		_ = tx.Rollback(context.Background())
		<-finished
		t.Fatal("startup retry was not observed")
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("startup did not recover after migration lock released: %v", err)
	}
}

func TestPostgresCoordinatorStartsWithoutMigrationLock(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	tx, err := c.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock(40310005)`); err != nil {
		t.Fatal(err)
	}
	startupCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	next, err := NewCoordinator(startupCtx, databaseURL, 200*time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if next != nil {
		next.Close()
	}
	if err != nil {
		t.Fatalf("a current database must not take migration locks at startup: %v", err)
	}
}

func TestPostgresCoordinatorLegacyMigrationPreservesStateAndRejectsFutureSchema(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	ctx := t.Context()
	lease := acquire(t, c, "existing-owner")
	event := testHazard()
	if err := c.AcceptEvent(ctx, "BMKG", "existing-owner", lease.Token, &event, "migration-regression"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(ctx, `DROP TABLE coordination_schema`); err != nil {
		t.Fatal(err)
	}
	next, err := NewCoordinator(ctx, databaseURL, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	next.Close()
	var leases, intents, version int
	if err := c.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM source_leases),(SELECT count(*) FROM ingestion_outbox),(SELECT version FROM coordination_schema WHERE singleton=true)`).Scan(&leases, &intents, &version); err != nil || leases != 1 || intents != 1 || version != 3 {
		t.Fatalf("legacy migration changed existing state: leases=%d intents=%d version=%d err=%v", leases, intents, version, err)
	}
	if _, err := c.pool.Exec(ctx, `UPDATE coordination_schema SET version=4`); err != nil {
		t.Fatal(err)
	}
	next, err = NewCoordinator(ctx, databaseURL, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if next != nil {
		next.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "unsupported coordination schema version") {
		t.Fatalf("future schema was accepted or downgraded: %v", err)
	}
	if err := c.pool.QueryRow(ctx, `SELECT version FROM coordination_schema WHERE singleton=true`).Scan(&version); err != nil || version != 4 {
		t.Fatalf("future schema marker was changed: version=%d err=%v", version, err)
	}
}

// Each test owns a random schema. No existing application table is touched.
func integrationCoordinator(t *testing.T) (*Coordinator, string) {
	t.Helper()
	rawURL := os.Getenv("P2_TEST_DATABASE_URL")
	if rawURL == "" {
		t.Skip("set P2_TEST_DATABASE_URL to run PostgreSQL failure/concurrency tests")
	}
	ctx := t.Context()
	admin, err := pgxpool.New(ctx, rawURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p2_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		if err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	coordinator, err := NewCoordinator(ctx, u.String(), 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coordinator.Close)
	return coordinator, u.String()
}

func acquire(t *testing.T, c *Coordinator, holder string) Lease {
	t.Helper()
	lease, won, err := c.AcquireOrRenewLease(t.Context(), "BMKG", holder, 15*time.Second)
	if err != nil || !won {
		t.Fatalf("acquire lease: won=%v err=%v", won, err)
	}
	return lease
}

func expire(t *testing.T, c *Coordinator) {
	t.Helper()
	if _, err := c.pool.Exec(t.Context(), `UPDATE source_leases SET lease_until=clock_timestamp()-interval '1 second' WHERE source='BMKG'`); err != nil {
		t.Fatal(err)
	}
}

func testHazard() model.HazardEvent {
	return model.HazardEvent{Source: model.SourceBMKG, SourceRefID: "eq-1", HazardType: model.HazardTypeSEISMIC,
		Severity: model.SeverityWASPADA, AreaName: "Padang", OccuredAt: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC),
		Attributes: map[string]any{"magnitude": 5.1}}
}

func TestPostgresSourceClockUsesProgressNotAbsoluteOffset(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	generated := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	if err := c.CheckSourceProgress(ctx, "SENSOR_PANTAI", generated, 30*time.Second); err != nil {
		t.Fatalf("independent source epoch rejected: %v", err)
	}
	if _, err := c.pool.Exec(ctx, `UPDATE source_clock_progress SET advanced_at=clock_timestamp()-interval '31 seconds'`); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckSourceProgress(ctx, "SENSOR_PANTAI", generated, 30*time.Second); !errors.Is(err, ErrSourceClockStale) {
		t.Fatalf("stalled clock not detected: %v", err)
	}
	if err := c.CheckSourceProgress(ctx, "SENSOR_PANTAI", generated.Add(time.Second), 30*time.Second); err != nil {
		t.Fatalf("progress did not recover automatically: %v", err)
	}
	if err := c.CheckSourceProgress(ctx, "SENSOR_PANTAI", generated, 30*time.Second); !errors.Is(err, ErrSourceClockRegression) {
		t.Fatalf("regressing snapshot not detected: %v", err)
	}
}

func TestPostgresBatchAdmissionReplayAndCriticalPriority(t *testing.T) {
	c, _ := integrationCoordinator(t)
	ctx := t.Context()
	lease, won, err := c.AcquireOrRenewLease(ctx, "SENSOR_PANTAI", "coastal-owner", 15*time.Second)
	if err != nil || !won {
		t.Fatalf("acquire coastal lease: %v", err)
	}
	events := []model.HazardEvent{testHazard(), testHazard()}
	for i := range events {
		events[i].Source = model.SourceSENSORPANTAI
		events[i].SourceRefID = fmt.Sprintf("observation-%d", i)
		events[i].HazardType = model.HazardTypeCOASTAL
		events[i].Attributes = map[string]any{"critical": i == 1}
	}
	for range 2 {
		if err := c.AcceptBatch(ctx, "SENSOR_PANTAI", "coastal-owner", lease.Token, events, "batch-corr"); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	_ = c.pool.QueryRow(ctx, `SELECT count(*) FROM ingestion_outbox`).Scan(&count)
	if count != 2 {
		t.Fatalf("page replay enqueued duplicates: %d", count)
	}
	var delivered []string
	for range 2 {
		worked, err := c.DeliverNext(ctx, "SENSOR_PANTAI", "broker", func(_ context.Context, item AcceptedEvent) error {
			if item.Event.IngestedAt.IsZero() || item.CorrelationID != "batch-corr" {
				t.Error("missing admission metadata")
			}
			delivered = append(delivered, item.Event.SourceRefID)
			return nil
		})
		if err != nil || !worked {
			t.Fatalf("delivery: worked=%v err=%v", worked, err)
		}
	}
	if delivered[0] != "observation-1" || delivered[1] != "observation-0" {
		t.Fatalf("critical did not overtake normal backlog: %v", delivered)
	}
	if _, err := c.pool.Exec(ctx, `UPDATE source_leases SET lease_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	events[0].Attributes["changed"] = true
	if err := c.AcceptBatch(ctx, "SENSOR_PANTAI", "coastal-owner", lease.Token, events, "stale-corr"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired batch accepted: %v", err)
	}
}

func TestPostgresOnlyOneLeaseWinner(t *testing.T) {
	c, _ := integrationCoordinator(t)
	var group sync.WaitGroup
	winners := make(chan bool, 12)
	for index := range 12 {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			_, won, err := c.AcquireOrRenewLease(t.Context(), "BMKG", fmt.Sprintf("pod-%d", index), 15*time.Second)
			if err != nil {
				t.Error(err)
			}
			winners <- won
		}(index)
	}
	group.Wait()
	close(winners)
	count := 0
	for won := range winners {
		if won {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("got %d leaders", count)
	}
}

func TestPostgresRejectsFrozenOwnerBeforeAnyShardSeesNewToken(t *testing.T) {
	c, _ := integrationCoordinator(t)
	old := acquire(t, c, "old-pod")
	if err := c.ValidateLease(t.Context(), "BMKG", "old-pod", old.Token); err != nil {
		t.Fatal(err)
	}
	// Pause after the old application's validation; replacement does NO shard write.
	expire(t, c)
	newLease := acquire(t, c, "new-pod")
	if newLease.Token <= old.Token {
		t.Fatal("replacement did not advance epoch")
	}
	event := testHazard()
	if err := c.AcceptEvent(t.Context(), "BMKG", "old-pod", old.Token, &event, "stale"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale admission: %v", err)
	}
	var count int
	if err := c.pool.QueryRow(t.Context(), `SELECT count(*) FROM ingestion_outbox`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("stale admission changed outbox: count=%d err=%v", count, err)
	}
	if err := c.AcceptEvent(t.Context(), "BMKG", "new-pod", newLease.Token, &event, "new"); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRenewalCannotReviveExpiredEpoch(t *testing.T) {
	c, _ := integrationCoordinator(t)
	old := acquire(t, c, "same-pod")
	expire(t, c)
	if err := c.RenewLease(t.Context(), "BMKG", "same-pod", old.Token, 15*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renew expired epoch: %v", err)
	}
	current := acquire(t, c, "same-pod")
	if current.Token <= old.Token {
		t.Fatal("reacquisition reused expired epoch")
	}
	if err := c.ValidateLease(t.Context(), "BMKG", "same-pod", old.Token); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old epoch revived: %v", err)
	}
}

func TestPostgresCursorErrorsDoNotRestartFromBeginning(t *testing.T) {
	c, _ := integrationCoordinator(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.TimeCursor(canceled, "BMKG", time.Unix(0, 0)); err == nil {
		t.Fatal("time cursor swallowed database error")
	}
	if _, err := c.OpaqueCursor(canceled, "BMKG"); err == nil {
		t.Fatal("opaque cursor swallowed database error")
	}
	old := acquire(t, c, "old-pod")
	first := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	if err := c.AdvanceTimeCursor(t.Context(), "BMKG", "old-pod", old.Token, first); err != nil {
		t.Fatal(err)
	}
	if err := c.AdvanceTimeCursor(t.Context(), "BMKG", "old-pod", old.Token, first.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, err := c.TimeCursor(t.Context(), "BMKG", time.Time{})
	if err != nil || !got.Equal(first) {
		t.Fatalf("cursor regressed: %v %v", got, err)
	}
	expire(t, c)
	acquire(t, c, "new-pod")
	if err := c.AdvanceTimeCursor(t.Context(), "BMKG", "old-pod", old.Token, first.Add(time.Hour)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale cursor write: %v", err)
	}
}

func TestPostgresAdmissionReplayAndDeliveryRetry(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "pod")
	event := testHazard()
	for range 2 {
		if err := c.AcceptEvent(t.Context(), "BMKG", "pod", lease.Token, &event, "corr"); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := c.pool.QueryRow(t.Context(), `SELECT count(*) FROM ingestion_outbox`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("replayed admission duplicated: %d %v", count, err)
	}
	var firstVersion int64
	worked, err := c.DeliverNext(t.Context(), "BMKG", "broker", func(_ context.Context, item AcceptedEvent) error {
		firstVersion = item.Version
		return fmt.Errorf("simulated crash after remote write")
	})
	if !worked || err == nil {
		t.Fatalf("failed delivery not retained: worked=%v err=%v", worked, err)
	}
	if _, err := c.pool.Exec(t.Context(), `UPDATE ingestion_projections SET next_attempt_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	worked, err = c.DeliverNext(t.Context(), "BMKG", "broker", func(_ context.Context, item AcceptedEvent) error {
		if item.Version != firstVersion {
			t.Fatal("retry changed admission identity")
		}
		return nil
	})
	if !worked || err != nil {
		t.Fatalf("retry: worked=%v err=%v", worked, err)
	}
	worked, err = c.DeliverNext(t.Context(), "BMKG", "broker", func(context.Context, AcceptedEvent) error {
		t.Fatal("delivered twice after acknowledgement")
		return nil
	})
	if worked || err != nil {
		t.Fatalf("completed outbox: %v %v", worked, err)
	}
	for _, target := range c.projectionTargets(event)[1:] {
		worked, err := c.DeliverNext(t.Context(), "BMKG", target, func(context.Context, AcceptedEvent) error { return nil })
		if !worked || err != nil {
			t.Fatalf("canonical target %s: %v %v", target, worked, err)
		}
	}
	var retained int
	if err := c.pool.QueryRow(t.Context(), `SELECT count(*) FROM ingestion_outbox WHERE payload IS NOT NULL`).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("delivered payload retained: %d %v", retained, err)
	}
}

func TestPostgresServerKillsAbandonedLeaseTransaction(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "pod")
	tx, err := c.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), `SET LOCAL idle_in_transaction_session_timeout='100ms'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `UPDATE source_leases SET updated_at=clock_timestamp() WHERE source='BMKG'`); err != nil {
		t.Fatal(err)
	}
	// Wait using a second backend, while the first backend's transaction is idle.
	if _, err := c.pool.Exec(t.Context(), `SELECT pg_sleep(0.25)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err == nil {
		t.Fatal("suspended client committed abandoned admission")
	}
	if err := c.ValidateLease(t.Context(), "BMKG", "pod", lease.Token); err != nil {
		t.Fatal(err)
	}
	if err := c.RenewLease(t.Context(), "BMKG", "pod", lease.Token, 15*time.Second); err != nil {
		t.Fatal("abandoned transaction retained row lock:", err)
	}
}

func TestPostgresInterruptedAdmissionRollsBackEntireIntent(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "pod")
	// Fault injection INSIDE the real transaction, after its lease validation.
	_, err := c.pool.Exec(t.Context(), `CREATE FUNCTION delay_admission() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN PERFORM pg_sleep(3); RETURN NEW; END $$;
 CREATE TRIGGER delay_admission BEFORE INSERT ON ingestion_outbox FOR EACH ROW EXECUTE FUNCTION delay_admission()`)
	if err != nil {
		t.Fatal(err)
	}
	event := testHazard()
	if err := c.AcceptEvent(t.Context(), "BMKG", "pod", lease.Token, &event, "interrupted"); err == nil {
		t.Fatal("interrupted admission committed")
	}
	var count int
	if err := c.pool.QueryRow(t.Context(), `SELECT count(*) FROM ingestion_outbox`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial admission survived: %d %v", count, err)
	}
	if _, err := c.pool.Exec(t.Context(), `DROP TRIGGER delay_admission ON ingestion_outbox`); err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptEvent(t.Context(), "BMKG", "pod", lease.Token, &event, "retry"); err != nil {
		t.Fatal("failed to recover admission after interrupted transaction:", err)
	}
}

func TestPostgresProjectionRejectsOutOfOrderDelivery(t *testing.T) {
	_, databaseURL := integrationCoordinator(t)
	s, err := New(t.Context(), []config.StorageUnit{{Name: "store-a", DatabaseURL: databaseURL}}, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	newer := testHazard()
	newer.Severity = model.SeverityAWAS
	newer.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(t.Context(), &newer, 20); err != nil {
		t.Fatal(err)
	}
	older := testHazard()
	older.IngestedAt = newer.IngestedAt.Add(-time.Second)
	if err := s.UpsertAccepted(t.Context(), &older, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertAccepted(t.Context(), &newer, 20); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetEventBySourceRef(t.Context(), "BMKG", "eq-1")
	if err != nil || got.Event == nil || got.Event.Severity != model.SeverityAWAS {
		t.Fatalf("late replay overwrote new event: %+v %v", got, err)
	}
}

func TestPostgresReadReplicaStartupDoesNotRepairHealthyStorage(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	ctx := t.Context()
	settings := []config.StorageUnit{{Name: "store-a", DatabaseURL: databaseURL}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := New(ctx, settings, 2*time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	event := testHazard()
	event.IngestedAt = time.Now().UTC()
	if err := first.UpsertAccepted(ctx, &event, 1); err != nil {
		t.Fatal(err)
	}
	// Starting another read instance must not even temporarily withdraw an
	// already healthy shared storage unit from the existing readers.
	_, err = c.pool.Exec(ctx, `CREATE FUNCTION reject_unneeded_recovery() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF OLD.status='ready' AND NEW.status='recovering' THEN RAISE EXCEPTION 'healthy store withdrawn'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER no_startup_repair BEFORE UPDATE ON p1_metadata FOR EACH ROW EXECUTE FUNCTION reject_unneeded_recovery()`)
	if err != nil {
		t.Fatal(err)
	}
	readTransaction, err := c.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer readTransaction.Rollback(context.Background())
	if _, err := readTransaction.Exec(ctx, `SELECT 1 FROM hazard_events LIMIT 0`); err != nil {
		t.Fatal(err)
	}
	second, err := New(ctx, settings, 2*time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := readTransaction.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := second.GetEventBySourceRef(ctx, "BMKG", event.SourceRefID)
	if err != nil || got.Event == nil || got.Partial {
		t.Fatalf("new replica withdrew healthy data: %+v %v", got, err)
	}
}

func TestPostgresRepairBatchAtomicAndVersionGuarded(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	ctx := t.Context()
	s, err := New(ctx, []config.StorageUnit{{Name: "store-a", DatabaseURL: databaseURL}}, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = c.pool.Exec(ctx, `CREATE TABLE repair_transactions (id bigint PRIMARY KEY);
 CREATE FUNCTION record_repair_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN INSERT INTO repair_transactions VALUES(txid_current()) ON CONFLICT DO NOTHING; RETURN NEW; END $$;
 CREATE TRIGGER repair_tx BEFORE INSERT ON hazard_events FOR EACH ROW EXECUTE FUNCTION record_repair_transaction();
 ALTER TABLE hazard_events ADD CONSTRAINT reject_fault CHECK(source_ref_id <> 'injected-fault')`)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]record, 25)
	for i := range records {
		e := testHazard()
		e.SourceRefID = fmt.Sprintf("repair-%d", i)
		e.HazardID = eventID(string(e.Source), e.SourceRefID)
		e.IngestedAt = time.Now().UTC()
		records[i] = record{event: e, partition: 0, revision: 10, ingestionVersion: 10}
	}
	records[len(records)-1].event.SourceRefID = "injected-fault"
	if err := s.writeRecords(ctx, s.units[0], records); err == nil {
		t.Fatal("fault in the final row must abort the entire page")
	}
	var rows, transactions int
	if err := c.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM hazard_events),(SELECT count(*) FROM repair_transactions)`).Scan(&rows, &transactions); err != nil || rows != 0 || transactions != 0 {
		t.Fatalf("partial repair committed: rows=%d tx=%d err=%v", rows, transactions, err)
	}
	records[len(records)-1].event.SourceRefID = "repair-last"
	if err := s.writeRecords(ctx, s.units[0], records); err != nil {
		t.Fatal(err)
	}
	if err := c.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM hazard_events),(SELECT count(*) FROM repair_transactions)`).Scan(&rows, &transactions); err != nil || rows != len(records) || transactions != 1 {
		t.Fatalf("repair page must use one durable transaction: rows=%d tx=%d err=%v", rows, transactions, err)
	}
	newer := records[0].event
	newer.Severity = model.SeverityAWAS
	if err := s.UpsertAccepted(ctx, &newer, 20); err != nil {
		t.Fatal(err)
	}
	if err := s.writeRecords(ctx, s.units[0], records); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetEventByID(ctx, newer.HazardID)
	if err != nil || got.Event == nil || got.Event.Severity != model.SeverityAWAS {
		t.Fatalf("replayed repair overwrote newer live intent: %+v %v", got, err)
	}
}

func TestPostgresReadBudgetDoesNotWithdrawHealthyStore(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	ctx := t.Context()
	s, err := New(ctx, []config.StorageUnit{{Name: "store-a", DatabaseURL: databaseURL}}, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(ctx, &e, 1); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _ = s.ListEvents(canceled, EventFilter{Limit: 10})
	if s.readStatus().Partial {
		t.Fatal("a canceled client request withdrew healthy shared storage")
	}
	// A blocked read is not evidence of a lost disk. Neither a lock wait nor
	// pool contention may poison all following requests or trigger repair.
	lock, err := c.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(ctx, `LOCK TABLE hazard_events IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	s.timeout = 100 * time.Millisecond
	_, err = s.ListEvents(ctx, EventFilter{Limit: 10})
	if err == nil || s.readStatus().Partial {
		t.Fatalf("busy read must fail explicitly without poisoning health: err=%v state=%+v", err, s.readStatus())
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	s.timeout = 2 * time.Second
	got, err := s.GetEventByID(ctx, e.HazardID)
	if err != nil || got.Event == nil || got.Partial {
		t.Fatalf("healthy data did not return immediately after contention ended: %+v %v", got, err)
	}
}

func TestPostgresProbeContentionDoesNotWithdrawReadableData(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	ctx := t.Context()
	s, err := New(ctx, []config.StorageUnit{{Name: "store-a", DatabaseURL: databaseURL}}, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(ctx, &e, 1); err != nil {
		t.Fatal(err)
	}
	lock, err := c.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(ctx, `LOCK TABLE p1_metadata IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	s.timeout = 100 * time.Millisecond
	for range 2 {
		s.refreshUnitStates(ctx)
		page, err := s.ListEvents(ctx, EventFilter{Limit: 10})
		if err != nil || page.Partial || len(page.Events) != 1 {
			t.Fatalf("ambiguous metadata probe timeout withdrew readable data: %+v %v", page, err)
		}
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	s.timeout = 2 * time.Second
	s.refreshUnitStates(ctx)
	if s.units[0].probeTimeouts != 0 || s.readStatus().Partial {
		t.Fatal("successful probe did not reset consecutive timeout evidence")
	}
}
