package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresCanonicalWriteRetriesTransientLockWithoutChangingRevision(t *testing.T) {
	s, _, _ := integrationReplicas(t)
	e := testHazard()
	if err := s.Upsert(t.Context(), &e); err != nil {
		t.Fatal(err)
	}
	u := s.units[s.destinations(partitionFor(string(e.Source), e.SourceRefID))[0]]
	lock, err := u.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(t.Context(), "SELECT hazard_id FROM hazard_events WHERE hazard_id=$1 FOR UPDATE", e.HazardID); err != nil {
		t.Fatal(err)
	}
	released := make(chan error, 1)
	go func() {
		_, waitErr := lock.Exec(t.Context(), "SELECT pg_sleep(0.15)")
		err := lock.Rollback(context.Background())
		released <- errors.Join(waitErr, err)
	}()
	s.timeout = 100 * time.Millisecond
	e.AreaName = "changed during import"
	writeErr := s.Upsert(t.Context(), &e)
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if writeErr != nil {
		t.Fatalf("transient destination lock made the import fail: %v", writeErr)
	}
	for _, index := range s.destinations(partitionFor(string(e.Source), e.SourceRefID)) {
		var revision, count int
		var area string
		if err := s.units[index].pool.QueryRow(t.Context(), "SELECT revision,area_name,(SELECT count(*) FROM hazard_events) FROM hazard_events WHERE hazard_id=$1", e.HazardID).Scan(&revision, &area, &count); err != nil {
			t.Fatal(err)
		}
		if revision != 2 || count != 1 || area != e.AreaName {
			t.Fatalf("retry changed identity/revision or missed a replica: revision=%d count=%d area=%q", revision, count, area)
		}
	}
}

func TestPostgresCanonicalWriteTimeoutIsEnforcedByServer(t *testing.T) {
	s, _, _ := integrationReplicas(t)
	e := testHazard()
	index := s.destinations(partitionFor(string(e.Source), e.SourceRefID))[0]
	u := s.units[index]
	_, err := u.pool.Exec(t.Context(), `CREATE FUNCTION delay_write() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF current_setting('idle_in_transaction_session_timeout')='0' OR current_setting('lock_timeout')='0' THEN
 RAISE EXCEPTION 'canonical transaction has no server-side lock/idle deadline'; END IF;
 PERFORM pg_sleep(2); RETURN NEW; END $$;
 CREATE TRIGGER delay_write BEFORE INSERT ON hazard_events FOR EACH ROW EXECUTE FUNCTION delay_write()`)
	if err != nil {
		t.Fatal(err)
	}
	s.timeout = 200 * time.Millisecond
	r := record{event: e, partition: partitionFor(string(e.Source), e.SourceRefID), revision: 1}
	r.event.HazardID = eventID(string(e.Source), e.SourceRefID)
	err = s.writeRecord(t.Context(), u, r)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "57014" {
		t.Fatalf("expected PostgreSQL statement timeout before client I/O expiry, got %v", err)
	}
	var count int
	if err := u.pool.QueryRow(t.Context(), "SELECT count(*) FROM hazard_events").Scan(&count); err != nil || count != 0 {
		t.Fatalf("timed-out transaction had partial effects: count=%d error=%v", count, err)
	}
}

func TestPostgresCanonicalWriteRetryStopsAtCallerDeadline(t *testing.T) {
	s, _, _ := integrationReplicas(t)
	e := testHazard()
	if err := s.Upsert(t.Context(), &e); err != nil {
		t.Fatal(err)
	}
	u := s.units[s.destinations(partitionFor(string(e.Source), e.SourceRefID))[0]]
	lock, err := u.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(t.Context(), "SELECT hazard_id FROM hazard_events WHERE hazard_id=$1 FOR UPDATE", e.HazardID); err != nil {
		t.Fatal(err)
	}
	s.timeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := s.Upsert(ctx, &e); err == nil {
		t.Fatal("blocked write was falsely acknowledged")
	}
	if time.Since(started) > 300*time.Millisecond {
		t.Fatal("retry outlived the cancelled caller")
	}
}
