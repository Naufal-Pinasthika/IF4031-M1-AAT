package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// p1 writer lock serializes revisions state lock never spans network I/O
type Store struct {
	units       []*unit
	timeout     time.Duration
	logger      *slog.Logger
	writer      sync.Mutex
	state       sync.RWMutex
	recovery    sync.Mutex
	antiEntropy map[[2]int]string // elected recovery worker only; restart safely rescans
}

// p1 all mutable fields are guarded by Store.state.
type unit struct {
	name                        string
	pool                        *pgxpool.Pool
	probe                       *pgxpool.Pool
	priority                    *pgxpool.Pool
	maintenance                 *pgxpool.Pool
	readSlots                   chan struct{}
	repairing                   bool
	online, ready               bool
	generation                  uint64
	writes, writeErrors, copied int64
	writeDuration               time.Duration
	checkpoint                  string
	probeTimeouts               int
}

// p1 a PostgreSQL row carries its internal revision during repair.
type record struct {
	event            model.HazardEvent
	partition        int
	revision         int64
	fencingToken     int64
	ingestionVersion int64
}

// p1 startup accepts unavailable units existing ready units are reconciled before serving HTTP
func New(ctx context.Context, configs []config.StorageUnit, timeout time.Duration, logger *slog.Logger) (*Store, error) {
	if len(configs) != 1 && len(configs) != 3 {
		return nil, fmt.Errorf("storage requires one or three units")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("storage timeout must be positive")
	}
	s := &Store{timeout: timeout, logger: logger}
	fresh := 0
	for _, cfg := range configs {
		pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("invalid storage configuration for %s", cfg.Name)
		}
		pc.ConnConfig.Tracer = &postgresTracer{logger: logger.With("storage_unit", cfg.Name)}
		pc.ConnConfig.ConnectTimeout = timeout
		// Reserve one EXISTING regular connection for maintenance. Do not grow
		// the per-unit connection ceiling when adding the recovery bulkhead.
		maintenanceConfig := pc.Copy()
		dedicatedMaintenance := pc.MaxConns > 1
		if dedicatedMaintenance {
			pc.MaxConns--
			maintenanceConfig.MaxConns = 1
			maintenanceConfig.MinConns = 0
			pc.MinConns = min(pc.MinConns, pc.MaxConns)
		}
		p, err := pgxpool.NewWithConfig(ctx, pc)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("create pool for %s", cfg.Name)
		}
		// A separate small pool prevents regular reads/audits from consuming
		// every connection needed by the authenticated Field path.
		priorityConfig := pc.Copy()
		priorityConfig.MaxConns = 2
		priorityConfig.MinConns = 0
		priority, err := pgxpool.NewWithConfig(ctx, priorityConfig)
		if err != nil {
			p.Close()
			s.Close()
			return nil, fmt.Errorf("create priority pool for %s", cfg.Name)
		}
		probeConfig := pc.Copy()
		probeConfig.MaxConns = 1
		probeConfig.MinConns = 0
		probe, err := pgxpool.NewWithConfig(ctx, probeConfig)
		if err != nil {
			p.Close()
			priority.Close()
			s.Close()
			return nil, fmt.Errorf("create health pool for %s", cfg.Name)
		}
		u := &unit{name: cfg.Name, pool: p, probe: probe, priority: priority,
			readSlots: make(chan struct{}, max(1, pc.MaxConns-1))}
		s.units = append(s.units, u)
		if dedicatedMaintenance {
			maintenance, err := pgxpool.NewWithConfig(ctx, maintenanceConfig)
			if err != nil {
				s.Close()
				return nil, fmt.Errorf("create maintenance pool for %s", cfg.Name)
			}
			u.maintenance = maintenance
		}
		op, cancel := context.WithTimeout(ctx, timeout)
		err = s.ensureSchema(op, u)
		if err == nil {
			u.online = true
			var status string
			err = u.pool.QueryRow(op, "SELECT status FROM p1_metadata WHERE singleton=true").Scan(&status)
			if err == nil {
				u.ready = status == "ready"
				if status == "new" {
					fresh++
				}
			}
		}
		cancel()
		if err != nil && !availabilityError(err) {
			s.Close()
			return nil, fmt.Errorf("initialize %s: %w", u.name, err)
		}
	}
	if fresh == len(s.units) {
		// Only genuinely empty first-use volumes can be activated immediately.
		// Existing volumes are repaired by the elected recovery worker below.
		empty := true
		for _, u := range s.units {
			op, cancel := context.WithTimeout(ctx, timeout)
			var hasRows bool
			err := u.pool.QueryRow(op, "SELECT EXISTS(SELECT 1 FROM hazard_events LIMIT 1)").Scan(&hasRows)
			cancel()
			empty = empty && err == nil && !hasRows
		}
		if empty {
			for _, u := range s.units {
				op, cancel := context.WithTimeout(ctx, timeout)
				_, err := u.pool.Exec(op, "UPDATE p1_metadata SET status='ready' WHERE singleton=true AND status='new'")
				cancel()
				u.ready = err == nil
			}
		}
	}
	if len(s.units) == 1 && s.units[0].online {
		s.units[0].ready = true
	}
	// Starting an additional read replica must NOT mark healthy shared stores
	// 'recovering' and recopy the dataset. All nonempty repairs are performed
	// by RunRecovery after acquiring the shared P1_RECOVERY lease. This also
	// prevents simultaneous startup/rolling pods from interfering with readers.
	return s, nil
}

// p1
func (s *Store) Close() {
	for _, u := range s.units {
		u.pool.Close()
		u.probe.Close()
		if u.priority != nil {
			u.priority.Close()
		}
		if u.maintenance != nil {
			u.maintenance.Close()
		}
	}
}

// Ping succeeds when at least one canonical storage unit is reachable. Reads
// can remain explicitly partial while a failed P1 replica is recovering.
func (s *Store) Ping(ctx context.Context) error {
	var lastErr error
	for _, u := range s.units {
		op, cancel := context.WithTimeout(ctx, s.timeout)
		err := u.probe.Ping(op)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return fmt.Errorf("no storage units configured")
	}
	return lastErr
}

// p1
func (s *Store) destinations(partition int) []int {
	if len(s.units) == 1 {
		return []int{0}
	}
	return []int{partition, (partition + 1) % 3}
}

// p1 errors in application SQL must not masquerade as an unavailable node
func availabilityError(err error) bool {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		return pgerr.Code == "55000" && pgerr.Severity == "FATAL" || pgerr.Code == "57P01" || pgerr.Code == "57P02" || pgerr.Code == "57P03" || pgerr.Code == "42P01" || len(pgerr.Code) >= 2 && pgerr.Code[:2] == "08"
	}
	var connectError *pgconn.ConnectError
	var networkError net.Error
	return errors.As(err, &connectError) || errors.As(err, &networkError) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || pgconn.SafeToRetry(err)
}

// p1
func (s *Store) markDown(u *unit) {
	s.state.Lock()
	wasOnline := withdrawUnit(u)
	s.state.Unlock()
	if wasOnline {
		s.logger.Warn("storage_unit_unreachable", "storage_unit", u.name)
	}
}

// Caller holds Store.state. Keep health transitions atomic with generation
// checks so a stale probe cannot withdraw a newer successful repair epoch.
func withdrawUnit(u *unit) bool {
	wasOnline := u.online
	u.online = false
	u.ready = false
	u.generation++
	u.checkpoint = ""
	return wasOnline
}

// A single probe deadline can mean scheduler/pool pressure, not a failed disk.
// Only ambiguous deadlines require three consecutive observations (5 s cadence).
// Definitive connection/schema failures and application I/O failures retain the
// immediate down transition. Reads still attempt bounded live I/O in this window;
// this does not manufacture a successful response from an unavailable database.
func (s *Store) noteProbeFailure(ctx context.Context, u *unit, err error, observedGeneration ...uint64) {
	if ctx.Err() != nil {
		return
	}
	s.state.Lock()
	if len(observedGeneration) > 0 && u.generation != observedGeneration[0] {
		s.state.Unlock()
		return
	}
	ambiguous := budgetError(err)
	if ambiguous {
		u.probeTimeouts++
	}
	count := u.probeTimeouts
	wasOnline := false
	if !ambiguous || count >= 3 {
		wasOnline = withdrawUnit(u)
	}
	s.state.Unlock()
	if ambiguous {
		s.logger.Warn("storage_probe_timeout", "storage_unit", u.name, "consecutive", count)
	}
	if wasOnline {
		s.logger.Warn("storage_unit_unreachable", "storage_unit", u.name)
	}
}

// A client's cancellation or exhausted query budget says nothing about disk
// health. Only the independent probe (which cannot queue behind application
// traffic) may turn a probe timeout into an availability-state transition.
func budgetError(err error) bool {
	var networkError net.Error
	var postgresError *pgconn.PgError
	return errors.Is(err, ErrStorageBusy) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) ||
		errors.As(err, &postgresError) && postgresError.Code == "57014" ||
		errors.As(err, &networkError) && networkError.Timeout()
}

func (s *Store) noteIOFailure(u *unit, err error) {
	if !budgetError(err) {
		s.markDown(u)
	}
}

// p1 source identities determine placement independently of geography
func (s *Store) Upsert(ctx context.Context, event *model.HazardEvent) error {
	return s.upsert(ctx, event, 0)
}

// UpsertAccepted projects ONLY a committed coordinator intent. The immutable
// admission version protects every replica against delayed/repeated delivery.
func (s *Store) UpsertAccepted(ctx context.Context, event *model.HazardEvent, version int64) error {
	if version <= 0 {
		return fmt.Errorf("positive admission version is required")
	}
	if event == nil || event.IngestedAt.IsZero() {
		return fmt.Errorf("accepted event must have its authoritative ingestion time")
	}
	return s.upsert(ctx, event, version)
}

func (s *Store) upsert(ctx context.Context, event *model.HazardEvent, ingestionVersion int64) error {
	if event == nil || event.Source == "" || event.SourceRefID == "" {
		return fmt.Errorf("hazard event source and source_ref_id are required")
	}
	if _, err := json.Marshal(event.Attributes); err != nil {
		return fmt.Errorf("invalid attributes: %w", err)
	}
	s.writer.Lock()
	defer s.writer.Unlock()
	event.HazardID = eventID(string(event.Source), event.SourceRefID)
	partition := partitionFor(string(event.Source), event.SourceRefID)
	if len(s.units) == 1 {
		partition = 0
	}
	var revision int64
	for _, i := range s.destinations(partition) {
		if ingestionVersion > 0 {
			break
		}
		u := s.units[i]
		s.state.RLock()
		online := u.online
		s.state.RUnlock()
		if !online {
			continue
		}
		op, cancel := context.WithTimeout(ctx, s.timeout)
		var rev int64
		err := u.pool.QueryRow(op, "SELECT revision FROM hazard_events WHERE hazard_id=$1", event.HazardID).Scan(&rev)
		cancel()
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			if !availabilityError(err) {
				return err
			}
			s.noteIOFailure(u, err)
			continue
		}
		if rev > revision {
			revision = rev
		}
	}
	if ingestionVersion == 0 {
		event.IngestedAt = time.Now().UTC()
	} else {
		revision = ingestionVersion - 1
	}
	r := record{event: *event, partition: partition, revision: revision + 1, ingestionVersion: ingestionVersion}
	successes := 0
	for _, i := range s.destinations(partition) {
		u := s.units[i]
		s.state.RLock()
		online := u.online
		s.state.RUnlock()
		if !online {
			continue
		}
		started := time.Now()
		err := s.writeRecord(ctx, u, r)
		s.state.Lock()
		u.writeDuration += time.Since(started)
		if err == nil {
			u.writes++
		} else {
			u.writeErrors++
		}
		s.state.Unlock()
		if err != nil {
			if !availabilityError(err) {
				return err
			}
			s.noteIOFailure(u, err)
			continue
		}
		successes++
	}
	if successes != len(s.destinations(partition)) {
		return fmt.Errorf("canonical replication incomplete: %d/%d destinations acknowledged", successes, len(s.destinations(partition)))
	}
	return nil
}

// UpsertAcceptedTarget acknowledges EXACTLY one assigned replica. A timeout,
// cancellation, unavailable pool, or ambiguous commit always returns an error;
// the coordinator retains this destination's immutable repair intent. Accepted
// versions use a database-side guard, not the process-local revision mutex.
func (s *Store) UpsertAcceptedTarget(ctx context.Context, event *model.HazardEvent, version int64, target string) error {
	if version <= 0 || event == nil || event.Source == "" || event.SourceRefID == "" || event.IngestedAt.IsZero() {
		return fmt.Errorf("valid committed event and positive version are required")
	}
	if _, err := json.Marshal(event.Attributes); err != nil {
		return err
	}
	partition := partitionFor(string(event.Source), event.SourceRefID)
	if len(s.units) == 1 {
		partition = 0
	}
	for _, index := range s.destinations(partition) {
		u := s.units[index]
		if "canonical:"+u.name != target {
			continue
		}
		event.HazardID = eventID(string(event.Source), event.SourceRefID)
		started := time.Now()
		err := s.writeRecord(ctx, u, record{event: *event, partition: partition, revision: version, ingestionVersion: version})
		s.state.Lock()
		u.writeDuration += time.Since(started)
		if err == nil {
			u.writes++
		} else {
			u.writeErrors++
		}
		s.state.Unlock()
		if err != nil && availabilityError(err) {
			s.noteIOFailure(u, err)
		}
		return err
	}
	return fmt.Errorf("target %q is not assigned to this event", target)
}

// p1 revision guard makes copying old rows safe alongside new writes
func (s *Store) writeRecord(ctx context.Context, u *unit, r record) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.writeRecords(ctx, u, []record{r})
		if err == nil || attempt == 1 || ctx.Err() != nil || !retryableCanonicalWrite(err) {
			return err
		}
		// A lost commit acknowledgement may already have stored this record.
		// Retry the SAME identity, ingestion time and revision: the SQL version
		// guard makes replay a no-op, rather than manufacturing another update.
		s.logger.Warn("canonical_write_retry", "correlation_id", correlation.ID(ctx), "storage_unit", u.name, "attempt", attempt+1, "error", err)
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func retryableCanonicalWrite(err error) bool {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		code := postgresError.Code
		return code == "40001" || code == "40P01" || code == "55P03" || code == "57014" || code == "57P01" ||
			code == "57P02" || code == "57P03" || len(code) >= 2 && code[:2] == "08"
	}
	return availabilityError(err)
}

// Repair pages are bounded and committed atomically, rather than paying a
// network round trip and durable commit for every row. The same version guard
// is used by live projection, so an old repair cannot overwrite a new intent.
func (s *Store) writeRecords(ctx context.Context, u *unit, records []record) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) > 100 {
		return fmt.Errorf("storage write batch exceeds 100 records")
	}
	batch := &pgx.Batch{}
	for _, r := range records {
		attrs, err := json.Marshal(r.event.Attributes)
		if err != nil {
			return err
		}
		e := r.event
		batch.Queue(writeRecordSQL, e.HazardID, e.Source, e.SourceRefID, e.HazardType, e.Severity, e.AreaName, e.Latitude, e.Longitude, e.OccuredAt, e.IngestedAt, attrs, r.partition, r.revision, r.fencingToken, r.ingestionVersion)
	}

	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, err := u.operationPool(ctx).Begin(op)
	if err != nil {
		return err
	}
	defer tx.Rollback(op)
	// Client cancellation alone cannot promptly release server-side locks if
	// the connection/commit acknowledgement is lost. Bound lock acquisition,
	// statement execution and an abandoned transaction on PostgreSQL itself.
	// Leave response time inside the existing client-side I/O budget.
	milliseconds := max(int64(1), s.timeout.Milliseconds())
	if _, err := tx.Exec(op, fmt.Sprintf("SET LOCAL lock_timeout='%dms'; SET LOCAL statement_timeout='%dms'; SET LOCAL idle_in_transaction_session_timeout='%dms'",
		max(int64(1), milliseconds*3/4), max(int64(1), milliseconds*9/10), milliseconds)); err != nil {
		return err
	}
	results := tx.SendBatch(op, batch)
	if err := results.Close(); err != nil {
		return err
	}
	return tx.Commit(op)
}

const writeRecordSQL = `INSERT INTO hazard_events
 (hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,partition_id,revision,fencing_token,ingestion_version)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15)
 ON CONFLICT (source,source_ref_id) DO UPDATE SET
 hazard_id=EXCLUDED.hazard_id,hazard_type=EXCLUDED.hazard_type,severity=EXCLUDED.severity,
 area_name=EXCLUDED.area_name,latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude,
 occurred_at=EXCLUDED.occurred_at,ingested_at=EXCLUDED.ingested_at,attributes=EXCLUDED.attributes,
 partition_id=EXCLUDED.partition_id,revision=EXCLUDED.revision,fencing_token=EXCLUDED.fencing_token,
 ingestion_version=EXCLUDED.ingestion_version
 WHERE hazard_events.ingestion_version < EXCLUDED.ingestion_version
 OR (hazard_events.ingestion_version = EXCLUDED.ingestion_version AND hazard_events.revision < EXCLUDED.revision)`

// new P1 volumes only existing M1 data requires explicit migration.
func (s *Store) ensureSchema(ctx context.Context, u *unit) error {
	// ALTER TABLE takes an ACCESS EXCLUSIVE lock even when ADD COLUMN IF NOT
	// EXISTS is a no-op. A new read replica must not queue DDL ahead of live
	// requests on an already current schema. Keep this probe in sync with
	// future schema versions; missing tables/columns still take the migration.
	pool := u.operationPool(ctx)
	rows, probeErr := pool.Query(ctx, `SELECT partition_id,revision,fencing_token,ingestion_version FROM hazard_events LIMIT 0`)
	if probeErr == nil {
		rows.Close()
		var name string
		probeErr = pool.QueryRow(ctx, `SELECT unit_name FROM p1_metadata WHERE singleton=true`).Scan(&name)
		if probeErr == nil && name == u.name {
			return nil
		}
	}
	if probeErr != nil && !errors.Is(probeErr, pgx.ErrNoRows) {
		var postgresError *pgconn.PgError
		if !errors.As(probeErr, &postgresError) || (postgresError.Code != "42P01" && postgresError.Code != "42703") {
			return probeErr
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(40310004)`); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS hazard_events (
 hazard_id TEXT NOT NULL UNIQUE,source TEXT NOT NULL,source_ref_id TEXT NOT NULL,
 hazard_type TEXT NOT NULL,severity TEXT NOT NULL,area_name TEXT NOT NULL,
 latitude DOUBLE PRECISION NOT NULL,longitude DOUBLE PRECISION NOT NULL,
 occurred_at TIMESTAMPTZ NOT NULL,ingested_at TIMESTAMPTZ NOT NULL,attributes JSONB NOT NULL DEFAULT '{}',
 partition_id INTEGER NOT NULL,revision BIGINT NOT NULL,fencing_token BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(source,source_ref_id));
 ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0;
 ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS ingestion_version BIGINT NOT NULL DEFAULT 0;
 CREATE INDEX IF NOT EXISTS p1_occurred ON hazard_events(partition_id,occurred_at,hazard_id);
 CREATE INDEX IF NOT EXISTS p1_ingested ON hazard_events(partition_id,ingested_at,hazard_id);
 CREATE INDEX IF NOT EXISTS p1_analytics_live ON hazard_events(partition_id,hazard_id)
 INCLUDE(ingestion_version,revision,hazard_type,severity,area_name,occurred_at)
 WHERE COALESCE(attributes->'_retracted','false'::jsonb)<>'true'::jsonb;
 CREATE INDEX IF NOT EXISTS p1_analytics_retracted ON hazard_events(partition_id,hazard_id)
 INCLUDE(ingestion_version,revision,hazard_type,severity,area_name,occurred_at)
 WHERE COALESCE(attributes->'_retracted','false'::jsonb)='true'::jsonb;
 CREATE TABLE IF NOT EXISTS p1_metadata (singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
 unit_name TEXT NOT NULL,node_name TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'new',
 checkpoint TEXT NOT NULL DEFAULT '',copied BIGINT NOT NULL DEFAULT 0);
 INSERT INTO p1_metadata(singleton,unit_name) VALUES(true,'') ON CONFLICT DO NOTHING;
 CREATE INDEX IF NOT EXISTS p1_occurred ON hazard_events(partition_id,occurred_at,hazard_id);
 CREATE INDEX IF NOT EXISTS p1_ingested ON hazard_events(partition_id,ingested_at,hazard_id);
 ;`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS partition_id INTEGER;
 ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS revision BIGINT;
 UPDATE hazard_events SET partition_id=0 WHERE partition_id IS NULL;
 UPDATE hazard_events SET revision=1 WHERE revision IS NULL;`)
	if err != nil {
		return err
	}
	identityRows, err := tx.Query(ctx, `
		SELECT source, source_ref_id
		FROM hazard_events
		WHERE partition_id=0`)
	if err != nil {
		return err
	}
	type existingIdentity struct {
		source, sourceRefID string
	}
	identities := make([]existingIdentity, 0)
	for identityRows.Next() {
		var identity existingIdentity
		if err := identityRows.Scan(&identity.source, &identity.sourceRefID); err != nil {
			identityRows.Close()
			return err
		}
		identities = append(identities, identity)
	}
	identityRows.Close()
	if err := identityRows.Err(); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, identity := range identities {
		partition := partitionFor(identity.source, identity.sourceRefID)
		batch.Queue(`
			UPDATE hazard_events
			SET partition_id=$1
			WHERE source=$2 AND source_ref_id=$3`,
			partition, identity.source, identity.sourceRefID)
	}
	if len(identities) > 0 {
		results := tx.SendBatch(ctx, batch)
		for range identities {
			if _, err := results.Exec(); err != nil {
				results.Close()
				return err
			}
		}
		if err := results.Close(); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, "UPDATE p1_metadata SET unit_name=$1 WHERE singleton=true", u.name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
