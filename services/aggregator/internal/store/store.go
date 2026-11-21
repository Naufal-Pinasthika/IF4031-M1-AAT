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
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// p1 writer lock serializes revisions state lock never spans network I/O
type Store struct {
	units     []*unit
	timeout   time.Duration
	logger    *slog.Logger
	writer    sync.Mutex
	satellite sync.Mutex
	state     sync.RWMutex
	recovery  sync.Mutex
}

// p1 all mutable fields are guarded by Store.state.
type unit struct {
	name                        string
	pool                        *pgxpool.Pool
	online, ready               bool
	generation                  uint64
	writes, writeErrors, copied int64
	writeDuration               time.Duration
	checkpoint                  string
}

// p1 a PostgreSQL row carries its internal revision during repair.
type record struct {
	event     model.HazardEvent
	partition int
	revision  int64
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
		pc.ConnConfig.Tracer = &postgresTracer{logger: logger}
		pc.ConnConfig.ConnectTimeout = timeout
		p, err := pgxpool.NewWithConfig(ctx, pc)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("create pool for %s", cfg.Name)
		}
		u := &unit{name: cfg.Name, pool: p}
		s.units = append(s.units, u)
		op, cancel := context.WithTimeout(ctx, timeout)
		err = s.ensureSchema(op, u)
		if err == nil {
			err = s.ensureSatelliteSchema(op, u)
		}
		if err == nil {
			err = s.ensureOutboxSchema(op, u)
		}
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
		for _, u := range s.units {
			u.ready = true
		}
	}
	if len(s.units) == 1 && s.units[0].online {
		s.units[0].ready = true
	}
	for i, u := range s.units {
		if len(s.units) == 1 {
			u.ready = true
			continue
		}
		if u.online {
			s.recoverUnit(ctx, i, u.ready)
		}
	}
	return s, nil
}

// p1
func (s *Store) Close() {
	for _, u := range s.units {
		u.pool.Close()
	}
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
	u.online = false
	u.ready = false
	u.generation++
	u.checkpoint = ""
	s.state.Unlock()
}

// p1 source identities determine placement independently of geography
func (s *Store) Upsert(ctx context.Context, event *model.HazardEvent) error {
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
			s.markDown(u)
			continue
		}
		if rev > revision {
			revision = rev
		}
	}
	event.IngestedAt = time.Now().UTC()
	r := record{event: *event, partition: partition, revision: revision + 1}
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
			s.markDown(u)
			continue
		}
		successes++
	}
	if successes == 0 {
		return fmt.Errorf("no storage destination accepted event")
	}
	if err := s.enqueueOutbox(ctx, outboxEnvelope(ctx, "hazard.event.upserted", *event)); err != nil {
		return fmt.Errorf("enqueue HazardEvent outbox: %w", err)
	}
	return nil
}

// p1 revision guard makes copying old rows safe alongside new writes
func (s *Store) writeRecord(ctx context.Context, u *unit, r record) error {
	attrs, err := json.Marshal(r.event.Attributes)
	if err != nil {
		return err
	}

	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	e := r.event
	_, err = u.pool.Exec(op, `INSERT INTO hazard_events
 (hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,partition_id,revision)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)
 ON CONFLICT (source,source_ref_id) DO UPDATE SET
 hazard_id=EXCLUDED.hazard_id,hazard_type=EXCLUDED.hazard_type,severity=EXCLUDED.severity,
 area_name=EXCLUDED.area_name,latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude,
 occurred_at=EXCLUDED.occurred_at,ingested_at=EXCLUDED.ingested_at,attributes=EXCLUDED.attributes,
 partition_id=EXCLUDED.partition_id,revision=EXCLUDED.revision
 WHERE hazard_events.revision < EXCLUDED.revision`, e.HazardID, e.Source, e.SourceRefID, e.HazardType, e.Severity, e.AreaName, e.Latitude, e.Longitude, e.OccuredAt, e.IngestedAt, attrs, r.partition, r.revision)
	return err
}

// new P1 volumes only existing M1 data requires explicit migration.
func (s *Store) ensureSchema(ctx context.Context, u *unit) error {
	_, err := u.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS hazard_events (
 hazard_id TEXT NOT NULL UNIQUE,source TEXT NOT NULL,source_ref_id TEXT NOT NULL,
 hazard_type TEXT NOT NULL,severity TEXT NOT NULL,area_name TEXT NOT NULL,
 latitude DOUBLE PRECISION NOT NULL,longitude DOUBLE PRECISION NOT NULL,
 occurred_at TIMESTAMPTZ NOT NULL,ingested_at TIMESTAMPTZ NOT NULL,attributes JSONB NOT NULL DEFAULT '{}',
 partition_id INTEGER NOT NULL,revision BIGINT NOT NULL,PRIMARY KEY(source,source_ref_id));
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

	_, err = u.pool.Exec(ctx, `ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS partition_id INTEGER;
 ALTER TABLE hazard_events ADD COLUMN IF NOT EXISTS revision BIGINT;
 UPDATE hazard_events SET partition_id=0 WHERE partition_id IS NULL;
 UPDATE hazard_events SET revision=1 WHERE revision IS NULL;`)
	if err != nil {
		return err
	}
	rows, err := u.pool.Query(ctx, `
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
	for rows.Next() {
		var identity existingIdentity
		if err := rows.Scan(&identity.source, &identity.sourceRefID); err != nil {
			rows.Close()
			return err
		}
		identities = append(identities, identity)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
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
		results := u.pool.SendBatch(ctx, batch)
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
	_, err = u.pool.Exec(ctx, "UPDATE p1_metadata SET unit_name=$1 WHERE singleton=true", u.name)
	return err
}
