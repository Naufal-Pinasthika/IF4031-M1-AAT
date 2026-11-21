package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

// A graceful owner may give up its epoch early. A stale release is a no-op;
// keeping the row ensures every later acquisition has a higher fencing token.
func (c *Coordinator) ReleaseLease(ctx context.Context, source, holder string, token int64) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.pool.Exec(op, `UPDATE source_leases SET lease_until=clock_timestamp(),updated_at=clock_timestamp()
 WHERE source=$1 AND holder_id=$2 AND fencing_token=$3`, source, holder, token)
	return err
}

// RenewLease cannot resurrect an expired lease or renew a replacement epoch.
func (c *Coordinator) RenewLease(ctx context.Context, source, holder string, token int64, ttl time.Duration) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	result, err := c.pool.Exec(op, `UPDATE source_leases
 SET lease_until=clock_timestamp()+($4*interval '1 millisecond'),updated_at=clock_timestamp()
 WHERE source=$1 AND holder_id=$2 AND fencing_token=$3 AND lease_until>clock_timestamp()`, source, holder, token, ttl.Milliseconds())
	if err != nil {
		return fmt.Errorf("renew %s lease: %w", source, err)
	}
	if result.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// AcceptEvent is the ingestion linearization point. Lease ownership and the
// durable write intent are checked/committed in the SAME database transaction.
// Source pollers never write to P1 or Kafka directly. A stale client cannot
// bypass this gate, even before any shard has seen the replacement fencing token.
func (c *Coordinator) AcceptEvent(ctx context.Context, source, holder string, token int64, event *model.HazardEvent, correlationID string) error {
	if event == nil || string(event.Source) != source || event.SourceRefID == "" || token <= 0 {
		return fmt.Errorf("invalid fenced event identity")
	}
	copyEvent := *event
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return err
	}
	defer tx.Rollback(op)
	// UPDATE locks the lease row against takeover. If a suspended client's
	// transaction is killed by the server, all later commands on it fail.
	var acceptedAt time.Time
	err = tx.QueryRow(op, `UPDATE source_leases SET updated_at=clock_timestamp()
 WHERE source=$1 AND holder_id=$2 AND fencing_token=$3 AND lease_until>clock_timestamp()
 RETURNING clock_timestamp()`, source, holder, token).Scan(&acceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if err := c.admitTx(op, tx, source, holder, token, &copyEvent, correlationID, acceptedAt); err != nil {
		return err
	}
	if err := tx.Commit(op); err != nil {
		return err
	}
	*event = copyEvent
	return nil
}

// Callers hold the source lease (or their inbox row) in this transaction.
// State, checkpoint, deduplication and destination intents commit together.
func (c *Coordinator) admitTx(op context.Context, tx pgx.Tx, source, holder string, token int64, event *model.HazardEvent, correlationID string, acceptedAt time.Time) error {
	copyEvent := *event
	copyEvent.HazardID = eventID(source, event.SourceRefID)
	// Canonical timestamptz is microsecond-precision. Normalize before hashing
	// so a parent read back from storage cannot invent a changed warning.
	copyEvent.OccuredAt = copyEvent.OccuredAt.UTC().Truncate(time.Microsecond)
	copyEvent.IngestedAt = time.Time{}
	payload, err := json.Marshal(copyEvent)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	fingerprint := hex.EncodeToString(digest[:])
	copyEvent.IngestedAt = acceptedAt.UTC()
	payload, err = json.Marshal(copyEvent)
	if err != nil {
		return err
	}
	var inserted int
	// Identity serialization is held by the caller. Deduplication, the parent
	// intent and every destination remain atomic, with one database round trip
	// instead of four on every stream mutation.
	err = tx.QueryRow(op, `WITH admitted AS (
 INSERT INTO ingestion_outbox(source,holder_id,fencing_token,hazard_id,payload,correlation_id,accepted_at)
 SELECT $1,$2,$3,$4,$5::jsonb,$6,$7
 WHERE NOT EXISTS(SELECT 1 FROM ingestion_heads WHERE hazard_id=$4 AND fingerprint=$8)
 RETURNING version
 ), heads AS (
 INSERT INTO ingestion_heads(hazard_id,fingerprint,version) SELECT $4,$8,version FROM admitted
 ON CONFLICT(hazard_id) DO UPDATE SET fingerprint=EXCLUDED.fingerprint,version=EXCLUDED.version
 ), projections AS (
 INSERT INTO ingestion_projections(version,target,source,hazard_id)
 SELECT version,unnest($9::text[]),$1,$4 FROM admitted
 ) SELECT count(*) FROM admitted`, source, holder, token, copyEvent.HazardID, payload, correlationID,
		acceptedAt, fingerprint, c.projectionTargets(copyEvent)).Scan(&inserted)
	if err != nil {
		return err
	}
	if inserted > 0 {
		*event = copyEvent
	} else {
		event.HazardID = copyEvent.HazardID
	}
	return nil
}

// AcceptBatch admits a bounded source page with one durable commit, rather than
// one fsync/round trip per observation. The lease row is locked until commit;
// every intent, fingerprint and admission version is part of that transaction.
// Retrying a page after a crash does not enqueue unchanged events again.
func (c *Coordinator) AcceptBatch(ctx context.Context, source, holder string, token int64, events []model.HazardEvent, correlationID string) error {
	if len(events) == 0 {
		return nil
	}
	if len(events) > 1000 || token <= 0 {
		return fmt.Errorf("invalid fenced batch bounds")
	}
	type candidate struct {
		HazardID    string          `json:"hazard_id"`
		Fingerprint string          `json:"fingerprint"`
		Payload     json.RawMessage `json:"payload"`
		Targets     []string        `json:"targets"`
	}
	items := make([]candidate, 0, len(events))
	seen := make(map[string]bool, len(events))
	for _, event := range events {
		if string(event.Source) != source || event.SourceRefID == "" {
			return fmt.Errorf("invalid fenced batch identity")
		}
		event.HazardID = eventID(source, event.SourceRefID)
		event.OccuredAt = event.OccuredAt.UTC().Truncate(time.Microsecond)
		if seen[event.HazardID] {
			return fmt.Errorf("duplicate identity inside fenced batch")
		}
		seen[event.HazardID] = true
		event.IngestedAt = time.Time{}
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(payload)
		items = append(items, candidate{event.HazardID, hex.EncodeToString(digest[:]), payload, c.projectionTargets(event)})
	}
	payload, err := json.Marshal(items)
	if err != nil {
		return err
	}
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return err
	}
	defer tx.Rollback(op)
	var acceptedAt time.Time
	err = tx.QueryRow(op, `UPDATE source_leases SET updated_at=clock_timestamp()
 WHERE source=$1 AND holder_id=$2 AND fencing_token=$3 AND lease_until>clock_timestamp()
 RETURNING clock_timestamp()`, source, holder, token).Scan(&acceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(op, `WITH candidates AS MATERIALIZED (
 SELECT i.* FROM jsonb_to_recordset($1::jsonb) AS i(hazard_id text,fingerprint text,payload jsonb,targets text[])
 WHERE NOT EXISTS(SELECT 1 FROM ingestion_heads h WHERE h.hazard_id=i.hazard_id AND h.fingerprint=i.fingerprint)
 ), inserted AS (
 INSERT INTO ingestion_outbox(source,holder_id,fencing_token,hazard_id,payload,correlation_id,accepted_at)
 SELECT $2,$3,$4,i.hazard_id,jsonb_set(i.payload,'{ingested_at}',to_jsonb($6::timestamptz)),$5,$6
 FROM candidates i RETURNING version,hazard_id
 ), projections AS (
 INSERT INTO ingestion_projections(version,target,source,hazard_id)
 SELECT o.version,unnest(i.targets),$2,o.hazard_id FROM inserted o JOIN candidates i USING(hazard_id)
 ) INSERT INTO ingestion_heads(hazard_id,fingerprint,version)
 SELECT o.hazard_id,i.fingerprint,o.version FROM inserted o JOIN candidates i USING(hazard_id)
 ON CONFLICT(hazard_id) DO UPDATE SET fingerprint=EXCLUDED.fingerprint,version=EXCLUDED.version`, payload, source, holder, token, correlationID, acceptedAt)
	if err != nil {
		return err
	}
	return tx.Commit(op)
}

// PendingEvent resolves a parent quake admitted earlier in this or a previous
// batch but not yet projected to canonical storage.
func (c *Coordinator) PendingEvent(ctx context.Context, source, ref string) (*model.HazardEvent, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var payload []byte
	err := c.pool.QueryRow(op, `SELECT o.payload FROM ingestion_heads h
 JOIN ingestion_outbox o ON o.version=h.version
 WHERE h.hazard_id=$1 AND o.payload IS NOT NULL`, eventID(source, ref)).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var event model.HazardEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

type AcceptedEvent struct {
	Version       int64
	Event         model.HazardEvent
	CorrelationID string
	Target        string
}

func (c *Coordinator) projectionTargets(event model.HazardEvent) []string {
	targets := []string{"broker"}
	if critical, _ := event.Attributes["critical"].(bool); c.criticalLane && string(event.Source) == "SENSOR_PANTAI" && critical {
		targets = append(targets, "broker:critical")
	}
	partition := partitionFor(string(event.Source), event.SourceRefID)
	if len(c.storage) == 1 {
		return append(targets, "canonical:"+c.storage[0].Name)
	}
	return append(targets, "canonical:"+c.storage[partition].Name, "canonical:"+c.storage[(partition+1)%3].Name)
}

// Each target has its own durable acknowledgement, backoff and per-identity
// ordering. Kafka never waits behind a failed canonical replica. A successful
// remote commit followed by a lost acknowledgement replays the SAME version.
// Empty source permits one bounded worker per target across all sources.
func (c *Coordinator) DeliverNext(ctx context.Context, source, target string, apply func(context.Context, AcceptedEvent) error) (bool, error) {
	op, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(op)
	// A frozen dispatcher must not prevent retry forever. Its remote side
	// effects remain replays of a previously authorized intent, not new inputs.
	if _, err := tx.Exec(op, `SET LOCAL idle_in_transaction_session_timeout='10s'`); err != nil {
		return false, err
	}
	item := AcceptedEvent{Target: target}
	var payload []byte
	err = tx.QueryRow(op, `SELECT o.version,o.payload,o.correlation_id FROM ingestion_projections p
 JOIN ingestion_outbox o ON o.version=p.version
 WHERE ($1='' OR p.source=$1) AND p.target=$2 AND p.delivered_at IS NULL AND p.next_attempt_at<=clock_timestamp()
 AND NOT EXISTS(SELECT 1 FROM ingestion_projections earlier WHERE earlier.hazard_id=p.hazard_id
 AND earlier.target=p.target AND earlier.delivered_at IS NULL AND earlier.version<p.version)
 ORDER BY CASE WHEN o.source='SENSOR_PANTAI' AND o.payload->'attributes'->>'critical'='true' THEN 0 ELSE 1 END,
 o.version LIMIT 1 FOR UPDATE OF p SKIP LOCKED`, source, target).Scan(&item.Version, &payload, &item.CorrelationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(payload, &item.Event); err != nil {
		return false, err
	}
	applyErr := apply(op, item)
	if applyErr != nil {
		_, err = tx.Exec(op, `UPDATE ingestion_projections SET attempts=attempts+1,last_error=$3,
 next_attempt_at=clock_timestamp()+(LEAST(5000,100*power(2,LEAST(attempts,6)))*interval '1 millisecond') WHERE version=$1 AND target=$2`, item.Version, target, applyErr.Error())
	} else {
		_, err = tx.Exec(op, `UPDATE ingestion_projections SET delivered_at=clock_timestamp(),last_error=NULL WHERE version=$1 AND target=$2`, item.Version, target)
	}
	if err != nil {
		return false, err
	}
	// Serialize only final acknowledgement/cleanup, never remote I/O. The
	// following statement sees every previously committed target ack after
	// obtaining the parent lock (READ COMMITTED); concurrent final acks cannot
	// leave a completed intent permanently retaining its payload.
	if _, err := tx.Exec(op, `SELECT version FROM ingestion_outbox WHERE version=$1 FOR UPDATE`, item.Version); err != nil {
		return false, err
	}
	if _, err := tx.Exec(op, `INSERT INTO satellite_pipeline_samples(version,latency_ms)
 SELECT o.version,EXTRACT(EPOCH FROM(clock_timestamp()-o.accepted_at))*1000 FROM ingestion_outbox o
 WHERE o.version=$1 AND o.source='SATELIT' AND NOT EXISTS(SELECT 1 FROM ingestion_projections p WHERE p.version=o.version AND p.target LIKE 'canonical%' AND p.delivered_at IS NULL)
 ON CONFLICT DO NOTHING`, item.Version); err != nil {
		return false, err
	}
	if _, err := tx.Exec(op, `UPDATE ingestion_outbox o SET delivered_at=clock_timestamp(),payload=NULL,last_error=NULL
 WHERE version=$1 AND NOT EXISTS(SELECT 1 FROM ingestion_projections p WHERE p.version=o.version AND p.delivered_at IS NULL)`, item.Version); err != nil {
		return false, err
	}
	if err := tx.Commit(op); err != nil {
		return false, err
	}
	return true, applyErr
}
