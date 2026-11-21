package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/jackc/pgx/v5"
)

const MaxIngressBytes = 1 << 20

var ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")

type InboxMessage struct {
	ID            string    `json:"message_id"`
	Source        string    `json:"source"`
	Ref           string    `json:"source_ref_id"`
	Kind          string    `json:"kind"`
	Payload       []byte    `json:"-"`
	ReceivedAt    time.Time `json:"received_at"`
	Reason        string    `json:"reason"`
	Status        string    `json:"status"`
	Attempts      int       `json:"attempts"`
	CorrelationID string    `json:"correlation_id"`
}
type InboxLease struct {
	Holder        string
	Token         int64
	CorrelationID string
}

func (c *Coordinator) initIngress(ctx context.Context) error {
	var exists bool
	if err := c.pool.QueryRow(ctx, `SELECT to_regclass('m2_ingress_schema') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(40310304)`); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS durable_inbox (
 ordinal BIGSERIAL UNIQUE, message_id TEXT PRIMARY KEY,source TEXT NOT NULL,source_ref_id TEXT NOT NULL,
 kind TEXT NOT NULL,fingerprint TEXT NOT NULL,correlation_id TEXT NOT NULL,received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 status TEXT NOT NULL DEFAULT 'retrying',reason TEXT NOT NULL DEFAULT '',attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),completed_at TIMESTAMPTZ);
 CREATE INDEX IF NOT EXISTS durable_inbox_pending ON durable_inbox(source,ordinal) WHERE status<>'done';
 CREATE TABLE IF NOT EXISTS inbox_chunks(message_id TEXT NOT NULL REFERENCES durable_inbox(message_id),
 chunk INTEGER NOT NULL,payload BYTEA NOT NULL,PRIMARY KEY(message_id,chunk));
 CREATE TABLE IF NOT EXISTS field_report_results(report_id TEXT PRIMARY KEY,client_id TEXT NOT NULL,
 payload JSONB NOT NULL,processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
 CREATE TABLE IF NOT EXISTS satellite_pipeline_samples(version BIGINT PRIMARY KEY,latency_ms DOUBLE PRECISION NOT NULL,
 completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
 CREATE TABLE IF NOT EXISTS satellite_session_state(singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),latest_offset BIGINT NOT NULL,observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
 CREATE TABLE IF NOT EXISTS m2_ingress_schema(version INTEGER NOT NULL);`)
	if err != nil {
		return err
	}
	// All stream state belongs to Aggregator's coordinator. Never select a
	// different canonical shard as the stream checkpoint when a node disappears.
	// Use the SAME transaction/connection for DDL to avoid self-deadlock.
	if err := ensureSatelliteTx(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO m2_ingress_schema VALUES(1)`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// StageInbox streams the complete original bytes in small chunks. Oversized
// messages are retained intact without allocating the entire body in RAM.
// The hash and spool are prepared by the caller; duplicates do not reprocess.
func (c *Coordinator) StageInbox(ctx context.Context, id, source, ref, kind, fingerprint string, body io.Reader, ownership ...InboxLease) (string, error) {
	op, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(op)
	if len(ownership) > 0 && ownership[0].Token > 0 {
		lease := ownership[0]
		result, err := tx.Exec(op, `UPDATE source_leases SET updated_at=clock_timestamp() WHERE source=$1 AND holder_id=$2 AND fencing_token=$3 AND lease_until>clock_timestamp()`, source, lease.Holder, lease.Token)
		if err != nil {
			return "", err
		}
		if result.RowsAffected() == 0 {
			return "", ErrLeaseLost
		}
	}
	correlationID := id
	if len(ownership) > 0 && ownership[0].CorrelationID != "" {
		correlationID = ownership[0].CorrelationID
	}
	result, err := tx.Exec(op, `INSERT INTO durable_inbox(message_id,source,source_ref_id,kind,fingerprint,correlation_id)
 VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, id, source, ref, kind, fingerprint, correlationID)
	if err != nil {
		return "", err
	}
	if result.RowsAffected() == 0 {
		var old, status string
		if err = tx.QueryRow(op, `SELECT fingerprint,status FROM durable_inbox WHERE message_id=$1`, id).Scan(&old, &status); err != nil {
			return "", err
		}
		if old != fingerprint {
			return "", ErrIdempotencyConflict
		}
		return status, nil
	}
	buf := make([]byte, 64<<10)
	for chunk := 0; ; chunk++ {
		n, readErr := io.ReadFull(body, buf)
		if n > 0 {
			if _, err = tx.Exec(op, `INSERT INTO inbox_chunks VALUES($1,$2,$3)`, id, chunk, buf[:n]); err != nil {
				return "", err
			}
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	return "retrying", tx.Commit(op)
}

func InboxID(source string, payload []byte) string {
	sum := sha256.Sum256(append([]byte(source+"\x00"), payload...))
	return "msg-" + hex.EncodeToString(sum[:])
}

func inboxPayload(ctx context.Context, tx pgx.Tx, id string) ([]byte, error) {
	rows, err := tx.Query(ctx, `SELECT payload FROM inbox_chunks WHERE message_id=$1 ORDER BY chunk`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var payload []byte
	for rows.Next() {
		var part []byte
		if err = rows.Scan(&part); err != nil {
			return nil, err
		}
		if len(payload)+len(part) > MaxIngressBytes {
			return nil, fmt.Errorf("payload_too_large: exceeds %d bytes", MaxIngressBytes)
		}
		payload = append(payload, part...)
	}
	return payload, rows.Err()
}

// Each identity is processed in arrival order. A parked older revision blocks
// that identity only, not the rest of its source. Admission and completion use
// one transaction; a lost acknowledgement may replay, never lose a 202 report.
func (c *Coordinator) ProcessInbox(ctx context.Context, source string, convert func(InboxMessage) (*model.HazardEvent, error)) (worked bool, err error) {
	op, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(op)
	var m InboxMessage
	defer func() {
		if err == nil || m.ID == "" || ctx.Err() != nil {
			return
		}
		// A transient downstream failure rolls back both completion and effects.
		// Record its reason separately, without turning a retryable message into
		// a permanent schema rejection or clearing another worker's completion.
		_ = tx.Rollback(op)
		retry, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_, updateErr := c.pool.Exec(retry, `UPDATE durable_inbox SET reason=$2,attempts=attempts+1,
 next_attempt_at=clock_timestamp()+make_interval(secs=>LEAST(30,power(2,LEAST(attempts,5)))::integer)
 WHERE message_id=$1 AND status='retrying'`, m.ID, "downstream_retry: "+err.Error())
		if updateErr != nil {
			err = errors.Join(err, fmt.Errorf("record inbox retry: %w", updateErr))
		}
	}()
	err = tx.QueryRow(op, `SELECT message_id,source,source_ref_id,kind,received_at,attempts,correlation_id FROM durable_inbox m
 WHERE source=$1 AND status='retrying' AND next_attempt_at<=clock_timestamp()
 AND NOT EXISTS(SELECT 1 FROM durable_inbox older WHERE older.source=m.source AND older.source_ref_id=m.source_ref_id
 AND older.ordinal<m.ordinal AND older.status<>'done') ORDER BY ordinal LIMIT 1 FOR UPDATE SKIP LOCKED`, source).Scan(&m.ID, &m.Source, &m.Ref, &m.Kind, &m.ReceivedAt, &m.Attempts, &m.CorrelationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	m.Payload, err = inboxPayload(op, tx, m.ID)
	var event *model.HazardEvent
	if err == nil {
		event, err = convert(m)
	}
	if err != nil {
		_, dbErr := tx.Exec(op, `UPDATE durable_inbox SET status='parked',reason=$2,attempts=attempts+1 WHERE message_id=$1`, m.ID, err.Error())
		if dbErr != nil {
			return false, dbErr
		}
		return true, tx.Commit(op)
	}
	if event != nil {
		// Same identity lock serializes concurrent replayers even across pods.
		if _, err = tx.Exec(op, `SELECT pg_advisory_xact_lock(hashtextextended($1,4031))`, m.Source+":"+m.Ref); err != nil {
			return false, err
		}
		var now time.Time
		if err = tx.QueryRow(op, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return false, err
		}
		if err = c.admitTx(op, tx, m.Source, "durable-inbox", 0, event, m.CorrelationID, now); err != nil {
			return false, err
		}
	} else if m.Source == "FIELD_REPORT" {
		var report struct {
			ClientID string          `json:"client_id"`
			Data     json.RawMessage `json:"data"`
		}
		if err = json.Unmarshal(m.Payload, &report); err != nil {
			return false, err
		}
		if _, err = tx.Exec(op, `INSERT INTO field_report_results(report_id,client_id,payload) VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING`, m.ID, report.ClientID, report.Data); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(op, `UPDATE durable_inbox SET status='done',reason='',attempts=attempts+1,completed_at=clock_timestamp() WHERE message_id=$1`, m.ID); err != nil {
		return false, err
	}
	return true, tx.Commit(op)
}

func (c *Coordinator) ReplayInbox(ctx context.Context, source string) (int64, error) {
	result, err := c.pool.Exec(ctx, `UPDATE durable_inbox SET status='retrying',next_attempt_at=clock_timestamp() WHERE source=$1 AND status='parked'`, source)
	return result.RowsAffected(), err
}

func (c *Coordinator) InboxDetails(ctx context.Context, id string, chunk int) (map[string]any, error) {
	var source, status, reason string
	var received time.Time
	var attempts int
	var size int64
	err := c.pool.QueryRow(ctx, `SELECT source,status,reason,received_at,attempts,(SELECT COALESCE(sum(octet_length(payload)),0) FROM inbox_chunks WHERE message_id=m.message_id) FROM durable_inbox m WHERE message_id=$1`, id).Scan(&source, &status, &reason, &received, &attempts, &size)
	if err != nil {
		return nil, err
	}
	var payload []byte
	err = c.pool.QueryRow(ctx, `SELECT payload FROM inbox_chunks WHERE message_id=$1 AND chunk=$2`, id, chunk).Scan(&payload)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return map[string]any{"message_id": id, "source": source, "status": status, "reason": reason, "received_at": received, "attempts": attempts, "size_bytes": size, "chunk": chunk, "payload_base64": base64.StdEncoding.EncodeToString(payload), "has_more": int64((chunk+1)*(64<<10)) < size}, nil
}

func (c *Coordinator) FieldReport(ctx context.Context, id, client string, ops bool) (map[string]any, error) {
	var status, owner string
	var payload []byte
	err := c.pool.QueryRow(ctx, `SELECT m.status,COALESCE(r.client_id,m.source_ref_id),r.payload FROM durable_inbox m LEFT JOIN field_report_results r ON r.report_id=m.message_id WHERE m.message_id=$1 AND m.source='FIELD_REPORT'`, id).Scan(&status, &owner, &payload)
	if err != nil {
		return nil, err
	}
	// Ownership is also available before the asynchronous worker has run.
	if owner == "" {
		var raw []byte
		if err = c.pool.QueryRow(ctx, `SELECT payload FROM inbox_chunks WHERE message_id=$1 AND chunk=0`, id).Scan(&raw); err != nil {
			return nil, err
		}
		var v struct {
			ClientID string `json:"client_id"`
		}
		_ = json.Unmarshal(raw, &v)
		owner = v.ClientID
	}
	if !ops && owner != client {
		return nil, pgx.ErrNoRows
	}
	result := map[string]any{"report_id": id, "status": status}
	if len(payload) > 0 {
		result["report"] = json.RawMessage(payload)
	}
	return result, nil
}
