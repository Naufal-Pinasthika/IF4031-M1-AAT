package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	storageconfig "github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrLeaseLost = errors.New("source lease lost")
var ErrSourceClockStale = errors.New("source_clock_stalled")
var ErrSourceClockRegression = errors.New("source_clock_regressed")

type Coordinator struct {
	pool         *pgxpool.Pool
	timeout      time.Duration
	storage      []storageconfig.StorageUnit
	criticalLane bool
}

type Lease struct {
	Token      int64
	LeaseUntil time.Time
}

type UnprocessedMessage struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"`
}

type UnprocessedPage struct {
	Items      []UnprocessedMessage `json:"items"`
	NextCursor *string              `json:"next_cursor"`
}

type unprocessedCursor struct {
	ReceivedAt time.Time `json:"received_at"`
	MessageID  string    `json:"message_id"`
}

func NewCoordinator(ctx context.Context, databaseURL string, timeout time.Duration, logger *slog.Logger, storage ...storageconfig.StorageUnit) (*Coordinator, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("coordination database URL is required")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse coordination database configuration: %w", err)
	}
	config.ConnConfig.ConnectTimeout = timeout
	config.MaxConns = 12 // Leave room for polling heartbeats alongside delivery workers.
	// Enforce limits on the server as well: a suspended client cannot keep
	// an abandoned transaction (and its lease row lock) alive indefinitely.
	config.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprint(timeout.Milliseconds())
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = fmt.Sprint(timeout.Milliseconds())
	config.ConnConfig.Tracer = &postgresTracer{logger: logger.With("storage_unit", "coordinator")}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create coordination database pool: %w", err)
	}
	if len(storage) == 0 {
		storage = []storageconfig.StorageUnit{{Name: "store-a"}, {Name: "store-b"}, {Name: "store-c"}}
	}
	if len(storage) != 1 && len(storage) != 3 {
		pool.Close()
		return nil, fmt.Errorf("coordination requires one or three canonical targets")
	}
	coordinator := &Coordinator{pool: pool, timeout: timeout, storage: storage}
	if err := retryStartup(ctx, logger, func() error { return coordinator.migrate(ctx) }); err != nil {
		pool.Close()
		return nil, err
	}
	if err := retryStartup(ctx, logger, func() error { return coordinator.initIngress(ctx) }); err != nil {
		pool.Close()
		return nil, err
	}
	if err := retryStartup(ctx, logger, func() error { return coordinator.initSourceRetry(ctx) }); err != nil {
		pool.Close()
		return nil, err
	}
	return coordinator, nil
}

func (c *Coordinator) Close() { c.pool.Close() }

func (c *Coordinator) Ping(ctx context.Context) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.pool.Ping(op)
}

func (c *Coordinator) Now(ctx context.Context) (time.Time, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var now time.Time
	if err := c.pool.QueryRow(op, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read coordination clock: %w", err)
	}
	return now.UTC(), nil
}

func (c *Coordinator) migrate(ctx context.Context) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	// Normal pod startup must not serialize behind migration locks or repeat
	// DDL on hot ingestion tables. The marker is committed atomically only
	// after all migration statements succeed. Legacy databases have no marker.
	current, err := coordinationSchemaCurrent(op, c.pool)
	if err != nil || current {
		return err
	}
	tx, err := c.pool.Begin(op)
	if err != nil {
		return fmt.Errorf("begin coordination migration: %w", err)
	}
	defer tx.Rollback(op)
	if _, err := tx.Exec(op, `SELECT pg_advisory_xact_lock(40310005)`); err != nil {
		return fmt.Errorf("lock coordination migration: %w", err)
	}
	// Another starting pod may have migrated while this one waited for the lock.
	// Missing-table errors abort a PostgreSQL transaction. Check the catalog
	// without raising one before reading the marker inside this transaction.
	var schemaExists bool
	if err := tx.QueryRow(op, `SELECT to_regclass('coordination_schema') IS NOT NULL`).Scan(&schemaExists); err != nil {
		return err
	}
	if schemaExists {
		if current, err := coordinationSchemaCurrent(op, tx); err != nil {
			return err
		} else if current {
			return tx.Commit(op)
		}
	}
	statements := []string{`
		CREATE TABLE IF NOT EXISTS source_leases (
			source TEXT PRIMARY KEY,
			holder_id TEXT NOT NULL,
			fencing_token BIGINT NOT NULL,
			lease_until TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)`, `
		CREATE TABLE IF NOT EXISTS source_cursors (
			source TEXT PRIMARY KEY,
			occurred_at TIMESTAMPTZ,
			opaque_cursor TEXT,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)`, `
		CREATE TABLE IF NOT EXISTS unprocessed_messages (
			message_id TEXT PRIMARY KEY,
			source TEXT NOT NULL,
			received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			reason TEXT NOT NULL,
			raw_payload BYTEA NOT NULL,
			retryable BOOLEAN NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL CHECK (status IN ('retrying', 'parked'))
		)`,
		`CREATE INDEX IF NOT EXISTS unprocessed_source_order_idx ON unprocessed_messages(source, received_at, message_id)`,
		`CREATE TABLE IF NOT EXISTS ingestion_heads (
			hazard_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, version BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS ingestion_outbox (
			version BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, holder_id TEXT NOT NULL,
			fencing_token BIGINT NOT NULL, hazard_id TEXT NOT NULL,
			payload JSONB, correlation_id TEXT NOT NULL,
			accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			delivered_at TIMESTAMPTZ, attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), last_error TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS ingestion_pending_idx ON ingestion_outbox(source,version) WHERE delivered_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS ingestion_pending_identity_idx ON ingestion_outbox(hazard_id,version) WHERE delivered_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS ingestion_projections (
 version BIGINT NOT NULL REFERENCES ingestion_outbox(version), target TEXT NOT NULL,
 source TEXT NOT NULL, hazard_id TEXT NOT NULL,
 delivered_at TIMESTAMPTZ, attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), last_error TEXT,
 PRIMARY KEY(version,target))`,
		`CREATE INDEX IF NOT EXISTS projection_pending_idx ON ingestion_projections(target,version) WHERE delivered_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS projection_identity_idx ON ingestion_projections(target,hazard_id,version) WHERE delivered_at IS NULL`,
		// Legacy pending intents have no per-destination acknowledgements. Replay
		// them conservatively to Kafka and ALL canonical destinations. Already
		// completed legacy rows have no payload; anti-entropy repairs their peers.
		`INSERT INTO ingestion_projections(version,target,source,hazard_id)
 SELECT o.version,t.target,o.source,o.hazard_id FROM ingestion_outbox o
 CROSS JOIN (VALUES('broker'),('canonical')) t(target)
 WHERE o.delivered_at IS NULL AND NOT EXISTS(
 SELECT 1 FROM ingestion_projections p WHERE p.version=o.version)
 ON CONFLICT DO NOTHING`,
		`CREATE TABLE IF NOT EXISTS source_clock_progress (
		 source TEXT PRIMARY KEY, generated_at TIMESTAMPTZ NOT NULL,
		 advanced_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)`,
		`CREATE TABLE IF NOT EXISTS coordination_schema (
		 singleton BOOLEAN PRIMARY KEY CHECK(singleton), version INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS poll_attempts (
		 poll_id BIGSERIAL PRIMARY KEY, source TEXT NOT NULL, holder_id TEXT NOT NULL,
		 fencing_token BIGINT NOT NULL, previous_poll_id BIGINT,
		 status TEXT NOT NULL CHECK(status IN ('in_flight','completed','uncertain')),
		 started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), finished_at TIMESTAMPTZ,
		 last_error TEXT NOT NULL DEFAULT '', checkpoint JSONB NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE INDEX IF NOT EXISTS poll_attempts_source_idx ON poll_attempts(source,poll_id DESC)`,
		`CREATE TABLE IF NOT EXISTS source_poll_heads (
		 source TEXT PRIMARY KEY, poll_id BIGINT NOT NULL REFERENCES poll_attempts(poll_id),
		 next_poll_at TIMESTAMPTZ NOT NULL, attempt_count BIGINT NOT NULL DEFAULT 1
		)`,
		`INSERT INTO coordination_schema(singleton,version) VALUES(true,3)
 ON CONFLICT(singleton) DO UPDATE SET version=EXCLUDED.version`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(op, statement); err != nil {
			return fmt.Errorf("migrate coordination database: %w", err)
		}
	}
	if err := tx.Commit(op); err != nil {
		return fmt.Errorf("commit coordination migration: %w", err)
	}
	return nil
}

func coordinationSchemaCurrent(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (bool, error) {
	var version int
	err := db.QueryRow(ctx, `SELECT version FROM coordination_schema WHERE singleton=true`).Scan(&version)
	var databaseError *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &databaseError) && databaseError.Code == "42P01" {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read coordination schema version: %w", err)
	}
	if version < 1 || version > 3 {
		return false, fmt.Errorf("unsupported coordination schema version %d (expected 1 through 3)", version)
	}
	return version == 3, nil
}

// Sensor timestamps are an independent clock (the official image can run at
// accelerated speed). Persist its high-water mark and measure time without
// progress with the database clock, not a pod clock or a hard-coded epoch.
// A fixed initial offset is unknowable from this contract alone; bootstrap is
// explicitly unverified until progress is seen. No seed/profile heuristics.
func (c *Coordinator) CheckSourceProgress(ctx context.Context, source string, generatedAt time.Time, maxIdle time.Duration) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var latest, advanced, now time.Time
	err := c.pool.QueryRow(op, `INSERT INTO source_clock_progress(source,generated_at)
 VALUES($1,$2) ON CONFLICT(source) DO UPDATE SET
 advanced_at=CASE WHEN EXCLUDED.generated_at>source_clock_progress.generated_at THEN clock_timestamp() ELSE source_clock_progress.advanced_at END,
 generated_at=GREATEST(source_clock_progress.generated_at,EXCLUDED.generated_at)
 RETURNING generated_at,advanced_at,clock_timestamp()`, source, generatedAt.UTC()).Scan(&latest, &advanced, &now)
	if err != nil {
		return err
	}
	if generatedAt.Before(latest) {
		return ErrSourceClockRegression
	}
	if now.Sub(advanced) > maxIdle {
		return ErrSourceClockStale
	}
	return nil
}

// AcquireOrRenewLease uses the database clock as the sole time authority.
// A token changes on ownership change or reacquisition after expiration.
func (c *Coordinator) AcquireOrRenewLease(ctx context.Context, source, holder string, ttl time.Duration) (Lease, bool, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var lease Lease
	err := c.pool.QueryRow(op, `
		INSERT INTO source_leases (source, holder_id, fencing_token, lease_until, updated_at)
		VALUES ($1, $2, 1, clock_timestamp() + ($3 * interval '1 millisecond'), clock_timestamp())
		ON CONFLICT (source) DO UPDATE SET
			holder_id = EXCLUDED.holder_id,
			fencing_token = CASE
				WHEN source_leases.holder_id = EXCLUDED.holder_id
				 AND source_leases.lease_until > clock_timestamp() THEN source_leases.fencing_token
				ELSE source_leases.fencing_token + 1
			END,
			lease_until = clock_timestamp() + ($3 * interval '1 millisecond'),
			updated_at = clock_timestamp()
		WHERE source_leases.holder_id = EXCLUDED.holder_id
		   OR source_leases.lease_until <= clock_timestamp()
		RETURNING fencing_token, lease_until
	`, source, holder, ttl.Milliseconds()).Scan(&lease.Token, &lease.LeaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, false, nil
	}
	if err != nil {
		return Lease{}, false, fmt.Errorf("acquire lease for %s: %w", source, err)
	}
	lease.LeaseUntil = lease.LeaseUntil.UTC()
	return lease, true, nil
}

func (c *Coordinator) ValidateLease(ctx context.Context, source, holder string, token int64) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var valid bool
	if err := c.pool.QueryRow(op, `
		SELECT EXISTS (
			SELECT 1 FROM source_leases
			WHERE source=$1 AND holder_id=$2 AND fencing_token=$3
			  AND lease_until > clock_timestamp()
		)
	`, source, holder, token).Scan(&valid); err != nil {
		return fmt.Errorf("validate lease for %s: %w", source, err)
	}
	if !valid {
		return ErrLeaseLost
	}
	return nil
}

func (c *Coordinator) TimeCursor(ctx context.Context, source string, fallback time.Time) (time.Time, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var value *time.Time
	err := c.pool.QueryRow(op, `SELECT occurred_at FROM source_cursors WHERE source=$1`, source).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return fallback, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("load cursor for %s: %w", source, err)
	}
	if value == nil {
		return fallback, nil
	}
	return value.UTC(), nil
}

func (c *Coordinator) OpaqueCursor(ctx context.Context, source string) (string, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var value *string
	err := c.pool.QueryRow(op, `SELECT opaque_cursor FROM source_cursors WHERE source=$1`, source).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load opaque cursor for %s: %w", source, err)
	}
	if value == nil {
		return "", nil
	}
	return *value, nil
}

func (c *Coordinator) AdvanceTimeCursor(ctx context.Context, source, holder string, token int64, value time.Time) error {
	return c.advanceCursor(ctx, source, holder, token, &value, nil)
}

func (c *Coordinator) AdvanceOpaqueCursor(ctx context.Context, source, holder string, token int64, value string) error {
	return c.advanceCursor(ctx, source, holder, token, nil, &value)
}

func (c *Coordinator) advanceCursor(ctx context.Context, source, holder string, token int64, occurredAt *time.Time, opaque *string) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	command, err := c.pool.Exec(op, `
		WITH lease AS (
		 UPDATE source_leases SET updated_at=clock_timestamp()
		 WHERE source=$1 AND holder_id=$4 AND fencing_token=$5
		   AND lease_until > clock_timestamp() RETURNING source
		)
		INSERT INTO source_cursors (source, occurred_at, opaque_cursor, updated_at)
		SELECT $1, $2, $3, clock_timestamp()
		FROM lease
		ON CONFLICT (source) DO UPDATE SET
			occurred_at = GREATEST(EXCLUDED.occurred_at, source_cursors.occurred_at),
			opaque_cursor = COALESCE(EXCLUDED.opaque_cursor, source_cursors.opaque_cursor),
			updated_at = clock_timestamp()
	`, source, occurredAt, opaque, holder, token)
	if err != nil {
		return fmt.Errorf("advance cursor for %s: %w", source, err)
	}
	if command.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (c *Coordinator) Quarantine(ctx context.Context, source, reason string, payload []byte, retryable bool) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	digest := sha256.Sum256(append([]byte(source+"\x00"+reason+"\x00"), payload...))
	messageID := "msg-" + hex.EncodeToString(digest[:12])
	status := "parked"
	if retryable {
		status = "retrying"
	}
	_, err := c.pool.Exec(op, `
		INSERT INTO unprocessed_messages (message_id, source, reason, raw_payload, retryable, status)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (message_id) DO UPDATE SET
			attempts=unprocessed_messages.attempts+1,
			retryable=EXCLUDED.retryable,
			status=EXCLUDED.status
	`, messageID, source, reason, payload, retryable, status)
	if err != nil {
		return fmt.Errorf("quarantine %s message: %w", source, err)
	}
	return nil
}

func (c *Coordinator) ListUnprocessed(ctx context.Context, source string, limit int, rawCursor string) (UnprocessedPage, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	var cursor *unprocessedCursor
	if rawCursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(rawCursor)
		if err != nil || len(decoded) > 2048 {
			return UnprocessedPage{}, ErrInvalidCursor
		}
		var value unprocessedCursor
		if json.Unmarshal(decoded, &value) != nil || value.ReceivedAt.IsZero() || value.MessageID == "" {
			return UnprocessedPage{}, ErrInvalidCursor
		}
		cursor = &value
	}
	query := `SELECT message_id,source,received_at,reason,retryable,attempts,status FROM (
 SELECT message_id,source,received_at,reason,retryable,attempts,status FROM unprocessed_messages
 UNION ALL SELECT message_id,source,received_at,reason,status='retrying',attempts,status FROM durable_inbox WHERE status<>'done'
 ) pending WHERE true`
	args := make([]any, 0, 5)
	if source != "" {
		args = append(args, source)
		query += fmt.Sprintf(" AND source=$%d", len(args))
	}
	if cursor != nil {
		args = append(args, cursor.ReceivedAt, cursor.MessageID)
		query += fmt.Sprintf(" AND (received_at,message_id)>($%d,$%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY received_at,message_id LIMIT $%d", len(args))
	rows, err := c.pool.Query(op, query, args...)
	if err != nil {
		return UnprocessedPage{}, err
	}
	defer rows.Close()
	items := make([]UnprocessedMessage, 0, limit+1)
	for rows.Next() {
		var item UnprocessedMessage
		if err := rows.Scan(&item.MessageID, &item.Source, &item.ReceivedAt, &item.Reason, &item.Retryable, &item.Attempts, &item.Status); err != nil {
			return UnprocessedPage{}, err
		}
		item.ReceivedAt = item.ReceivedAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return UnprocessedPage{}, err
	}
	var next *string
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		encoded, _ := json.Marshal(unprocessedCursor{ReceivedAt: last.ReceivedAt, MessageID: last.MessageID})
		value := base64.RawURLEncoding.EncodeToString(encoded)
		next = &value
	}
	return UnprocessedPage{Items: items, NextCursor: next}, nil
}
