package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
)

type Postgres struct{ pool *pgxpool.Pool }

func OpenPostgres(ctx context.Context, url string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("invalid notifier database configuration")
	}
	cfg.MaxConns = 6
	cfg.ConnConfig.ConnectTimeout = time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Postgres{pool: pool}
	tx, err := pool.Begin(ctx)
	if err == nil {
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(40310502);
CREATE TABLE IF NOT EXISTS notifier_notifications (
 domain_key TEXT PRIMARY KEY, hazard_id TEXT NOT NULL, payload JSONB NOT NULL);
CREATE INDEX IF NOT EXISTS notifier_by_hazard ON notifier_notifications(hazard_id,domain_key);
CREATE TABLE IF NOT EXISTS notifier_processed (message_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS notifier_counts (
 singleton BOOLEAN PRIMARY KEY CHECK(singleton), processed BIGINT NOT NULL DEFAULT 0,
 notifications BIGINT NOT NULL DEFAULT 0, critical BIGINT NOT NULL DEFAULT 0, coastal BIGINT NOT NULL DEFAULT 0);
INSERT INTO notifier_counts(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS notifier_latency (
 metric INTEGER NOT NULL, upper_ms BIGINT NOT NULL, samples BIGINT NOT NULL,
 PRIMARY KEY(metric,upper_ms));`)
		if err == nil {
			err = tx.Commit(ctx)
		}
	}
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize notifier database: %w", err)
	}
	return s, nil
}

func (s *Postgres) Deliver(id string, e event.Envelope) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("message ID is required")
	}
	if e.EventType == "hazard.event.deleted" {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `INSERT INTO notifier_processed VALUES($1) ON CONFLICT DO NOTHING`, id)
	if err != nil || result.RowsAffected() == 0 {
		return false, err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return false, err
	}
	n := notificationFor(id, e, now.UTC())
	delivered, err := insertNotification(ctx, tx, e.EventType+":"+e.Data.HazardID+":"+e.Data.Severity, n)
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE notifier_counts SET processed=processed+1 WHERE singleton`)
	}
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		// Unknown commit outcome is retried with the same record/domain keys.
		return false, err
	}
	return delivered, nil
}

func insertNotification(ctx context.Context, tx pgx.Tx, key string, n Notification) (bool, error) {
	payload, err := json.Marshal(n)
	if err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, `INSERT INTO notifier_notifications VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, key, n.HazardID, payload)
	if err != nil || result.RowsAffected() == 0 {
		return false, err
	}
	critical, coastal := 0, 0
	if n.Critical {
		critical = 1
		if n.Source == "SENSOR_PANTAI" {
			coastal = 1
		}
	}
	_, err = tx.Exec(ctx, `UPDATE notifier_counts SET notifications=notifications+1,critical=critical+$1,coastal=coastal+$2 WHERE singleton`, critical, coastal)
	if err != nil {
		return false, err
	}
	values := []*int64{&n.PipelineLatencyMS, n.SourceLatencyMS, nil}
	if coastal == 1 {
		values[2] = &n.IngressLatencyMS
	}
	for metric, value := range values {
		if value != nil {
			_, err = tx.Exec(ctx, `INSERT INTO notifier_latency VALUES($1,$2,1)
 ON CONFLICT(metric,upper_ms) DO UPDATE SET samples=notifier_latency.samples+1`, metric, latencyUpper(*value))
			if err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// At most 201 fine buckets plus 63 tail buckets per metric, regardless of
// history size. Reported p95 is a conservative bucket upper bound; acceptance
// tests use individual durable confirmations, not this approximate metric.
func latencyUpper(ms int64) int64 {
	if ms <= 0 {
		return 0
	}
	if ms <= 10000 {
		return ((ms + 49) / 50) * 50
	}
	for upper := int64(20000); upper > 0; upper *= 2 {
		if ms <= upper {
			return upper
		}
	}
	return 1<<63 - 1
}

func (s *Postgres) Stats() (Stats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var out Stats
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `SELECT notifications,processed,critical,coastal FROM notifier_counts WHERE singleton`).Scan(
		&out.NotificationCount, &out.ProcessedCount, &out.CriticalCount, &out.CoastalCriticalCount)
	if err != nil {
		return out, err
	}
	rows, err := tx.Query(ctx, `WITH bins AS (
 SELECT metric,upper_ms,sum(samples) OVER(PARTITION BY metric ORDER BY upper_ms) AS cumulative,
 sum(samples) OVER(PARTITION BY metric) AS total FROM notifier_latency)
 SELECT metric,min(upper_ms) FROM bins WHERE cumulative >= total-total/20 GROUP BY metric`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	values := []*int64{&out.P95PipelineLatencyMS, &out.P95SourceLatencyMS, &out.P95CoastalCriticalIngressMS}
	for rows.Next() {
		var metric int
		var value int64
		if err := rows.Scan(&metric, &value); err != nil {
			return out, err
		}
		if metric >= 0 && metric < len(values) {
			*values[metric] = value
		}
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}

func (s *Postgres) ListNotifications(ctx context.Context, limit int, cursor string) (NotificationPage, error) {
	out := NotificationPage{Notifications: make([]Notification, 0)}
	if limit < 1 || limit > 1000 || len(cursor) > 4096 {
		return out, ErrInvalidPage
	}
	after := ""
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(decoded) == 0 {
			return out, ErrInvalidPage
		}
		after = string(decoded)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT domain_key,payload FROM notifier_notifications WHERE domain_key>$1 ORDER BY domain_key LIMIT $2`, after, limit+1)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	last := ""
	for rows.Next() {
		var key string
		var payload []byte
		if err := rows.Scan(&key, &payload); err != nil {
			return out, err
		}
		if len(out.Notifications) == limit {
			out.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(last))
			break
		}
		var n Notification
		if err := json.Unmarshal(payload, &n); err != nil {
			return out, err
		}
		out.Notifications = append(out.Notifications, n)
		last = key
	}
	return out, rows.Err()
}

func (s *Postgres) ConfirmNotification(ctx context.Context, id string) (Notification, error) {
	var out Notification
	if len(id) < 1 || len(id) > 128 || strings.ContainsAny(id, "\x00\n\r") {
		return out, ErrInvalidPage
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT payload FROM notifier_notifications WHERE hazard_id=$1 ORDER BY domain_key LIMIT 1`, id).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotificationAbsent
	}
	if err != nil {
		return out, err
	}
	return out, json.Unmarshal(payload, &out)
}

func (s *Postgres) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Postgres) Close() error                   { s.pool.Close(); return nil }
