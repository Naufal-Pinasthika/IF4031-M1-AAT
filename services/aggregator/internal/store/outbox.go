package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/broker"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/jackc/pgx/v5"
)

type outboxRow struct {
	id       int64
	envelope broker.EventEnvelope
}

func (s *Store) ensureOutboxSchema(ctx context.Context, u *unit) error {
	_, err := u.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS hazard_event_outbox (
			id BIGSERIAL PRIMARY KEY,
			envelope JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			published_at TIMESTAMPTZ,
			attempts INTEGER NOT NULL DEFAULT 0,
			failed_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS hazard_event_outbox_pending_idx
			ON hazard_event_outbox(published_at,id)
			WHERE published_at IS NULL AND failed_at IS NULL;
		CREATE INDEX IF NOT EXISTS hazard_event_outbox_failed_idx
			ON hazard_event_outbox(failed_at,id)
			WHERE failed_at IS NOT NULL;`)
	if err != nil {
		return err
	}
	_, err = u.pool.Exec(ctx, `
		ALTER TABLE hazard_event_outbox
		ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
		ALTER TABLE hazard_event_outbox
		ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;`)
	return err
}

// RunOutbox publishes committed event envelopes until the context is canceled.
// Kafka consumers must remain tolerant of duplicate envelopes because publishing
// and marking an outbox row published is not a distributed transaction.
func (s *Store) RunOutbox(ctx context.Context, publisher broker.Publisher) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		more := s.publishOutboxCycle(ctx, publisher)
		s.cleanupOutbox(ctx)
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Store) publishOutboxCycle(ctx context.Context, publisher broker.Publisher) bool {
	if publisher == nil {
		return false
	}
	more := false
	for _, u := range s.onlineUnits() {
		tx, err := u.pool.Begin(ctx)
		if err != nil {
			s.logger.Error("outbox_transaction_failed", "unit", u.name, "error", err)
			continue
		}
		var leader bool
		if err := tx.QueryRow(ctx,
			`SELECT pg_try_advisory_xact_lock(4031)`).Scan(&leader); err != nil {
			s.logger.Error("outbox_leader_check_failed", "unit", u.name, "error", err)
			tx.Rollback(ctx)
			continue
		}
		if !leader {
			tx.Rollback(ctx)
			continue
		}

		rows, err := tx.Query(ctx, `
			SELECT id, envelope
			FROM hazard_event_outbox
			WHERE published_at IS NULL AND failed_at IS NULL
			ORDER BY id
			LIMIT 500
			FOR UPDATE`)
		if err != nil {
			s.logger.Error("outbox_query_failed", "unit", u.name, "error", err)
			tx.Rollback(ctx)
			continue
		}

		pending := make([]outboxRow, 0)
		ids := make([]int64, 0)
		for rows.Next() {
			var id int64
			var payload []byte
			if err := rows.Scan(&id, &payload); err != nil {
				s.logger.Error("outbox_scan_failed", "unit", u.name, "error", err)
				continue
			}
			var envelope broker.EventEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				s.logger.Error("outbox_decode_failed", "unit", u.name, "outbox_id", id, "error", err)
				if _, updateErr := tx.Exec(ctx, `
					UPDATE hazard_event_outbox
					SET attempts=attempts+1,
					    failed_at=CASE WHEN attempts+1 >= 3 THEN NOW() ELSE failed_at END
					WHERE id=$1`, id); updateErr != nil {
					s.logger.Error("outbox_park_failed", "unit", u.name, "outbox_id", id, "error", updateErr)
				}
				continue
			}
			pending = append(pending, outboxRow{id: id, envelope: envelope})
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			s.logger.Error("outbox_rows_failed", "unit", u.name, "error", err)
		}
		rows.Close()

		if len(pending) > 0 {
			envelopes := make([]broker.EventEnvelope, 0, len(pending))
			for _, row := range pending {
				envelopes = append(envelopes, row.envelope)
			}
			publishCtx, publishCancel := context.WithTimeout(ctx, 5*time.Second)
			err := publisher.PublishBatch(publishCtx, envelopes)
			publishCancel()
			if err != nil {
				s.logger.Error("outbox_publish_failed", "unit", u.name, "count", len(envelopes), "error", err)
				tx.Rollback(ctx)
				continue
			}
			if _, err := tx.Exec(ctx, `
				UPDATE hazard_event_outbox
				SET published_at=NOW()
				WHERE id=ANY($1) AND published_at IS NULL`,
				ids); err != nil {
				s.logger.Error("outbox_mark_published_failed", "unit", u.name, "error", err)
				tx.Rollback(ctx)
				continue
			}
			more = more || len(pending) == 500
		}
		if err := tx.Commit(ctx); err != nil {
			s.logger.Error("outbox_transaction_commit_failed", "unit", u.name, "error", err)
		}
	}
	return more
}

func (s *Store) firstOnlineUnit() *unit {
	s.state.RLock()
	defer s.state.RUnlock()
	for _, u := range s.units {
		if u.online {
			return u
		}
	}
	return nil
}

func (s *Store) onlineUnits() []*unit {
	s.state.RLock()
	defer s.state.RUnlock()
	result := make([]*unit, 0)
	for _, u := range s.units {
		if u.online {
			result = append(result, u)
		}
	}
	return result
}

func (s *Store) cleanupOutbox(ctx context.Context) {
	for _, u := range s.onlineUnits() {
		op, cancel := context.WithTimeout(ctx, s.timeout)
		_, err := u.pool.Exec(op, `
			DELETE FROM hazard_event_outbox
			WHERE published_at IS NOT NULL
			  AND published_at < NOW()-INTERVAL '7 days'`)
		cancel()
		if err != nil {
			s.logger.Error("outbox_cleanup_failed", "unit", u.name, "error", err)
		}
	}
}

func (s *Store) enqueueOutbox(ctx context.Context, envelope broker.EventEnvelope) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox envelope: %w", err)
	}

	u := s.firstOnlineUnit()
	if u == nil {
		return fmt.Errorf("no outbox storage unit is available")
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err = u.pool.Exec(op, `
		INSERT INTO hazard_event_outbox (envelope, created_at)
		VALUES ($1::jsonb,NOW())`,
		payload)
	return err
}

func outboxEnvelope(ctx context.Context, eventType string, event model.HazardEvent) broker.EventEnvelope {
	correlationID := correlation.ID(ctx)
	if correlationID == "" {
		correlationID = "aggregator"
	}
	return broker.EventEnvelope{
		EventType:     eventType,
		SchemaVersion: 1,
		CorrelationID: correlationID,
		PublishedAt:   time.Now().UTC(),
		Data:          event,
	}
}

func enqueueOutboxTx(ctx context.Context, tx pgx.Tx, correlationID, eventType string, event model.HazardEvent) error {
	envelope := broker.EventEnvelope{
		EventType:     eventType,
		SchemaVersion: 1,
		CorrelationID: correlationID,
		PublishedAt:   time.Now().UTC(),
		Data:          event,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO hazard_event_outbox (envelope,created_at)
		VALUES ($1::jsonb,NOW())`,
		payload)
	return err
}
