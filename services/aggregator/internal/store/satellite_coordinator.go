package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/jackc/pgx/v5"
)

func (c *Coordinator) SatelliteOffset(ctx context.Context) (int64, error) {
	var offset int64
	err := c.pool.QueryRow(ctx, `SELECT last_offset FROM satellite_feed_state WHERE singleton=true`).Scan(&offset)
	return offset, err
}

func (c *Coordinator) ProcessSatelliteEvent(ctx context.Context, holder string, token int64, eventType string, payload []byte, offset int64) error {
	return c.ProcessSatelliteBatch(ctx, holder, token, []model.SatelliteMutation{{Type: eventType, Payload: payload, Offset: offset, ReceivedAt: time.Now()}})
}

func (c *Coordinator) ProcessSatelliteBatch(ctx context.Context, holder string, token int64, batch []model.SatelliteMutation) error {
	if len(batch) == 0 || len(batch) > 16 {
		return fmt.Errorf("satellite batch must contain 1..16 frames")
	}
	op, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := c.pool.Begin(op)
	if err != nil {
		return err
	}
	defer tx.Rollback(op)
	var now time.Time
	err = tx.QueryRow(op, `UPDATE source_leases SET updated_at=clock_timestamp() WHERE source='SATELIT' AND holder_id=$1 AND fencing_token=$2 AND lease_until>clock_timestamp() RETURNING clock_timestamp()`, holder, token).Scan(&now)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	adapter := &Store{}
	clockOffset := now.Sub(time.Now())
	for _, frame := range batch {
		received := frame.ReceivedAt
		if received.IsZero() {
			received = time.Now()
		}
		now = received.Add(clockOffset)
		events, deleted, err := adapter.processSatelliteEventTx(op, tx, frame.Type, frame.Payload, frame.Offset)
		if err != nil {
			return err
		}
		for i := range events {
			if err = c.admitTx(op, tx, "SATELIT", holder, token, &events[i], fmt.Sprintf("satellite-%d", frame.Offset), now); err != nil {
				return err
			}
		}
		for i := range deleted {
			// A versioned tombstone is repaired just like an ordinary record. Physical
			// deletion would let an unavailable stale replica resurrect a removed fire.
			deleted[i].OccuredAt = now
			deleted[i].Attributes = map[string]any{"_retracted": true}
			if err = c.admitTx(op, tx, "SATELIT", holder, token, &deleted[i], fmt.Sprintf("satellite-%d", frame.Offset), now); err != nil {
				return err
			}
		}
	}
	return tx.Commit(op)
}

func (c *Coordinator) SatelliteStats(ctx context.Context) (map[string]any, error) {
	var offset, late, singles, fires, pending, latest, samples int64
	var latency float64
	err := c.pool.QueryRow(ctx, `SELECT
 (SELECT last_offset FROM satellite_feed_state WHERE singleton=true),
 (SELECT count(*) FROM satellite_late_detections),
 (SELECT count(*) FROM satellite_detections WHERE active AND cluster_id IS NULL),
 (SELECT count(*) FROM satellite_events),
 (SELECT count(*) FROM ingestion_outbox WHERE source='SATELIT' AND delivered_at IS NULL),
 COALESCE((SELECT percentile_cont(0.95) WITHIN GROUP(ORDER BY latency_ms) FROM satellite_pipeline_samples WHERE completed_at>clock_timestamp()-interval '1 hour'),0),
 COALESCE((SELECT latest_offset FROM satellite_session_state WHERE singleton=true),-1),
 (SELECT count(*) FROM satellite_pipeline_samples WHERE completed_at>clock_timestamp()-interval '1 hour')`).Scan(&offset, &late, &singles, &fires, &pending, &latency, &latest, &samples)
	var p95, lag any
	if samples > 0 {
		p95 = latency
	}
	if latest >= 0 {
		lag = max(int64(0), latest-offset)
	}
	return map[string]any{"committed_offset": offset, "latest_offset": latest, "lag_offsets": lag, "late_detections": late, "singleton_detections": singles, "fires": fires, "pending_projections": pending, "p95_ingress_to_canonical_ms": p95, "latency_samples": samples}, err
}

func (c *Coordinator) ObserveSatelliteSession(ctx context.Context, offset int64) error {
	_, err := c.pool.Exec(ctx, `INSERT INTO satellite_session_state(singleton,latest_offset) VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE SET latest_offset=EXCLUDED.latest_offset,observed_at=clock_timestamp()`, offset)
	return err
}
