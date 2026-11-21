package store

import "context"

// EnableCriticalLane runs before workers start. Existing undelivered intents
// remain intact; only an additional independently acknowledged projection is
// created. Historical payloads already cleaned up are not invented or replayed.
func (c *Coordinator) EnableCriticalLane(ctx context.Context) error {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.pool.Exec(op, `WITH pending AS MATERIALIZED (
 SELECT version,source,hazard_id FROM ingestion_outbox
 WHERE delivered_at IS NULL AND source='SENSOR_PANTAI'
 AND payload->'attributes'->>'critical'='true' FOR UPDATE
 ) INSERT INTO ingestion_projections(version,target,source,hazard_id)
 SELECT version,'broker:critical',source,hazard_id FROM pending ON CONFLICT DO NOTHING`)
	if err == nil {
		c.criticalLane = true
	}
	return err
}
