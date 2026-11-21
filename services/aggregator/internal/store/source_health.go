package store

import (
	"context"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/status"
)

// SourceHealth reads small shared polling metadata, never canonical payloads.
// One SQL statement gives all replicas the same source-health interpretation.
// This is called by a bounded background refresher, NOT on each client request.
func (c *Coordinator) SourceHealth(ctx context.Context) (map[string]status.SourceStatus, error) {
	op, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	rows, err := c.pool.Query(op, `SELECT l.source,l.lease_until,clock_timestamp(),
 f.status,f.finished_at,s.finished_at,
 CASE WHEN f.status='uncertain' THEN
 (SELECT MIN(p.finished_at) FROM poll_attempts p WHERE p.source=l.source AND p.status='uncertain'
 AND p.poll_id>COALESCE(s.poll_id,0)) END
 FROM source_leases l
 LEFT JOIN LATERAL (SELECT status,finished_at FROM poll_attempts
 WHERE source=l.source AND status<>'in_flight' ORDER BY poll_id DESC LIMIT 1) f ON true
 LEFT JOIN LATERAL (SELECT poll_id,finished_at FROM poll_attempts
 WHERE source=l.source AND status='completed' ORDER BY poll_id DESC LIMIT 1) s ON true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]status.SourceStatus)
	for rows.Next() {
		var source string
		var leaseUntil, now time.Time
		var finalStatus *string
		var finished, success, stale *time.Time
		if err := rows.Scan(&source, &leaseUntil, &now, &finalStatus, &finished, &success, &stale); err != nil {
			return nil, err
		}
		health := status.SourceStatus{State: "UNAVAILABLE", LastSuccessAt: success}
		if finalStatus != nil && *finalStatus == "completed" && leaseUntil.After(now) {
			health.State = "UP"
		} else {
			if success != nil {
				health.State = "STALE"
			}
			if stale != nil {
				health.StaleSince = stale
			} else if !leaseUntil.After(now) {
				health.StaleSince = &leaseUntil
			} else {
				health.StaleSince = finished
			}
		}
		result[source] = health
	}
	return result, rows.Err()
}
