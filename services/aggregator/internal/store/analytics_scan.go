package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const analyticsBatchSize = 2048

// The disjoint partial indexes cover both branches, including tombstones.
// Returning the retraction flag as a literal avoids detoasting attributes.
const analyticsQuery = `SELECT * FROM (
 (SELECT hazard_id,ingestion_version,revision,hazard_type,severity,area_name,occurred_at,false AS retracted
 FROM hazard_events WHERE partition_id=$1 AND COALESCE(attributes->'_retracted','false'::jsonb)<>'true'::jsonb
 ORDER BY hazard_id)
 UNION ALL
 (SELECT hazard_id,ingestion_version,revision,hazard_type,severity,area_name,occurred_at,true AS retracted
 FROM hazard_events WHERE partition_id=$1 AND COALESCE(attributes->'_retracted','false'::jsonb)='true'::jsonb
 ORDER BY hazard_id)
 ) versions ORDER BY hazard_id`

var errAnalyticsUnavailable = errors.New("analytical replica not ready")

// Only fields needed by aggregates cross the network. Mutable filters and
// retractions are applied AFTER reconciling versions, never in the SQL prefix.
type analyticalRecord struct {
	id, hazard, severity, area string
	version, revision          int64
	occurred                   time.Time
	retracted                  bool
}

type analyticalBatch struct {
	rows []analyticalRecord
	err  error
}

type analyticalStream struct {
	unit       *unit
	batches    <-chan analyticalBatch
	rows       []analyticalRecord
	position   int
	done, read bool
	err        error
}

func (s *analyticalStream) head(ctx context.Context) *analyticalRecord {
	if s.position < len(s.rows) {
		return &s.rows[s.position]
	}
	if s.done {
		return nil
	}
	select {
	case <-ctx.Done():
		s.err, s.done = ctx.Err(), true
		return nil
	case batch, ok := <-s.batches:
		if !ok {
			s.done = true
			return nil
		}
		s.err = batch.err
		s.rows, s.position = batch.rows, 0
		s.done = batch.err != nil || len(batch.rows) < analyticsBatchSize
		s.read = s.read || batch.err == nil
		if len(batch.rows) == 0 {
			return nil
		}
		return &s.rows[0]
	}
}

func (s *Store) analyticalBatch(ctx context.Context, tx pgx.Tx) ([]analyticalRecord, error) {
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := tx.Query(op, fmt.Sprintf("FETCH FORWARD %d FROM analytics_rows", analyticsBatchSize))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]analyticalRecord, 0, analyticsBatchSize)
	for rows.Next() {
		var r analyticalRecord
		if err := rows.Scan(&r.id, &r.version, &r.revision, &r.hazard, &r.severity, &r.area, &r.occurred, &r.retracted); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) streamAnalytics(ctx context.Context, u *unit, partition int, output chan<- analyticalBatch) {
	defer close(output)
	err := s.scanAnalytics(ctx, u, partition, output)
	if err != nil {
		select {
		case output <- analyticalBatch{err: err}:
		case <-ctx.Done():
		}
	}
}

func (s *Store) scanAnalytics(ctx context.Context, u *unit, partition int, output chan<- analyticalBatch) error {
	s.state.RLock()
	ready := u.online && u.ready
	s.state.RUnlock()
	if !ready {
		return errAnalyticsUnavailable
	}
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	release, err := u.admitRead(op)
	if err != nil {
		return err
	}
	defer release()
	tx, err := u.operationPool(op).BeginTx(op, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), s.timeout)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	// Prefer the ordered covering indexes to a blocking heap scan/sort even
	// immediately after bulk inserts, before auto-analyze updates statistics.
	// Planner settings are transaction-local and never affect ordinary reads.
	if _, err := tx.Exec(op, fmt.Sprintf("SET LOCAL statement_timeout='%dms'; SET LOCAL idle_in_transaction_session_timeout='10s'; SET LOCAL cursor_tuple_fraction=0.001; SET LOCAL enable_sort=off", max(int64(1), s.timeout.Milliseconds()))); err != nil {
		return err
	}
	// A server cursor scans/sorts each replica partition only once, even when
	// statistics are stale after a burst. FETCH bounds transfer and memory;
	// every database operation retains its original deadline.
	if _, err := tx.Exec(op, "DECLARE analytics_rows NO SCROLL CURSOR FOR "+analyticsQuery, partition); err != nil {
		return err
	}
	for {
		rows, err := s.analyticalBatch(ctx, tx)
		batch := analyticalBatch{rows: rows, err: err}
		select {
		case output <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
		if batch.err != nil || len(batch.rows) < analyticsBatchSize {
			return nil
		}
	}
}

func (s *Store) analyticalPartition(ctx context.Context, partition int, from, to *time.Time) (Analytics, error) {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	streams := make([]*analyticalStream, 0, 2)
	for _, index := range s.destinations(partition) {
		u := s.units[index]
		batches := make(chan analyticalBatch, 1)
		streams = append(streams, &analyticalStream{unit: u, batches: batches})
		workers.Go(func() { s.streamAnalytics(ctx, u, partition, batches) })
	}
	result := newAnalytics()
	days := map[string]map[string]int{}
	for {
		var heads [2]*analyticalRecord
		var selected *analyticalRecord
		for i, stream := range streams {
			heads[i] = stream.head(ctx)
			if r := heads[i]; r != nil && (selected == nil || r.id < selected.id || r.id == selected.id && analyticalNewer(*r, *selected)) {
				selected = r
			}
			if stream.err != nil && !errors.Is(stream.err, errAnalyticsUnavailable) && !budgetError(stream.err) && !availabilityError(stream.err) {
				return result, stream.err
			}
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if selected == nil {
			break
		}
		for i, stream := range streams {
			r := heads[i]
			if stream.err == nil && stream.read && (r == nil || r.id != selected.id || analyticalNewer(*selected, *r)) {
				result.ReadStatus = mergeStatus(result.ReadStatus, ReadStatus{ReplicationDegraded: true, LaggingUnits: []string{stream.unit.name}})
			}
			if r != nil && r.id == selected.id {
				stream.position++
			}
		}
		if selected.retracted || from != nil && selected.occurred.Before(*from) || to != nil && selected.occurred.After(*to) {
			continue
		}
		result.Total++
		result.ByHazard[selected.hazard]++
		result.BySeverity[selected.severity]++
		result.ByArea[selected.area]++
		day := selected.occurred.UTC().Format("2006-01-02")
		if days[day] == nil {
			days[day] = hazardCounts()
		}
		days[day][selected.hazard]++
	}
	read, busy := false, false
	for _, stream := range streams {
		read = read || stream.read
		if stream.err != nil {
			busy = busy || budgetError(stream.err)
			result.ReadStatus = mergeStatus(result.ReadStatus, ReadStatus{Partial: true, UnavailableUnits: []string{stream.unit.name}})
			s.logReadPartial(ctx, readFailureReason(stream.err), "analytics_scan", stream.unit.name, partition, analyticsBatchSize, stream.err)
			if availabilityError(stream.err) && !budgetError(stream.err) {
				s.markDown(stream.unit)
			}
		}
	}
	if !read && busy {
		return result, ErrStorageBusy
	}
	for day, counts := range days {
		result.Daily = append(result.Daily, DailyCounts{Date: day, Counts: counts})
	}
	return result, nil
}

func analyticalNewer(a, b analyticalRecord) bool {
	return a.version > b.version || a.version == b.version && a.revision > b.revision
}
