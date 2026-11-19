package store

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

const recordColumns = eventColumns + ",revision,fencing_token,ingestion_version"

func newerRecord(a, b record) bool {
	return a.ingestionVersion > b.ingestionVersion || a.ingestionVersion == b.ingestionVersion && a.revision > b.revision
}

func (s *Store) queryRecords(ctx context.Context, u *unit, partition int, phase, query string, args ...any) (records []record, err error) {
	pool := u.operationPool(ctx)
	started := time.Now()
	acquireTime := time.Duration(0)
	queryStarted := time.Time{}
	defer func() {
		elapsed := time.Since(started)
		if s.logger == nil || err == nil && elapsed < 100*time.Millisecond {
			return
		}
		queryTime := time.Duration(0)
		if !queryStarted.IsZero() {
			queryTime = time.Since(queryStarted)
		}
		stats := pool.Stat()
		s.logger.Warn("canonical_read_query", "correlation_id", correlation.ID(ctx),
			"storage_unit", u.name, "partition", partition, "phase", phase,
			"acquire_ms", float64(acquireTime)/float64(time.Millisecond),
			"query_and_scan_ms", float64(queryTime)/float64(time.Millisecond),
			"latency_ms", float64(elapsed)/float64(time.Millisecond),
			"pool_acquired", stats.AcquiredConns(), "pool_idle", stats.IdleConns(),
			"pool_max", stats.MaxConns(), "rows", len(records), "error", err)
	}()
	release, err := u.admitRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	// Acquire includes cold connection establishment. Preserve the same deadline.
	conn, err := pool.Acquire(ctx)
	acquireTime = time.Since(started)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	queryStarted = time.Now()
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records = make([]record, 0)
	for rows.Next() {
		r := record{partition: partition}
		e := &r.event
		var attrs []byte
		if err := rows.Scan(&e.HazardID, &e.Source, &e.SourceRefID, &e.HazardType, &e.Severity, &e.AreaName,
			&e.Latitude, &e.Longitude, &e.OccuredAt, &e.IngestedAt, &attrs, &r.revision, &r.fencingToken, &r.ingestionVersion); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(attrs, &e.Attributes); err != nil {
			return nil, err
		}
		e.OccuredAt, e.IngestedAt = e.OccuredAt.UTC(), e.IngestedAt.UTC()
		records = append(records, r)
	}
	return records, rows.Err()
}

func (s *Store) logReadPartial(ctx context.Context, reason, phase, name string, partition, limit int, err error) {
	if s.logger != nil {
		s.logger.Warn("canonical_read_partial", "correlation_id", correlation.ID(ctx),
			"reason", reason, "phase", phase, "storage_unit", name,
			"partition", partition, "fetch_limit", limit, "error", err)
	}
}

func readFailureReason(err error) string {
	if errors.Is(err, ErrStorageBusy) {
		return "query_budget_exhausted"
	}
	if budgetError(err) {
		return "query_timeout"
	}
	return "unit_unavailable"
}

func matchesRecord(e model.HazardEvent, f EventFilter, c *pageCursor) bool {
	if f.Source != "" && string(e.Source) != f.Source || f.HazardType != "" && string(e.HazardType) != f.HazardType ||
		f.Severity != "" && string(e.Severity) != f.Severity || f.id != "" && e.HazardID != f.id ||
		f.From != nil && e.OccuredAt.Before(*f.From) || f.To != nil && e.OccuredAt.After(*f.To) ||
		f.Since != nil && e.IngestedAt.Before(*f.Since) {
		return false
	}
	if c == nil {
		return true
	}
	if f.Audit {
		return e.IngestedAt.After(c.Time) || e.IngestedAt.Equal(c.Time) && e.HazardID > c.ID
	}
	return e.OccuredAt.Before(c.Time) || e.OccuredAt.Equal(c.Time) && e.HazardID < c.ID
}

func (s *Store) readReplicaUnion(ctx context.Context, partition int, f EventFilter, c *pageCursor, limit int, query string, args []any) ([]model.HazardEvent, ReadStatus, error) {
	requested, fetch := limit, limit
	for attempt := 0; ; attempt++ {
		boundedArgs := append([]any(nil), args...)
		boundedArgs[len(boundedArgs)-1] = fetch
		events, status, err := s.readReplicaPage(ctx, partition, f, c, fetch, query, boundedArgs)
		if err != nil || !status.coverageIncomplete || attempt >= 2 || fetch >= max(1000, requested) {
			if len(events) > requested {
				events = events[:requested]
			}
			return events, status, err
		}
		// Latest-version reconciliation can remove stale matching candidates.
		// Fetch a larger prefix instead of returning a falsely complete page.
		// Three rounds and the caller's fill deadline keep work bounded.
		fetch = min(fetch*2, max(1000, requested))
	}
}

func (s *Store) readReplicaPage(ctx context.Context, partition int, f EventFilter, c *pageCursor, limit int, query string, args []any) ([]model.HazardEvent, ReadStatus, error) {
	type response struct {
		unit *unit
		rows []record
		err  error
	}
	dest := s.destinations(partition)
	responses := make(chan response, len(dest))
	for _, index := range dest {
		u := s.units[index]
		go func() {
			s.state.RLock()
			ready := u.online && u.ready
			s.state.RUnlock()
			if !ready {
				responses <- response{unit: u}
				return
			}
			op, cancel := context.WithTimeout(ctx, s.timeout)
			defer cancel()
			rows, err := s.queryRecords(op, u, partition, "prefix", query, args...)
			responses <- response{u, rows, err}
		}()
	}
	status := ReadStatus{UnavailableUnits: []string{}}
	union := map[string]record{}
	var successful []response
	busy := false
	for range dest {
		result := <-responses
		if result.err != nil || result.rows == nil {
			reason := readFailureReason(result.err)
			s.logReadPartial(ctx, reason, "prefix", result.unit.name, partition, limit, result.err)
			status = mergeStatus(status, ReadStatus{Partial: true, UnavailableUnits: []string{result.unit.name}})
			if result.err != nil {
				if budgetError(result.err) {
					busy = true
				} else if availabilityError(result.err) {
					s.markDown(result.unit)
				} else {
					return nil, status, result.err
				}
			}
			continue
		}
		successful = append(successful, result)
		for _, r := range result.rows {
			old, exists := union[r.event.HazardID]
			if !exists || newerRecord(r, old) {
				union[r.event.HazardID] = r
			}
		}
	}
	if ctx.Err() != nil {
		return nil, status, ctx.Err()
	}
	if len(successful) == 0 && busy {
		return nil, status, ErrStorageBusy
	}
	// A newer peer version might no longer match a severity/time filter. Fetch
	// candidates absent from that peer's filtered page BEFORE choosing versions;
	// otherwise a stale matching version could reappear in the response.
	seenByUnit := map[*unit]map[string]record{}
	for _, result := range successful {
		seen := map[string]record{}
		for _, r := range result.rows {
			seen[r.event.HazardID] = r
		}
		missing := []string{}
		for id := range union {
			if _, ok := seen[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			op, cancel := context.WithTimeout(ctx, s.timeout)
			rows, err := s.queryRecords(op, result.unit, partition, "candidate_lookup", "SELECT "+recordColumns+" FROM hazard_events WHERE partition_id=$1 AND hazard_id=ANY($2::text[])", partition, missing)
			cancel()
			if err != nil {
				reason := readFailureReason(err)
				s.logReadPartial(ctx, reason, "candidate_lookup", result.unit.name, partition, limit, err)
				if !availabilityError(err) {
					return nil, status, err
				}
				status = mergeStatus(status, ReadStatus{Partial: true, UnavailableUnits: []string{result.unit.name}})
			} else {
				for _, r := range rows {
					seen[r.event.HazardID] = r
					if newerRecord(r, union[r.event.HazardID]) {
						union[r.event.HazardID] = r
					}
				}
			}
		}
		seenByUnit[result.unit] = seen
	}
	// Compare only after every latest-version lookup has completed: a later
	// lookup can reveal that an earlier unit's filtered candidate was stale.
	for u, seen := range seenByUnit {
		for id, r := range union {
			peer, exists := seen[id]
			if !exists || newerRecord(r, peer) || newerRecord(peer, r) {
				status = mergeStatus(status, ReadStatus{ReplicationDegraded: true, LaggingUnits: []string{u.name}})
				break
			}
		}
	}
	events := make([]model.HazardEvent, 0, len(union))
	for _, r := range union {
		if matchesRecord(r.event, f, c) {
			events = append(events, r.event)
		}
	}
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i].OccuredAt, events[j].OccuredAt
		if f.Audit {
			a, b = events[i].IngestedAt, events[j].IngestedAt
		}
		if a.Equal(b) {
			if f.Audit {
				return events[i].HazardID < events[j].HazardID
			}
			return events[i].HazardID > events[j].HazardID
		}
		if f.Audit {
			return a.Before(b)
		}
		return a.After(b)
	})
	// A lagging copy does not make a successful union incomplete. Prove that
	// no unseen row from EITHER non-exhausted prefix can precede the last row
	// returned here. If stale filtered candidates consumed those prefix slots,
	// refill; if the bound cannot be established, preserve partial=true.
	for _, result := range successful {
		if len(result.rows) < limit {
			continue
		} // exhausted matching rows
		if len(events) < limit || eventBefore(result.rows[len(result.rows)-1].event, events[limit-1], f.Audit) {
			s.logReadPartial(ctx, "coverage_unproven", "coverage", result.unit.name, partition, limit, nil)
			status.Partial = true
			status.coverageIncomplete = true
		}
	}
	if len(events) > limit {
		events = events[:limit]
	}
	return events, status, nil
}

func eventBefore(a, b model.HazardEvent, audit bool) bool {
	tmA, tmB := a.OccuredAt, b.OccuredAt
	if audit {
		tmA, tmB = a.IngestedAt, b.IngestedAt
	}
	if tmA.Equal(tmB) {
		if audit {
			return a.HazardID < b.HazardID
		}
		return a.HazardID > b.HazardID
	}
	if audit {
		return tmA.Before(tmB)
	}
	return tmA.After(tmB)
}
