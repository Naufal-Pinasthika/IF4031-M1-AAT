package store

import (
	"context"
	"encoding/json"
	"time"
)

// p1 one recovery loop + never holds writer lock over the dataset scan
func (s *Store) RunRecovery(ctx context.Context, coordinator *Coordinator, holder string, ttl time.Duration) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshUnitStates(ctx)
			lease, leader, err := coordinator.AcquireOrRenewLease(ctx, "P1_RECOVERY", holder, ttl)
			if err == nil && leader {
				// Repair can take longer than one lease. Renew while working and
				// stop this cycle if the exact epoch is no longer owned.
				cycle, cancel := context.WithCancel(ctx)
				done := make(chan struct{})
				go func() {
					defer close(done)
					renewal := time.NewTicker(ttl / 3)
					defer renewal.Stop()
					for {
						select {
						case <-cycle.Done():
							return
						case <-renewal.C:
							if coordinator.RenewLease(cycle, "P1_RECOVERY", holder, lease.Token, ttl) != nil {
								cancel()
								return
							}
						}
					}
				}()
				s.recoverCycle(cycle)
				// ready describes reachability/bootstrap, not proof that every
				// historical write reached this replica. Incremental anti-entropy
				// continuously closes old gaps without withdrawing healthy stores.
				s.reconcileReady(cycle)
				cancel()
				<-done
			}
		}
	}
}

// Reachability/ready flags are local to each Aggregator. Standby instances must
// observe repairs too; electing a repair worker must not strand their read state.
func (s *Store) refreshUnitStates(ctx context.Context) {
	for _, u := range s.units {
		s.state.RLock()
		generation := u.generation
		s.state.RUnlock()
		op, cancel := context.WithTimeout(ctx, s.timeout)
		var status string
		err := u.probe.QueryRow(op, `SELECT status FROM p1_metadata WHERE singleton=true`).Scan(&status)
		cancel()
		if err != nil {
			s.noteProbeFailure(ctx, u, err)
			continue
		}
		s.state.Lock()
		if u.generation == generation {
			u.probeTimeouts = 0
			u.online = true
			u.ready = status == "ready"
		}
		s.state.Unlock()
	}
}

// ready units are also probed (so failed replicas appear in conservative metadata)
func (s *Store) recoverCycle(ctx context.Context) {
	s.recovery.Lock()
	defer s.recovery.Unlock()
	for i, u := range s.units {
		if ctx.Err() != nil {
			return
		}
		s.state.RLock()
		ready, online := u.ready, u.online
		s.state.RUnlock()
		// refreshUnitStates already checked metadata through the independent
		// probe. Do not issue a second health observation in the same cycle.
		if ready && online {
			continue
		}
		// Metadata is created by the PostgreSQL bootstrap before application
		// migrations. A successful metadata probe only proves reachability:
		// initialization may have timed out, leaving hazard_events absent.
		// Validate/migrate every non-ready unit before attempting a repair.
		// ensureSchema has a non-DDL fast path for already current schemas.
		op, cancel := context.WithTimeout(ctx, s.timeout)
		err := s.ensureSchema(op, u)
		cancel()
		if err != nil {
			s.logger.Warn("storage_schema_repair_failed", "storage_unit", u.name, "error", err)
			continue
		}
		s.state.Lock()
		u.online = true
		s.state.Unlock()
		s.recoverUnit(ctx, i, false)
	}
}

// trusted only during synchronous startup for a previously complete volume
func (s *Store) recoverUnit(ctx context.Context, index int, trusted bool) {
	u := s.units[index]
	s.writer.Lock()
	s.state.Lock()
	u.ready = false
	generation := u.generation
	u.checkpoint = ""
	u.copied = 0
	s.state.Unlock()
	op, cancel := context.WithTimeout(ctx, s.timeout)
	_, err := u.pool.Exec(op, "UPDATE p1_metadata SET status='recovering',checkpoint='',copied=0 WHERE singleton=true")
	cancel()
	s.writer.Unlock()
	if err != nil {
		s.noteIOFailure(u, err)
		return
	}
	for partition := 0; partition < 3; partition++ {
		if len(s.units) == 1 {
			break
		}
		dest := s.destinations(partition)
		if index != dest[0] && index != dest[1] {
			continue
		}
		peer := dest[0]
		if peer == index {
			peer = dest[1]
		}
		source := s.units[peer]
		s.state.RLock()
		// after  all-offline startup there may be no ready seed. When
		// every unit is reachable, each partition's pair can reconcile its
		// union by revision before either recovering copy serves that partition.
		allOnline := true
		for _, candidate := range s.units {
			if !candidate.online {
				allOnline = false
				break
			}
		}
		available := source.online && (source.ready || allOnline)
		s.state.RUnlock()
		if !available {
			if trusted {
				continue
			}
			return
		}
		cursor := ""
		for {
			records, err := s.copyBatch(ctx, source, partition, cursor)
			if err != nil {
				if availabilityError(err) {
					s.noteIOFailure(source, err)
				}
				return
			}
			if err = s.writeRecords(ctx, u, records); err != nil {
				s.noteIOFailure(u, err)
				return
			}
			if len(records) > 0 {
				cursor = records[len(records)-1].event.HazardID
			}
			s.state.Lock()
			u.copied += int64(len(records))
			u.checkpoint = cursor
			copied := u.copied
			s.state.Unlock()
			op, cancel := context.WithTimeout(ctx, s.timeout)
			_, err = u.pool.Exec(op, "UPDATE p1_metadata SET checkpoint=$1,copied=$2 WHERE singleton=true", cursor, copied)
			cancel()
			if err != nil {
				s.noteIOFailure(u, err)
				return
			}
			if len(records) < 100 {
				break
			}
			if ctx.Err() != nil {
				return
			}
		}
	}
	// wait for writes in flight, then reject activation after any failed write during copy
	s.writer.Lock()
	defer s.writer.Unlock()
	s.state.RLock()
	valid := u.online && u.generation == generation
	s.state.RUnlock()
	if !valid {
		return
	}
	op, cancel = context.WithTimeout(ctx, s.timeout)
	_, err = u.pool.Exec(op, "UPDATE p1_metadata SET status='ready',checkpoint='' WHERE singleton=true")
	cancel()
	if err != nil {
		s.noteIOFailure(u, err)
		return
	}
	s.state.Lock()
	u.ready = true
	u.checkpoint = ""
	s.state.Unlock()
}

// cursor restarts from beginning after process restart + checkpoints describe progress only
func (s *Store) copyBatch(ctx context.Context, u *unit, partition int, cursor string) ([]record, error) {
	op, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := u.pool.Query(op, "SELECT "+eventColumns+",revision,fencing_token,ingestion_version FROM hazard_events WHERE partition_id=$1 AND hazard_id>$2 ORDER BY hazard_id LIMIT 100", partition, cursor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]record, 0, 100)
	for rows.Next() {
		r := record{partition: partition}
		e := &r.event
		var attrs []byte
		if err = rows.Scan(&e.HazardID, &e.Source, &e.SourceRefID, &e.HazardType, &e.Severity, &e.AreaName, &e.Latitude, &e.Longitude, &e.OccuredAt, &e.IngestedAt, &attrs, &r.revision, &r.fencingToken, &r.ingestionVersion); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(attrs, &e.Attributes); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// Compare bounded identity pages in BOTH directions. Keep only a cursor, not a
// full in-memory dataset; restarting an elected worker safely rescans. Repair
// is idempotent and the version guard prevents overwriting a newer live write.
func (s *Store) reconcileReady(ctx context.Context) {
	if len(s.units) != 3 {
		return
	}
	if s.antiEntropy == nil {
		s.antiEntropy = make(map[[2]int]string)
	}
	for partition := 0; partition < 3; partition++ {
		pair := s.destinations(partition)
		for direction := 0; direction < 2; direction++ {
			if ctx.Err() != nil {
				return
			}
			from, to := s.units[pair[direction]], s.units[pair[1-direction]]
			s.state.RLock()
			available := from.online && from.ready && to.online && to.ready
			s.state.RUnlock()
			if !available {
				continue
			}
			key := [2]int{partition, direction}
			op, cancel := context.WithTimeout(ctx, s.timeout)
			page, err := queryRecords(op, from, partition, "SELECT "+recordColumns+" FROM hazard_events WHERE partition_id=$1 AND hazard_id>$2 ORDER BY hazard_id LIMIT 1000", partition, s.antiEntropy[key])
			cancel()
			if err != nil {
				continue
			}
			ids := make([]string, 0, len(page))
			for _, r := range page {
				ids = append(ids, r.event.HazardID)
			}
			op, cancel = context.WithTimeout(ctx, s.timeout)
			peers, err := queryRecords(op, to, partition, "SELECT "+recordColumns+" FROM hazard_events WHERE partition_id=$1 AND hazard_id=ANY($2::text[])", partition, ids)
			cancel()
			if err != nil {
				continue
			}
			known := map[string]record{}
			for _, r := range peers {
				known[r.event.HazardID] = r
			}
			repairs := make([]record, 0)
			for _, r := range page {
				peer, exists := known[r.event.HazardID]
				if !exists || newerRecord(r, peer) {
					repairs = append(repairs, r)
				}
			}
			for offset := 0; offset < len(repairs); offset += 100 {
				end := min(offset+100, len(repairs))
				if err = s.writeRecords(ctx, to, repairs[offset:end]); err != nil {
					break
				}
			}
			if err != nil {
				continue
			} // retain cursor until ALL writes acknowledged
			if len(repairs) > 0 {
				s.logger.Info("canonical_replica_repaired", "partition", partition, "source_unit", from.name, "target_unit", to.name, "records", len(repairs))
			}
			if len(page) < 1000 {
				s.antiEntropy[key] = ""
			} else {
				s.antiEntropy[key] = page[len(page)-1].event.HazardID
			}
		}
	}
}
