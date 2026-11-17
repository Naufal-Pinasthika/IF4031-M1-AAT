package store

import (
	"context"
	"encoding/json"
	"time"
)

// p1 one recovery loop + never holds writer lock over the dataset scan
func (s *Store) RunRecovery(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverCycle(ctx)
		}
	}
}

//ready units are also probed (so failed replicas appear in conservative metadata)
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
		op, cancel := context.WithTimeout(ctx, s.timeout)
		err := u.pool.Ping(op)
		cancel()
		if err != nil {
			s.markDown(u)
			continue
		}
		if ready {
			continue
		}
		if !online {
			op, cancel = context.WithTimeout(ctx, s.timeout)
			err = s.ensureSchema(op, u)
			cancel()
			if err != nil {
				continue
			}
			s.state.Lock()
			u.online = true
			s.state.Unlock()
		}
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
		s.markDown(u)
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
					s.markDown(source)
				}
				return
			}
			for _, r := range records {
				if err = s.writeRecord(ctx, u, r); err != nil {
					s.markDown(u)
					return
				}
				cursor = r.event.HazardID
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
				s.markDown(u)
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
		s.markDown(u)
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
	rows, err := u.pool.Query(op, "SELECT "+eventColumns+",revision FROM hazard_events WHERE partition_id=$1 AND hazard_id>$2 ORDER BY hazard_id LIMIT 100", partition, cursor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]record, 0, 100)
	for rows.Next() {
		r := record{partition: partition}
		e := &r.event
		var attrs []byte
		if err = rows.Scan(&e.HazardID, &e.Source, &e.SourceRefID, &e.HazardType, &e.Severity, &e.AreaName, &e.Latitude, &e.Longitude, &e.OccuredAt, &e.IngestedAt, &attrs, &r.revision); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(attrs, &e.Attributes); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}
