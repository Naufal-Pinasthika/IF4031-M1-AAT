package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

// p1
type Checksum struct {
	Source    string    `json:"source"`
	Count     int       `json:"count"`
	Checksum  string    `json:"checksum"`
	Algorithm string    `json:"algorithm"`
	AsOf      time.Time `json:"as_of"`
	ReadStatus
}

// retain conservative degradation encountered on any page
func mergeStatus(a, b ReadStatus) ReadStatus {
	for _, name := range b.UnavailableUnits {
		found := false
		for _, old := range a.UnavailableUnits {
			if old == name {
				found = true
				break
			}
		}
		if !found {
			a.UnavailableUnits = append(a.UnavailableUnits, name)
		}
	}
	sort.Strings(a.UnavailableUnits)
	a.Partial = a.Partial || b.Partial
	return a
}

// p1 stream canonical batches (replicas are selected by ListEvents)
func (s *Store) forEach(ctx context.Context, f EventFilter, visit func(model.HazardEvent)) (ReadStatus, error) {
	f.Audit = true
	f.Limit = 1000
	f.Offset = 0
	status := ReadStatus{UnavailableUnits: []string{}}
	for {
		if err := ctx.Err(); err != nil {
			return status, err
		}
		page, err := s.ListEvents(ctx, f)
		if err != nil {
			return status, err
		}
		status = mergeStatus(status, page.ReadStatus)
		for _, e := range page.Events {
			visit(e)
		}
		if page.NextCursor == nil {
			break
		}
		f.Cursor = *page.NextCursor
	}
	return status, nil
}

func (s *Store) AuditChecksum(ctx context.Context, source string) (Checksum, error) {
	refs := make([]string, 0)
	status, err := s.forEach(ctx, EventFilter{Source: source}, func(e model.HazardEvent) { refs = append(refs, e.SourceRefID) })
	sort.Strings(refs)
	result := Checksum{Source: source, Count: len(refs), Checksum: fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(refs, "\n")))), Algorithm: "sha256", AsOf: time.Now().UTC(), ReadStatus: status}
	return result, err
}

// p1
type Location struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

// p1
type Locations struct {
	HazardID  string     `json:"hazard_id"`
	Locations []Location `json:"locations"`
	ReadStatus
}

// query actual rows and persisted Downward API metadata (including recovering copies)
func (s *Store) AuditLocation(ctx context.Context, id string) (Locations, error) {
	result := Locations{HazardID: id, Locations: []Location{}}
	for i, u := range s.units {
		s.state.RLock()
		online := u.online
		s.state.RUnlock()
		if !online {
			continue
		}
		op, cancel := context.WithTimeout(ctx, s.timeout)
		rows, err := u.pool.Query(op, `SELECT h.partition_id,m.node_name FROM hazard_events h CROSS JOIN p1_metadata m WHERE h.hazard_id=$1`, id)
		if err == nil {
			for rows.Next() {
				var partition int
				var node string
				if err = rows.Scan(&partition, &node); err != nil {
					break
				}
				role := "replica"
				if len(s.units) == 1 || partition == i {
					role = "primary"
				}
				result.Locations = append(result.Locations, Location{node, u.name, role})
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
		}
		cancel()
		if err != nil {
			if !availabilityError(err) {
				return result, err
			}
			s.markDown(u)
		}
	}
	result.ReadStatus = s.readStatus()
	return result, nil
}

// p1
type UnitStats struct {
	Unit        string  `json:"unit"`
	Node        string  `json:"node"`
	Status      string  `json:"status"`
	Primary     int64   `json:"primary_records"`
	Replica     int64   `json:"replica_records"`
	Writes      int64   `json:"writes"`
	WriteErrors int64   `json:"write_errors"`
	WriteMS     float64 `json:"write_duration_ms"`
	Copied      int64   `json:"copied_records"`
	Checkpoint  string  `json:"checkpoint"`
}

// p1
type StorageStats struct {
	Units []UnitStats `json:"units"`
	ReadStatus
}

// p1: physical counts expose distribution; logical counts belong to AuditChecksum.
func (s *Store) StorageStats(ctx context.Context) (StorageStats, error) {
	result := StorageStats{Units: []UnitStats{}}
	for i, u := range s.units {
		s.state.RLock()
		online, ready := u.online, u.ready
		stats := UnitStats{Unit: u.name, Status: "unavailable", Writes: u.writes, WriteErrors: u.writeErrors, WriteMS: float64(u.writeDuration) / float64(time.Millisecond), Copied: u.copied, Checkpoint: u.checkpoint}
		s.state.RUnlock()
		if online {
			stats.Status = "recovering"
			if ready {
				stats.Status = "ready"
			}
			op, cancel := context.WithTimeout(ctx, s.timeout)
			err := u.pool.QueryRow(op, `SELECT (SELECT node_name FROM p1_metadata WHERE singleton=true),count(*) FILTER(WHERE partition_id=$1),count(*) FILTER(WHERE partition_id<>$1) FROM hazard_events`, i).Scan(&stats.Node, &stats.Primary, &stats.Replica)
			cancel()
			if err != nil {
				if !availabilityError(err) {
					return result, err
				}
				s.markDown(u)
				stats.Status = "unavailable"
			}
		}
		result.Units = append(result.Units, stats)
	}
	result.ReadStatus = s.readStatus()
	return result, nil
}
