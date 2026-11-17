package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

// p1
var ErrInvalidCursor = errors.New("invalid cursor")

// p1 Audit changes ordering and time filtering, never the canonical replica selection
type EventFilter struct {
	Source, HazardType, Severity string
	From, To, Since              *time.Time
	Limit, Offset                int
	Cursor                       string
	Audit                        bool
	id                           string
}

// p1
const eventColumns = "hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes"

// p1
type pageCursor struct {
	Time  time.Time `json:"t"`
	ID    string    `json:"id"`
	Audit bool      `json:"a"`
}

// p1
func decodeCursor(raw string, audit bool) (*pageCursor, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 2048 {
		return nil, ErrInvalidCursor
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var c pageCursor
	if json.Unmarshal(b, &c) != nil || c.Time.IsZero() || c.ID == "" || c.Audit != audit {
		return nil, ErrInvalidCursor
	}
	return &c, nil
}

// p1
func (s *Store) ListEvents(ctx context.Context, f EventFilter) (EventPage, error) {
	c, err := decodeCursor(f.Cursor, f.Audit)
	if err != nil {
		return EventPage{}, err
	}
	max := 100
	if f.Audit {
		max = 1000
	}
	if f.Limit <= 0 || f.Limit > max {
		f.Limit = 50
	}
	if f.Offset < 0 {
		return EventPage{}, fmt.Errorf("invalid offset")
	}
	if f.Cursor != "" && f.Offset != 0 {
		return EventPage{}, ErrInvalidCursor
	}
	if f.Offset > 100000 {
		return EventPage{}, fmt.Errorf("offset exceeds 100000; use cursor")
	}
	type partResult struct {
		events []model.HazardEvent
		err    error
	}
	results := make(chan partResult, 3)
	partitions := 3
	if len(s.units) == 1 {
		partitions = 1
	}
	for partition := 0; partition < partitions; partition++ {
		go func(p int) {
			events, err := s.readPartition(ctx, p, f, c, f.Offset+f.Limit+1)
			results <- partResult{events, err}
		}(partition)
	}
	events := make([]model.HazardEvent, 0)
	for i := 0; i < partitions; i++ {
		result := <-results
		if result.err != nil {
			return EventPage{}, result.err
		}
		events = append(events, result.events...)
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
	if f.Offset >= len(events) {
		events = events[:0]
	} else {
		events = events[f.Offset:]
	}
	var next *string
	if len(events) > f.Limit {
		events = events[:f.Limit]
		last := events[len(events)-1]
		tm := last.OccuredAt
		if f.Audit {
			tm = last.IngestedAt
		}
		b, _ := json.Marshal(pageCursor{Time: tm, ID: last.HazardID, Audit: f.Audit})
		str := base64.RawURLEncoding.EncodeToString(b)
		next = &str
	}
	return EventPage{Events: events, NextCursor: next, ReadStatus: s.readStatus()}, nil
}

// p1 only ready replicas can answer
// each partition is queried once (with bounded fallback)
func (s *Store) readPartition(ctx context.Context, partition int, f EventFilter, c *pageCursor, limit int) ([]model.HazardEvent, error) {
	conditions := []string{"partition_id=$1"}
	args := []any{partition}
	add := func(column, operator string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf("%s %s $%d", column, operator, len(args)))
	}
	if f.Source != "" {
		add("source", "=", f.Source)
	}
	if f.HazardType != "" {
		add("hazard_type", "=", f.HazardType)
	}
	if f.Severity != "" {
		add("severity", "=", f.Severity)
	}
	if f.From != nil {
		add("occurred_at", ">=", *f.From)
	}
	if f.To != nil {
		add("occurred_at", "<=", *f.To)
	}
	if f.Since != nil {
		add("ingested_at", ">=", *f.Since)
	}
	if f.id != "" {
		add("hazard_id", "=", f.id)
	}
	column, order, op := "occurred_at", "DESC", "<"
	if f.Audit {
		column, order, op = "ingested_at", "ASC", ">"
	}
	if c != nil {
		args = append(args, c.Time, c.ID)
		conditions = append(conditions, fmt.Sprintf("(%s,hazard_id) %s ($%d,$%d)", column, op, len(args)-1, len(args)))
	}
	args = append(args, limit)
	query := "SELECT " + eventColumns + " FROM hazard_events WHERE " + strings.Join(conditions, " AND ") + fmt.Sprintf(" ORDER BY %s %s,hazard_id %s LIMIT $%d", column, order, order, len(args))
	for _, i := range s.destinations(partition) {
		u := s.units[i]
		s.state.RLock()
		ready := u.ready && u.online
		s.state.RUnlock()
		if !ready {
			continue
		}
		op, cancel := context.WithTimeout(ctx, s.timeout)
		events, err := queryEvents(op, u, query, args...)
		cancel()
		if err == nil {
			return events, nil
		}
		if !availabilityError(err) {
			return nil, err
		}
		s.markDown(u)
	}
	return []model.HazardEvent{}, nil
}

// p1
func queryEvents(ctx context.Context, u *unit, query string, args ...any) ([]model.HazardEvent, error) {
	rows, err := u.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]model.HazardEvent, 0)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

// p1
func (s *Store) GetEventByID(ctx context.Context, id string) (EventLookup, error) {
	page, err := s.ListEvents(ctx, EventFilter{id: id, Limit: 1})
	if err != nil {
		return EventLookup{}, err
	}
	result := EventLookup{ReadStatus: page.ReadStatus}
	if len(page.Events) > 0 {
		result.Event = &page.Events[0]
	}
	return result, nil
}

// p1
func (s *Store) readStatus() ReadStatus {
	status := ReadStatus{UnavailableUnits: []string{}}
	s.state.RLock()
	defer s.state.RUnlock()
	for _, u := range s.units {
		if !u.online || !u.ready {
			status.Partial = true
			status.UnavailableUnits = append(status.UnavailableUnits, u.name)
		}
	}
	return status
}

// p1
func scanEvent(row interface{ Scan(...any) error }) (model.HazardEvent, error) {
	var e model.HazardEvent
	var attrs []byte
	if err := row.Scan(&e.HazardID, &e.Source, &e.SourceRefID, &e.HazardType, &e.Severity, &e.AreaName, &e.Latitude, &e.Longitude, &e.OccuredAt, &e.IngestedAt, &attrs); err != nil {
		return e, err
	}
	if err := json.Unmarshal(attrs, &e.Attributes); err != nil {
		return e, err
	}
	e.OccuredAt = e.OccuredAt.UTC()
	e.IngestedAt = e.IngestedAt.UTC()
	return e, nil
}

// p1 identity lookup for late warnings without exposing storage to other services.
func (s *Store) GetEventBySourceRef(ctx context.Context, source, ref string) (EventLookup, error) {
	return s.GetEventByID(ctx, eventID(source, ref))
}
