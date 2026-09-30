package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

type EventFilter struct {
	Source     string
	HazardType string
	Severity   string
	From       *time.Time
	To         *time.Time
	Limit      int
	Offset     int
}

func (s *Store) ListEvents(ctx context.Context, filter EventFilter) ([]model.HazardEvent, error) {
	conditions := make([]string, 0, 5)
	args := make([]any, 0, 7)
	addCondition := func(condition string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf("%s = $%d", condition, len(args)))
	}

	if filter.Source != "" {
		addCondition("source", filter.Source)
	}
	if filter.HazardType != "" {
		addCondition("hazard_type", filter.HazardType)
	}
	if filter.Severity != "" {
		addCondition("severity", filter.Severity)
	}
	if filter.From != nil {
		args = append(args, *filter.From)
		conditions = append(conditions, fmt.Sprintf("occurred_at >= $%d", len(args)))
	}
	if filter.To != nil {
		args = append(args, *filter.To)
		conditions = append(conditions, fmt.Sprintf("occurred_at <= $%d", len(args)))
	}

	query := `SELECT hazard_id, source, source_ref_id, hazard_type, severity,
		area_name, latitude, longitude, occurred_at, ingested_at, attributes
		FROM hazard_events`
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	query += fmt.Sprintf(" ORDER BY occurred_at DESC, hazard_id DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list hazard events: %w", err)
	}
	defer rows.Close()

	events := make([]model.HazardEvent, 0)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan hazard event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hazard events: %w", err)
	}

	return events, nil
}

func (s *Store) GetEventByID(ctx context.Context, id string) (*model.HazardEvent, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT hazard_id, source, source_ref_id, hazard_type, severity,
			area_name, latitude, longitude, occurred_at, ingested_at, attributes
		FROM hazard_events
		WHERE hazard_id = $1
	`, id)

	event, err := scanEvent(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get hazard event %q: %w", id, err)
	}
	return &event, nil
}

func scanEvent(row interface{ Scan(...any) error }) (model.HazardEvent, error) {
	var event model.HazardEvent
	var attributes []byte
	if err := row.Scan(
		&event.HazardID,
		&event.Source,
		&event.SourceRefID,
		&event.HazardType,
		&event.Severity,
		&event.AreaName,
		&event.Latitude,
		&event.Longitude,
		&event.OccuredAt,
		&event.IngestedAt,
		&attributes,
	); err != nil {
		return model.HazardEvent{}, err
	}
	if err := json.Unmarshal(attributes, &event.Attributes); err != nil {
		return model.HazardEvent{}, fmt.Errorf("decode attributes: %w", err)
	}
	return event, nil
}
