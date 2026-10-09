package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string, logger *slog.Logger) (*Store, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres configuration: %w", err)
	}
	poolConfig.ConnConfig.Tracer = &postgresTracer{logger: logger}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store := &Store{pool: pool}
	if err := store.ensureSchema(ctx); err != nil {
		store.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Upsert(ctx context.Context, event *model.HazardEvent) error {
	if event == nil {
		return fmt.Errorf("hazard event is required")
	}
	if event.Source == "" || event.SourceRefID == "" {
		return fmt.Errorf("hazard event source and source_ref_id are required")
	}

	if event.IngestedAt.IsZero() {
		event.IngestedAt = time.Now().UTC()
	}

	attributes, err := json.Marshal(event.Attributes)
	if err != nil {
		return fmt.Errorf("marshal hazard event attributes: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		INSERT INTO hazard_events (
			source, source_ref_id, hazard_type, severity,
			area_name, latitude, longitude, occurred_at, ingested_at, attributes
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
		ON CONFLICT (source, source_ref_id) DO UPDATE SET
			hazard_type = EXCLUDED.hazard_type,
			severity = EXCLUDED.severity,
			area_name = EXCLUDED.area_name,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			occurred_at = EXCLUDED.occurred_at,
			ingested_at = EXCLUDED.ingested_at,
			attributes = EXCLUDED.attributes
		RETURNING hazard_id
	`, event.Source, event.SourceRefID, event.HazardType, event.Severity,
		event.AreaName, event.Latitude, event.Longitude, event.OccuredAt,
		event.IngestedAt, attributes).Scan(&event.HazardID)
	if err != nil {
		return fmt.Errorf("upsert hazard event %s/%s: %w", event.Source, event.SourceRefID, err)
	}

	return nil
}

func (s *Store) ensureSchema(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE SEQUENCE IF NOT EXISTS hazard_event_id_seq
	`); err != nil {
		return fmt.Errorf("ensure hazard event sequence: %w", err)
	}

	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS hazard_events (
			hazard_id TEXT NOT NULL DEFAULT ('HE-' || nextval('hazard_event_id_seq')),
			source TEXT NOT NULL,
			source_ref_id TEXT NOT NULL,
			hazard_type TEXT NOT NULL,
			severity TEXT NOT NULL,
			area_name TEXT NOT NULL,
			latitude DOUBLE PRECISION NOT NULL,
			longitude DOUBLE PRECISION NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL,
			ingested_at TIMESTAMPTZ NOT NULL,
			attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
			UNIQUE (hazard_id),
			PRIMARY KEY (source, source_ref_id)
		)
	`)
	if err != nil {
		return fmt.Errorf("ensure hazard_events schema: %w", err)
	}

	if _, err := s.pool.Exec(ctx, `
		ALTER TABLE hazard_events
		ALTER COLUMN hazard_id SET DEFAULT ('HE-' || nextval('hazard_event_id_seq'))
	`); err != nil {
		return fmt.Errorf("ensure hazard_id default: %w", err)
	}

	return nil
}
