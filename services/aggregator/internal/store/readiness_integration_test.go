package store

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresReadinessDoesNotSerializeUnavailableUnits(t *testing.T) {
	_, databaseURL := integrationCoordinator(t)
	s, err := New(t.Context(), []config.StorageUnit{{Name: "store-c", DatabaseURL: databaseURL}}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var unavailable []*unit
	for range 2 {
		cfg, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxConns = 1
		pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		held, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		unavailable = append(unavailable, &unit{probe: pool})
	}
	probe := &Store{units: append(unavailable, s.units[0]), timeout: time.Second}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	if err := probe.Ping(ctx); err != nil {
		t.Fatalf("a reachable third unit was hidden behind earlier blocked probes: %v", err)
	}
	probe.units = unavailable
	ctx2, cancel2 := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel2()
	if err := probe.Ping(ctx2); err == nil {
		t.Fatal("all unavailable units incorrectly reported ready")
	}
}
