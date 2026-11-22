package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresReadDistinguishesBusyFromUnavailablePartition(t *testing.T) {
	s, _, _ := integrationReplicas(t)
	e := testHazard()
	for n := 0; partitionFor(string(e.Source), e.SourceRefID) == 0; n++ {
		e.SourceRefID = fmt.Sprintf("unavailable-partition-%d", n)
	}
	if err := s.Upsert(t.Context(), &e); err != nil {
		t.Fatal(err)
	}
	s.timeout = 60 * time.Millisecond
	var held []*pgxpool.Conn
	defer func() {
		for _, c := range held {
			if c != nil {
				c.Release()
			}
		}
	}()
	// Exhaust query pools without withdrawing any replica's shared health.
	for _, u := range s.units {
		for range u.pool.Config().MaxConns {
			c, err := u.pool.Acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, c)
		}
	}
	if _, err := s.ListEvents(t.Context(), EventFilter{Limit: 20}); !errors.Is(err, ErrStorageBusy) {
		t.Fatalf("reachable databases with saturated query pools must reject: %v", err)
	}
	// Two units now also fail independent liveness. Keep one healthy unit's
	// query pool available: its records must survive the unavailable partition.
	for _, u := range s.units[:2] {
		c, err := u.probe.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	healthy := s.units[2]
	for i, c := range held {
		if c != nil && c.Conn().Config().RuntimeParams["search_path"] == healthy.pool.Config().ConnConfig.RuntimeParams["search_path"] {
			c.Release()
			held[i] = nil
		}
	}
	page, err := s.ListEvents(t.Context(), EventFilter{Limit: 20})
	if err != nil || !page.Partial || len(page.UnavailableUnits) != 2 || len(page.Events) != 1 || page.Events[0].HazardID != e.HazardID {
		t.Fatalf("unavailable partition discarded healthy results: %+v %v", page, err)
	}
	if !s.units[0].online || !s.units[1].online {
		t.Fatal("request-specific liveness evidence changed shared repair state")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.ListEvents(ctx, EventFilter{Limit: 20}); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation was disguised as partial success: %v", err)
	}
}
