package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

func projectionTest(t *testing.T) (*ReadProjection, string) {
	t.Helper()
	_, url := integrationCoordinator(t)
	p, err := NewReadProjection(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p, url
}
func TestPostgresReadProjectionSharesCompleteViewsAcrossProcesses(t *testing.T) {
	p, url := projectionTest(t)
	q, err := NewReadProjection(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var calls atomic.Int32
	fill := func(context.Context) (EventPage, error) {
		calls.Add(1)
		return EventPage{Events: []model.HazardEvent{{HazardID: "retained"}}}, nil
	}
	_, first, err := p.ReadPage(t.Context(), "arbitrary-filter", time.Second, fill)
	if err != nil {
		t.Fatal(err)
	}
	page, second, err := q.ReadPage(t.Context(), "arbitrary-filter", time.Second, fill)
	if err != nil || calls.Load() != 1 || len(page.Events) != 1 || page.Events[0].HazardID != "retained" || second.After(first.Add(20*time.Millisecond)) {
		t.Fatalf("view/age not shared: calls=%d err=%v", calls.Load(), err)
	}
	_, _, _ = q.ReadPage(t.Context(), "different-filter", time.Second, fill)
	if calls.Load() != 2 {
		t.Fatal("different query shared view")
	}
}
func TestPostgresReadProjectionNeverStoresPartialOrFailedViews(t *testing.T) {
	p, _ := projectionTest(t)
	for _, partial := range []bool{true, false} {
		calls := 0
		fill := func(context.Context) (EventPage, error) {
			calls++
			page := EventPage{ReadStatus: ReadStatus{Partial: partial}}
			if partial {
				return page, nil
			}
			return page, ErrStorageBusy
		}
		for range 2 {
			_, _, _ = p.ReadPage(t.Context(), "not-cacheable", time.Second, fill)
		}
		if calls != 2 {
			t.Fatal("partial/error cached")
		}
	}
	var count int
	if err := p.regular.QueryRow(t.Context(), "SELECT count(*) FROM read_page_projection").Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid retained state: %d %v", count, err)
	}
}
func TestPostgresReadProjectionExpiresAndSurvivesProjectionLoss(t *testing.T) {
	p, _ := projectionTest(t)
	calls := 0
	fill := func(context.Context) (EventPage, error) { calls++; return EventPage{}, nil }
	_, _, _ = p.ReadPage(t.Context(), "expires", 10*time.Millisecond, fill)
	time.Sleep(20 * time.Millisecond)
	_, _, _ = p.ReadPage(t.Context(), "expires", 10*time.Millisecond, fill)
	if calls != 2 {
		t.Fatal("expired view reused")
	}
	if _, err := p.regular.Exec(t.Context(), "DROP TABLE read_page_projection"); err != nil {
		t.Fatal(err)
	}
	_, _, err := p.ReadPage(t.Context(), "lost", time.Second, fill)
	if err != nil || calls != 3 {
		t.Fatalf("cache loss broke canonical fallback: %d %v", calls, err)
	}
}
func TestPostgresReadProjectionFieldPoolIsIsolatedAndCancellationBounded(t *testing.T) {
	p, _ := projectionTest(t)
	c1, err := p.regular.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Release()
	c2, err := p.regular.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Release()
	fill := func(context.Context) (EventPage, error) { return EventPage{}, nil }
	if _, _, err = p.ReadPage(WithReadPriority(t.Context(), true), "field", time.Second, fill); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = p.ReadPage(ctx, "cancel", time.Second, func(ctx context.Context) (EventPage, error) { return EventPage{}, ctx.Err() })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
func TestPostgresReadProjectionStorageIsHardBounded(t *testing.T) {
	p, _ := projectionTest(t)
	if _, err := p.regular.Exec(t.Context(), `INSERT INTO read_page_projection VALUES(32,'x','x',clock_timestamp())`); err == nil {
		t.Fatal("unbounded slot accepted")
	}
	if _, err := p.regular.Exec(t.Context(), `INSERT INTO read_page_projection VALUES(0,'x',decode(repeat('00',1048577),'hex'),clock_timestamp())`); err == nil {
		t.Fatal("oversized page accepted")
	}
}

func TestPostgresReadProjectionCoalescesConcurrentPods(t *testing.T) {
	p, url := projectionTest(t)
	q, err := NewReadProjection(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	began, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 2)
	var calls atomic.Int32
	fill := func(ctx context.Context) (EventPage, error) {
		if calls.Add(1) == 1 {
			close(began)
		}
		select {
		case <-release:
			return EventPage{}, nil
		case <-ctx.Done():
			return EventPage{}, ctx.Err()
		}
	}
	go func() { _, _, e := p.ReadPage(t.Context(), "coalesce", time.Second, fill); done <- e }()
	<-began
	go func() { _, _, e := q.ReadPage(t.Context(), "coalesce", time.Second, fill); done <- e }()
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent pods repeated fill: %d", calls.Load())
	}
}
