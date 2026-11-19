package store

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func beginAttempt(t *testing.T, c *Coordinator, holder string, token int64, interval time.Duration) PollAttempt {
	t.Helper()
	a, run, err := c.BeginPoll(t.Context(), "BMKG", holder, token, interval)
	if err != nil || !run {
		t.Fatalf("begin: run=%v err=%v", run, err)
	}
	return a
}

func TestPostgresPollGateHasOneWinnerAndFencesStaleRequests(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "owner")
	var winners atomic.Int32
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			_, run, err := c.BeginPoll(t.Context(), "BMKG", "owner", lease.Token, time.Second)
			if err != nil && !errors.Is(err, ErrPollBusy) {
				t.Error(err)
			}
			if run {
				winners.Add(1)
			}
		})
	}
	workers.Wait()
	if winners.Load() != 1 {
		t.Fatalf("poll winners %d", winners.Load())
	}
	items, err := c.ListPollAttempts(t.Context(), "BMKG", 100)
	if err != nil || len(items) != 1 {
		t.Fatalf("poll history: %v %v", items, err)
	}
	if err := c.ValidatePoll(t.Context(), "BMKG", "owner", lease.Token, items[0].ID); err != nil {
		t.Fatal(err)
	}
	expire(t, c)
	replacement := acquire(t, c, "replacement")
	if err := c.ValidatePoll(t.Context(), "BMKG", "owner", lease.Token, items[0].ID); !errors.Is(err, ErrPollSuperseded) {
		t.Fatalf("stale HTTP guard valid: %v", err)
	}
	if err := c.FinishPoll(t.Context(), items[0], time.Second, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old owner completed poll: %v", err)
	}
	if _, run, err := c.BeginPoll(t.Context(), "BMKG", "replacement", replacement.Token, time.Second); err != nil || run {
		t.Fatalf("uncertain takeover skipped cooldown: %v %v", run, err)
	}
	items, _ = c.ListPollAttempts(t.Context(), "BMKG", 100)
	if items[0].Status != "uncertain" || items[0].LastError != "owner_epoch_changed" {
		t.Fatalf("abandoned poll hidden: %+v", items[0])
	}
}

func TestPostgresCompletedPollCadenceSurvivesOwnershipChange(t *testing.T) {
	c, _ := integrationCoordinator(t)
	old := acquire(t, c, "old")
	a := beginAttempt(t, c, "old", old.Token, time.Minute)
	if err := c.FinishPoll(t.Context(), a, time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseLease(t.Context(), "BMKG", "old", old.Token); err != nil {
		t.Fatal(err)
	}
	next := acquire(t, c, "new")
	deferred, run, err := c.BeginPoll(t.Context(), "BMKG", "new", next.Token, time.Minute)
	if err != nil || run || deferred.RetryAfter <= 0 {
		t.Fatalf("ownership reset schedule: %v %v %+v", run, err, deferred)
	}
	// Fault injection changes only this test schema, not a live node's clock.
	if _, err := c.pool.Exec(t.Context(), `UPDATE source_poll_heads SET next_poll_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	b := beginAttempt(t, c, "new", next.Token, time.Minute)
	if b.ID <= a.ID || b.PreviousID == nil || *b.PreviousID != a.ID {
		t.Fatalf("lost durable chain: %+v", b)
	}
}

func TestPostgresUncertainPollPreservesCheckpointAndUsesBoundedRetry(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "owner")
	checkpoint := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	if err := c.AdvanceTimeCursor(t.Context(), "BMKG", "owner", lease.Token, checkpoint); err != nil {
		t.Fatal(err)
	}
	a := beginAttempt(t, c, "owner", lease.Token, time.Second)
	var snapshot struct {
		OccurredAt time.Time `json:"occurred_at"`
	}
	if err := json.Unmarshal(a.Checkpoint, &snapshot); err != nil || !snapshot.OccurredAt.Equal(checkpoint) {
		t.Fatalf("missing input checkpoint: %s %v", a.Checkpoint, err)
	}
	if err := c.FinishPoll(t.Context(), a, time.Second, errors.New("response lost")); err != nil {
		t.Fatal(err)
	}
	if _, run, err := c.BeginPoll(t.Context(), "BMKG", "owner", lease.Token, time.Second); err != nil || run {
		t.Fatalf("uncontrolled retry: %v %v", run, err)
	}
	got, err := c.TimeCursor(t.Context(), "BMKG", time.Time{})
	if err != nil || !got.Equal(checkpoint) {
		t.Fatalf("uncertain cycle advanced checkpoint: %v %v", got, err)
	}
	items, _ := c.ListPollAttempts(t.Context(), "BMKG", 100)
	if len(items) != 1 || items[0].Status != "uncertain" || items[0].LastError != "response lost" {
		t.Fatalf("uncertain evidence: %+v", items)
	}
	if string(items[0].Checkpoint) != string(a.Checkpoint) {
		t.Fatalf("checkpoint snapshot changed: %s", items[0].Checkpoint)
	}
	if err := c.FinishPoll(t.Context(), a, time.Second, nil); !errors.Is(err, ErrPollSuperseded) {
		t.Fatalf("finished cycle rewritten: %v", err)
	}
}

func TestPostgresV1MigrationPreservesLeaseAndCursor(t *testing.T) {
	c, url := integrationCoordinator(t)
	lease := acquire(t, c, "owner")
	checkpoint := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	if err := c.AdvanceTimeCursor(t.Context(), "BMKG", "owner", lease.Token, checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := c.pool.Exec(t.Context(), `DROP TABLE source_poll_heads;DROP TABLE poll_attempts;UPDATE coordination_schema SET version=1`); err != nil {
		t.Fatal(err)
	}
	next, err := NewCoordinator(t.Context(), url, 2*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if err := next.ValidateLease(t.Context(), "BMKG", "owner", lease.Token); err != nil {
		t.Fatal(err)
	}
	got, err := next.TimeCursor(t.Context(), "BMKG", time.Time{})
	if err != nil || !got.Equal(checkpoint) {
		t.Fatalf("migration changed checkpoint: %v %v", got, err)
	}
	_ = beginAttempt(t, next, "owner", lease.Token, time.Second)
}
