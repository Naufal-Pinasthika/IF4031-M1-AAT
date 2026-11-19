package store

import (
	"errors"
	"testing"
	"time"
)

func TestPostgresSharedSourceHealthTracksOutageRecoveryAndExpiredOwner(t *testing.T) {
	c, _ := integrationCoordinator(t)
	lease := acquire(t, c, "owner")
	a := beginAttempt(t, c, "owner", lease.Token, time.Second)
	if err := c.FinishPoll(t.Context(), a, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	before, err := c.SourceHealth(t.Context())
	if err != nil || before["BMKG"].State != "UP" || before["BMKG"].LastSuccessAt == nil {
		t.Fatalf("healthy: %v %v", before, err)
	}
	due := func() {
		t.Helper()
		if _, err := c.pool.Exec(t.Context(), `UPDATE source_poll_heads SET next_poll_at=clock_timestamp()-interval '1 second'`); err != nil {
			t.Fatal(err)
		}
	}
	due()
	b := beginAttempt(t, c, "owner", lease.Token, time.Second)
	if err := c.FinishPoll(t.Context(), b, time.Second, errors.New("HTTP 503")); err != nil {
		t.Fatal(err)
	}
	failed, err := c.SourceHealth(t.Context())
	if err != nil || failed["BMKG"].State != "STALE" || failed["BMKG"].StaleSince == nil || !failed["BMKG"].LastSuccessAt.Equal(*before["BMKG"].LastSuccessAt) {
		t.Fatalf("shared outage not retained: %v %v", failed, err)
	}
	due()
	d := beginAttempt(t, c, "owner", lease.Token, time.Second)
	if err := c.FinishPoll(t.Context(), d, time.Second, errors.New("still unavailable")); err != nil {
		t.Fatal(err)
	}
	again, err := c.SourceHealth(t.Context())
	if err != nil || !again["BMKG"].StaleSince.Equal(*failed["BMKG"].StaleSince) {
		t.Fatalf("outage start reset: %v %v", again, err)
	}
	due()
	e := beginAttempt(t, c, "owner", lease.Token, time.Second)
	if err := c.FinishPoll(t.Context(), e, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	recovered, err := c.SourceHealth(t.Context())
	if err != nil || recovered["BMKG"].State != "UP" || recovered["BMKG"].StaleSince != nil {
		t.Fatalf("recovery: %v %v", recovered, err)
	}
	expire(t, c)
	expired, err := c.SourceHealth(t.Context())
	if err != nil || expired["BMKG"].State != "STALE" || expired["BMKG"].StaleSince == nil {
		t.Fatalf("dead owner marked healthy: %v %v", expired, err)
	}
}
