package store

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestProbeDeadlineNeedsConsecutiveEvidence(t *testing.T) {
	u := &unit{name: "test", online: true, ready: true}
	s := &Store{units: []*unit{u}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for n := 1; n <= 3; n++ {
		s.noteProbeFailure(t.Context(), u, context.DeadlineExceeded)
		if n < 3 && s.readStatus().Partial {
			t.Fatal("one ambiguous probe deadline withdrew healthy storage")
		}
	}
	if !s.readStatus().Partial || u.online || u.generation != 1 {
		t.Fatal("persistent probe failure did not withdraw storage")
	}
}

func TestProbeCancellationAndDefinitiveFailure(t *testing.T) {
	u := &unit{name: "test", online: true, ready: true}
	s := &Store{units: []*unit{u}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s.noteProbeFailure(ctx, u, context.Canceled)
	if u.probeTimeouts != 0 || !u.online {
		t.Fatal("shutdown cancellation changed storage health")
	}
	s.noteProbeFailure(t.Context(), u, &pgconn.PgError{Code: "57P01"})
	if u.online || !s.readStatus().Partial {
		t.Fatal("definitive database shutdown was not immediately visible")
	}
}
