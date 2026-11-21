package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

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

func TestRawNetworkTimeoutDoesNotWithdrawWholeStore(t *testing.T) {
	err := fmt.Errorf("write failed: %w", &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded})
	if !budgetError(err) || !availabilityError(err) {
		t.Fatal("wrapped socket timeout was not recognized")
	}
	u := &unit{name: "test", online: true, ready: true}
	s := &Store{units: []*unit{u}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.noteIOFailure(u, err)
	if s.readStatus().Partial || u.generation != 0 {
		t.Fatal("one exhausted socket deadline poisoned shared health")
	}
	for range 3 {
		s.noteProbeFailure(t.Context(), u, err)
	}
	if u.online || !s.readStatus().Partial {
		t.Fatal("persistent independent network timeouts were ignored")
	}
}

func TestDefinitiveIOFailureStillWithdrawsStore(t *testing.T) {
	for _, err := range []error{io.EOF, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}, &pgconn.PgError{Code: "57P01"}} {
		u := &unit{name: "test", online: true, ready: true}
		s := &Store{units: []*unit{u}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		if budgetError(err) {
			t.Fatalf("definitive failure treated as pressure: %v", err)
		}
		s.noteIOFailure(u, err)
		if !s.readStatus().Partial || u.online {
			t.Fatalf("failure concealed: %v", err)
		}
	}
}

func TestProbeCannotActivateRepairOrUndoNewerGeneration(t *testing.T) {
	u := &unit{online: true, ready: false, repairing: true, generation: 3}
	s := &Store{units: []*unit{u}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.noteProbeSuccess(u, 3, "ready")
	if u.ready {
		t.Fatal("probe activated local repair before it committed")
	}
	u.repairing, u.ready, u.generation = false, true, 4
	s.noteProbeSuccess(u, 3, "recovering")
	if !u.ready {
		t.Fatal("stale probe undid successful repair")
	}
	s.noteProbeFailure(t.Context(), u, io.EOF, 3)
	if !u.online || !u.ready {
		t.Fatal("stale failed probe withdrew newer repair epoch")
	}
	u.online, u.ready, u.generation = false, false, 5
	s.noteProbeSuccess(u, 4, "ready")
	if u.online || u.ready {
		t.Fatal("stale probe undid newer failure")
	}
}

func TestProbeLoopContinuesWhileRepairIsBlockedAndJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, done := make(chan struct{}), make(chan struct{})
	var probes, repairs atomic.Int32
	go func() {
		defer close(done)
		runMaintenanceLoops(ctx, 5*time.Millisecond, func(context.Context) { probes.Add(1) }, func(ctx context.Context) {
			if repairs.Add(1) == 1 {
				close(started)
			}
			<-ctx.Done()
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("repair did not start")
	}
	first := probes.Load()
	deadline := time.After(time.Second)
	for probes.Load() < first+3 {
		select {
		case <-deadline:
			t.Fatal("blocked repair stopped probe cadence")
		case <-time.After(time.Millisecond):
		}
	}
	if repairs.Load() != 1 {
		t.Fatal("repair cycles overlapped")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance goroutines did not join")
	}
}

func TestReadWorkBudgetRejectsWithoutPoisoningHealth(t *testing.T) {
	u := &unit{name: "test", online: true, ready: true, readSlots: make(chan struct{}, 1)}
	s := &Store{units: []*unit{u}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	release, err := u.admitRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.admitRead(t.Context()); !errors.Is(err, ErrStorageBusy) || !budgetError(err) {
		t.Fatalf("query queue was not bounded: %v", err)
	}
	for _, ctx := range []context.Context{maintenanceContext(t.Context()), WithReadPriority(t.Context(), true)} {
		release, err := u.admitRead(ctx)
		if err != nil {
			t.Fatal("regular work consumed reserved capacity")
		}
		release()
	}
	release()
	release, err = u.admitRead(t.Context())
	if err != nil {
		t.Fatal("query slot leaked")
	}
	release()
	s.noteIOFailure(u, ErrStorageBusy)
	if s.readStatus().Partial {
		t.Fatal("admission rejection withdrew storage")
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
