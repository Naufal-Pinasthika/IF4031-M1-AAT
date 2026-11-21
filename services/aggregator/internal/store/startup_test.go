package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestStartupRetryRecoversTransientConnectionFailure(t *testing.T) {
	calls := 0
	err := retryStartup(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), func() error {
		calls++
		if calls == 1 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestStartupRetryRejectsConfigurationAndHonorsCancellation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, code := range []string{"28P01", "42501", "42601"} {
		calls := 0
		err := retryStartup(t.Context(), logger, func() error {
			calls++
			return &pgconn.PgError{Code: code}
		})
		if err == nil || calls != 1 {
			t.Fatalf("configuration error %s retried: calls=%d err=%v", code, calls, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	calls := 0
	err := retryStartup(ctx, logger, func() error { calls++; return context.DeadlineExceeded })
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("canceled startup retried: calls=%d err=%v", calls, err)
	}
}

func TestStartupRetryHasFiniteAttemptBudget(t *testing.T) {
	calls := 0
	err := retryStartup(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), func() error {
		calls++
		return &pgconn.PgError{Code: "57P03"}
	})
	if err == nil || calls != 6 {
		t.Fatalf("expected six finite attempts, got calls=%d err=%v", calls, err)
	}
}
