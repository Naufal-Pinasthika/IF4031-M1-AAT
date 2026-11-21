package store

import (
	"context"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestCanonicalWriteRetryClassification(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"deadline", context.DeadlineExceeded, true},
		{"lost connection", io.EOF, true},
		{"serialization", &pgconn.PgError{Code: "40001"}, true},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"lock timeout", &pgconn.PgError{Code: "55P03"}, true},
		{"statement timeout", &pgconn.PgError{Code: "57014"}, true},
		{"server restart", &pgconn.PgError{Code: "57P01"}, true},
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"constraint", &pgconn.PgError{Code: "23505"}, false},
		{"schema error", &pgconn.PgError{Code: "42P01"}, false},
		{"permission", &pgconn.PgError{Code: "42501"}, false},
		{"admission overload", ErrStorageBusy, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := retryableCanonicalWrite(test.err); got != test.want {
				t.Fatalf("retryable=%v, want %v", got, test.want)
			}
		})
	}
}
