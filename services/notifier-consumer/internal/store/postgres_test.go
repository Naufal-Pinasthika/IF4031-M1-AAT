package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLatencyUpperBounds(t *testing.T) {
	for _, n := range []int64{0, 1, 49, 50, 51, 4999, 5000, 5001, 10000, 10001, 999999999, 1<<63 - 1} {
		if latencyUpper(n) < n {
			t.Fatalf("bucket under-reports %d", n)
		}
	}
}

func TestPostgresConcurrentReceiversAndLegacyImport(t *testing.T) {
	databaseURL := os.Getenv("NOTIFIER_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set NOTIFIER_TEST_DATABASE_URL to a disposable PostgreSQL database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("notifier_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	isolatedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := isolatedURL.Query()
	query.Set("search_path", schema)
	isolatedURL.RawQuery = query.Encode()
	a, err := OpenPostgres(ctx, isolatedURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenPostgres(ctx, isolatedURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var activeSchema string
	if err := a.pool.QueryRow(ctx, "SELECT current_schema()").Scan(&activeSchema); err != nil || activeSchema != schema {
		t.Fatalf("test schema isolation failed: schema=%q err=%v", activeSchema, err)
	}
	e := event.Envelope{EventType: event.UpsertedType, PublishedAt: time.Now(), Data: event.HazardEvent{
		HazardID: "hz-shared", Source: "SENSOR_PANTAI", Severity: "AWAS", IngestedAt: time.Now(), Attributes: map[string]any{"critical": true}}}
	var workers sync.WaitGroup
	var delivered atomic.Int32
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			s := a
			if i%2 != 0 {
				s = b
			}
			ok, err := s.Deliver(fmt.Sprintf("topic:%d", i%2), e)
			if err != nil {
				t.Error(err)
			}
			if ok {
				delivered.Add(1)
			}
		}(i)
	}
	workers.Wait()
	if delivered.Load() != 1 {
		t.Fatalf("delivered %d times", delivered.Load())
	}
	stats, err := b.Stats()
	if err != nil || stats.NotificationCount != 1 || stats.ProcessedCount != 2 || stats.CoastalCriticalCount != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	legacyPath := t.TempDir() + "/legacy.db"
	legacy, err := Open(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	e.Data.HazardID = "hz-legacy"
	if _, err := legacy.Deliver("legacy:0", e); err != nil {
		t.Fatal(err)
	}
	legacy.Close()
	for i := 0; i < 2; i++ {
		if err := a.ImportBolt(ctx, legacyPath); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.ConfirmNotification(ctx, "hz-legacy"); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.Deliver("legacy:0", e); err != nil || ok {
		t.Fatalf("legacy replay delivered=%v err=%v", ok, err)
	}
	stats, err = b.Stats()
	if err != nil || stats.NotificationCount != 2 || stats.ProcessedCount != 3 {
		t.Fatalf("import stats=%+v err=%v", stats, err)
	}
}
