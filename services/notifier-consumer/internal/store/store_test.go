package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/bbolt"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
)

func TestNotificationPaginationIsBoundedAndComplete(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "pages.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 13; i++ {
		envelope := event.Envelope{EventType: event.UpsertedType, Data: event.HazardEvent{HazardID: fmt.Sprintf("hz-%02d", i), Severity: "NORMAL"}}
		if _, err := s.Deliver(fmt.Sprint(i), envelope); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 13 {
			t.Fatal("cursor did not terminate")
		}
		page, err := s.ListNotifications(context.Background(), 3, cursor)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Notifications) > 3 {
			t.Fatal("page exceeds bound")
		}
		for _, item := range page.Notifications {
			if seen[item.HazardID] {
				t.Fatal("duplicate notification across pages")
			}
			seen[item.HazardID] = true
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			t.Fatal("cursor did not advance")
		}
		cursor = page.NextCursor
	}
	if len(seen) != 13 {
		t.Fatalf("lost notifications: %d/13", len(seen))
	}
	for _, limit := range []int{0, -1, 1001} {
		if _, err := s.ListNotifications(context.Background(), limit, ""); !errors.Is(err, ErrInvalidPage) {
			t.Fatalf("invalid limit %d accepted", limit)
		}
	}
	if _, err := s.ListNotifications(context.Background(), 3, "!"); !errors.Is(err, ErrInvalidPage) {
		t.Fatal("invalid cursor accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListNotifications(ctx, 3, ""); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled request continued")
	}
	// A page must not decode the remainder of the database just to return three
	// records. Corruption at the end is reported only when that page is reached.
	if err := s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(notificationsBucket).Put([]byte("zzz"), []byte("invalid JSON"))
	}); err != nil {
		t.Fatal(err)
	}
	if page, err := s.ListNotifications(context.Background(), 3, ""); err != nil || len(page.Notifications) != 3 {
		t.Fatalf("page scanned unbounded history: %v", err)
	}
}

func TestDeliverIsIdempotentAndTracksCoastalCriticalLatency(t *testing.T) {
	notificationStore, err := Open(filepath.Join(t.TempDir(), "notifier.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer notificationStore.Close()

	now := time.Now().UTC()
	envelope := event.Envelope{
		EventType:     event.UpsertedType,
		SchemaVersion: 1,
		CorrelationID: "corr-1",
		PublishedAt:   now.Add(-100 * time.Millisecond),
		Data: event.HazardEvent{
			HazardID:   "hazard-1",
			Source:     "SENSOR_PANTAI",
			HazardType: "COASTAL",
			Severity:   "AWAS",
			AreaName:   "Sumatera Barat",
			OccurredAt: now.Add(-time.Second),
			IngestedAt: now.Add(-time.Second),
			Attributes: map[string]any{"critical": true},
		},
	}
	for index := 0; index < 2; index++ {
		delivered, err := notificationStore.Deliver("kafka-message-1", envelope)
		if err != nil {
			t.Fatal(err)
		}
		if delivered != (index == 0) {
			t.Fatalf("delivery %d idempotency result = %v", index, delivered)
		}
	}

	stats, err := notificationStore.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.NotificationCount != 1 || stats.ProcessedCount != 1 || stats.CriticalCount != 1 || stats.CoastalCriticalCount != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.P95PipelineLatencyMS < 0 || stats.P95SourceLatencyMS != 0 || stats.P95CoastalCriticalIngressMS < 900 {
		t.Fatalf("unexpected latency stats: %+v", stats)
	}
	items, _ := notificationStore.Notifications()
	if items[0].SourceLatencyMS != nil {
		t.Fatal("sensor clock must not be subtracted from wall clock")
	}
}
