package store

import (
	"context"
	"errors"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
	"path/filepath"
	"testing"
)

func TestConfirmationReadsCommittedBoltIdentity(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "notifier.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := event.Envelope{EventType: event.UpsertedType, Data: event.HazardEvent{HazardID: "hz-confirm", Source: "SENSOR_PANTAI", Severity: "AWAS", Attributes: map[string]any{"critical": true}}}
	if _, err := s.Deliver("kafka:0:1", e); err != nil {
		t.Fatal(err)
	}
	n, err := s.ConfirmNotification(context.Background(), "hz-confirm")
	if err != nil || n.HazardID != "hz-confirm" || !n.Critical {
		t.Fatalf("%+v %v", n, err)
	}
	if _, err := s.ConfirmNotification(context.Background(), "missing"); !errors.Is(err, ErrNotificationAbsent) {
		t.Fatal(err)
	}
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ConfirmNotification(c, "hz-confirm"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
