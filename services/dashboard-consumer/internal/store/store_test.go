package store

import (
	"path/filepath"
	"testing"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/dashboard-consumer/internal/event"
)

func TestOlderAdmissionCannotOverwriteNewerDashboardEvent(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "dashboard.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	newer := event.Envelope{EventType: event.UpsertedType, IngestionVersion: 20, Data: event.HazardEvent{HazardID: "hazard-1", Severity: "AWAS"}}
	if _, err := s.Apply("kafka-20", newer); err != nil {
		t.Fatal(err)
	}
	older := newer
	older.IngestionVersion = 10
	older.Data.Severity = "NORMAL"
	if applied, err := s.Apply("kafka-delayed-10", older); err != nil || applied {
		t.Fatalf("older event applied=%v err=%v", applied, err)
	}
	if applied, err := s.Apply("kafka-replayed-20", newer); err != nil || applied {
		t.Fatalf("duplicate version applied=%v err=%v", applied, err)
	}
	items, err := s.Events()
	if err != nil || len(items) != 1 || items[0].Severity != "AWAS" {
		t.Fatalf("out-of-order dashboard state: %+v %v", items, err)
	}
}
