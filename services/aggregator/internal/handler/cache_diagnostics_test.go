package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

func TestPartialFillDiagnosticsLinkEveryWaiterWithoutChangingResult(t *testing.T) {
	var output bytes.Buffer
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	reader := readerFunc(func(ctx context.Context, _ store.EventFilter) (store.EventPage, error) {
		if correlation.ID(ctx) != "leader" {
			t.Errorf("lost fill correlation: %s", correlation.ID(ctx))
		}
		close(started)
		<-release
		return store.EventPage{ReadStatus: store.ReadStatus{Partial: true, UnavailableUnits: []string{"store-a"}}}, nil
	})
	c := NewCachedReader(reader, time.Second, time.Second, slog.New(slog.NewJSONHandler(&output, nil)))
	var workers sync.WaitGroup
	request := func(id string) {
		defer workers.Done()
		page, err := c.ListEvents(correlation.WithID(t.Context(), id), store.EventFilter{Limit: 20})
		if err != nil || !page.Partial {
			t.Errorf("diagnostics changed result: %+v %v", page, err)
		}
	}
	workers.Add(2)
	go request("leader")
	<-started
	go request("follower")
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		joined := false
		for _, flight := range c.flights {
			joined = flight.waiters == 2
		}
		c.mu.Unlock()
		if joined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower did not join")
		}
		time.Sleep(time.Millisecond)
	}
	once.Do(func() { close(release) })
	workers.Wait()
	decoder := json.NewDecoder(&output)
	seen := map[string]bool{}
	for range 2 {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry["msg"] != "read_fill_result" || entry["fill_id"] != "leader" || entry["waiters"] != float64(2) || entry["partial"] != true {
			t.Fatalf("invalid diagnostics: %+v", entry)
		}
		seen[entry["correlation_id"].(string)] = true
	}
	if !seen["leader"] || !seen["follower"] {
		t.Fatalf("missing waiter: %v", seen)
	}
	if len(c.entries) != 0 {
		t.Fatal("partial result was cached")
	}
}
