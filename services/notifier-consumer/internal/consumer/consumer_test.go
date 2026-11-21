package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
	"github.com/segmentio/kafka-go"
)

type testReader struct {
	messages       []kafka.Message
	fetched        int
	committed      []int64
	commitFailures int
	cancel         context.CancelFunc
	beforeFetch    func()
}

func (r *testReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.beforeFetch != nil {
		r.beforeFetch()
	}
	if r.fetched == len(r.messages) {
		r.cancel()
		return kafka.Message{}, context.Canceled
	}
	message := r.messages[r.fetched]
	r.fetched++
	return message, nil
}
func (r *testReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if r.commitFailures > 0 {
		r.commitFailures--
		return errors.New("broker unavailable")
	}
	for _, m := range messages {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}
func (r *testReader) Close() error { return nil }

type storeFunc func(string, event.Envelope) (bool, error)

func (f storeFunc) Deliver(id string, e event.Envelope) (bool, error) { return f(id, e) }

func notificationMessage(t *testing.T, offset int64) kafka.Message {
	t.Helper()
	payload, err := json.Marshal(event.Envelope{EventType: event.UpsertedType, SchemaVersion: 1,
		Data: event.HazardEvent{HazardID: "critical", SourceRefID: "sensor-ref"}})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "hazards", Partition: 0, Offset: offset, Value: payload}
}

func TestFailedDeliveryCannotBeSkippedByLaterOffset(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &testReader{messages: []kafka.Message{notificationMessage(t, 10), notificationMessage(t, 11)}, cancel: cancel}
	attempts := map[string]int{}
	r.beforeFetch = func() {
		if r.fetched == 1 && (attempts["hazards:0:10"] != 2 || len(r.committed) != 1 || r.committed[0] != 10) {
			t.Fatal("fetched a later record before failed record was durable and committed")
		}
	}
	c := &Consumer{reader: r, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), store: storeFunc(func(id string, _ event.Envelope) (bool, error) {
		attempts[id]++
		if id == "hazards:0:10" && attempts[id] == 1 {
			return false, errors.New("disk unavailable")
		}
		return true, nil
	})}
	c.Run(ctx)
	if len(r.committed) != 2 || r.committed[1] != 11 {
		t.Fatalf("commits: %v", r.committed)
	}
}

func TestCommitFailureRetriesBeforeFetchingAnotherRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &testReader{messages: []kafka.Message{notificationMessage(t, 4)}, commitFailures: 2, cancel: cancel}
	deliveries := 0
	r.beforeFetch = func() {
		if r.fetched == 1 && (len(r.committed) != 1 || r.committed[0] != 4) {
			t.Fatal("commit skipped before next fetch")
		}
	}
	c := &Consumer{reader: r, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), store: storeFunc(func(string, event.Envelope) (bool, error) { deliveries++; return true, nil })}
	c.Run(ctx)
	if deliveries != 1 {
		t.Fatalf("commit retry repeated local side effect %d times", deliveries)
	}
}

func TestShutdownWithFailedDeliveryLeavesOffsetUncommitted(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &testReader{messages: []kafka.Message{notificationMessage(t, 8)}, cancel: cancel}
	c := &Consumer{reader: r, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), store: storeFunc(func(string, event.Envelope) (bool, error) { cancel(); return false, errors.New("disk unavailable") })}
	c.Run(ctx)
	if len(r.committed) != 0 {
		t.Fatalf("failed record committed: %v", r.committed)
	}
}
