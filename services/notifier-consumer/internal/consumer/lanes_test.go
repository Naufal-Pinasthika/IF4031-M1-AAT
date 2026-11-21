package consumer

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
	"github.com/segmentio/kafka-go"
)

type blockedReader struct{}

func (blockedReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}
func (blockedReader) CommitMessages(context.Context, ...kafka.Message) error { return nil }
func (blockedReader) Close() error                                           { return nil }

func TestCriticalLaneProgressesWhileGeneralFetchIsBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	delivered := make(chan struct{}, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storeFunc(func(string, event.Envelope) (bool, error) { delivered <- struct{}{}; return true, nil })
	r := &testReader{messages: []kafka.Message{notificationMessage(t, 1)}, cancel: cancel}
	lanes := Lanes{
		{reader: blockedReader{}, logger: logger, store: store},
		{reader: r, logger: logger, store: store},
	}
	lanes.Run(ctx)
	select {
	case <-delivered:
	default:
		t.Fatal("critical notification was blocked by the general lane")
	}
}
