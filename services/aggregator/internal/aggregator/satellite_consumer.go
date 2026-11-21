package aggregator

import (
	"context"
	"log/slog"
	"time"
)

type SatelliteConsumer struct {
	stream *SatelliteStream
	logger *slog.Logger
}

// The in-memory stream cursor may lead durable storage only by this bounded
// queue. Any processing error cancels the reader; the next ownership cycle
// reloads the DATABASE checkpoint, never this speculative cursor. Process loss
// therefore replays every uncommitted frame, including the queued tail.
func (c *SatelliteConsumer) ConsumeBatches(ctx context.Context, fromOffset int64, apply func([]SatelliteStreamEvent) error) error {
	cycle, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan SatelliteStreamEvent, 16)
	completed := make(chan error, 1)
	go func() {
		defer cancel()
		for {
			var first SatelliteStreamEvent
			select {
			case <-cycle.Done():
				completed <- cycle.Err()
				return
			case first = <-queue:
			}
			batch := []SatelliteStreamEvent{first}
			timer := time.NewTimer(5 * time.Millisecond)
		collect:
			for len(batch) < 16 {
				select {
				case event := <-queue:
					batch = append(batch, event)
				case <-timer.C:
					break collect
				case <-cycle.Done():
					timer.Stop()
					completed <- cycle.Err()
					return
				}
			}
			timer.Stop()
			if err := apply(batch); err != nil {
				completed <- err
				return
			}
			c.logger.Info("satellite_batch_committed", "frames", len(batch), "last_offset", batch[len(batch)-1].Offset)
		}
	}()
	err := c.stream.Run(cycle, fromOffset, func(event SatelliteStreamEvent) error {
		select {
		case queue <- event:
			return nil
		case <-cycle.Done():
			return cycle.Err()
		}
	})
	cancel()
	workerErr := <-completed
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	if workerErr != nil {
		return workerErr
	}
	return err
}

func NewSatelliteConsumer(baseURL, token string, logger *slog.Logger) *SatelliteConsumer {
	return &SatelliteConsumer{
		stream: NewSatelliteStream(baseURL, token, logger),
		logger: logger,
	}
}

func (c *SatelliteConsumer) Consume(ctx context.Context, fromOffset int64, handlers ...func(SatelliteStreamEvent) error) error {
	return c.stream.Run(ctx, fromOffset, func(event SatelliteStreamEvent) error {
		c.logger.Info(
			"satellite_event_received",
			"event_type", event.Type,
			"offset", event.Offset,
			"payload_bytes", len(event.Data),
		)
		for _, handler := range handlers {
			if err := handler(event); err != nil {
				return err
			}
		}
		return nil
	})
}
