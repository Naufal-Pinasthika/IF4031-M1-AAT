package aggregator

import (
	"context"
	"log/slog"
)

type SatelliteConsumer struct {
	stream *SatelliteStream
	logger *slog.Logger
}

func NewSatelliteConsumer(baseURL, token string, logger *slog.Logger) *SatelliteConsumer {
	return &SatelliteConsumer{
		stream: NewSatelliteStream(baseURL, token, logger),
		logger: logger,
	}
}

func (c *SatelliteConsumer) Consume(ctx context.Context, handlers ...func(SatelliteStreamEvent) error) error {
	return c.stream.Run(ctx, func(event SatelliteStreamEvent) error {
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
