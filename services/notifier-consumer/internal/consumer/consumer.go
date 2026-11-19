package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/event"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/store"
)

type Consumer struct {
	reader  messageReader
	brokers []string
	topic   string
	store   notificationStore
	logger  *slog.Logger
}

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type notificationStore interface {
	Deliver(string, event.Envelope) (bool, error)
}

func New(brokers []string, topic, group string, notificationStore *store.Store, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        group,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10 << 20,
			MaxWait:        time.Second,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
		}),
		brokers: brokers,
		topic:   topic,
		store:   notificationStore,
		logger:  logger,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		started := time.Now()
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("kafka_fetch_failed", "error", err, "latency_ms", time.Since(started).Milliseconds())
			if !pause(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}

		correlationID := header(message, "correlation_id")
		c.logger.Info("kafka_fetch_completed", "correlation_id", correlationID, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "latency_ms", time.Since(started).Milliseconds())
		envelope, err := event.Decode(message.Value)
		if err != nil {
			c.logger.Error("event_rejected", "error", err, "correlation_id", correlationID, "partition", message.Partition, "offset", message.Offset)
			if !c.commit(ctx, message, correlationID) {
				return
			}
			continue
		}
		if envelope.CorrelationID != "" {
			correlationID = envelope.CorrelationID
		}

		messageID := message.Topic + ":" + strconv.Itoa(message.Partition) + ":" + strconv.FormatInt(message.Offset, 10)
		var delivered bool
		deliveryStarted := time.Now()
		// Never fetch a later offset while this record is not durable. Kafka
		// commits the highest offset per partition, including earlier records.
		for delay := 100 * time.Millisecond; ; delay = min(2*time.Second, delay*2) {
			if ctx.Err() != nil {
				return
			}
			delivered, err = c.store.Deliver(messageID, envelope)
			if err == nil {
				break
			}
			c.logger.Error("notification_delivery_failed", "error", err, "correlation_id", correlationID, "message_id", messageID, "retry_in_ms", delay.Milliseconds())
			if !pause(ctx, delay) {
				return
			}
		}
		var sourceLatency any
		if envelope.Data.Source != "SENSOR_PANTAI" {
			sourceLatency = nonNegativeMilliseconds(time.Since(envelope.Data.OccurredAt))
		}
		c.logger.Info("notification_delivered", "correlation_id", correlationID, "message_id", messageID, "hazard_id", envelope.Data.HazardID, "delivered", delivered, "latency_ms", time.Since(deliveryStarted).Milliseconds(),
			"pipeline_latency_ms", nonNegativeMilliseconds(time.Since(envelope.PublishedAt)), "source_latency_ms", sourceLatency,
			"ingress_latency_ms", nonNegativeMilliseconds(time.Since(envelope.Data.IngestedAt)))
		if !c.commit(ctx, message, correlationID) {
			return
		}
	}
}

func (c *Consumer) commit(ctx context.Context, message kafka.Message, correlationID string) bool {
	for delay := 100 * time.Millisecond; ctx.Err() == nil; delay = min(2*time.Second, delay*2) {
		started := time.Now()
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			c.logger.Error("kafka_commit_failed", "error", err, "correlation_id", correlationID, "partition", message.Partition, "offset", message.Offset, "latency_ms", time.Since(started).Milliseconds())
			if !pause(ctx, delay) {
				return false
			}
			continue
		}
		c.logger.Info("kafka_commit_completed", "correlation_id", correlationID, "partition", message.Partition, "offset", message.Offset, "latency_ms", time.Since(started).Milliseconds())
		return true
	}
	return false
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *Consumer) Probe(ctx context.Context) error {
	var last error
	for _, broker := range c.brokers {
		if err := c.probeBroker(ctx, broker); err == nil {
			return nil
		} else {
			last = err
		}
	}
	if last == nil {
		return fmt.Errorf("no Kafka broker configured")
	}
	return last
}

func (c *Consumer) probeBroker(ctx context.Context, broker string) error {
	if len(c.brokers) == 0 {
		return fmt.Errorf("no Kafka broker configured")
	}
	connection, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer connection.Close()
	partitions, err := connection.ReadPartitions(c.topic)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return fmt.Errorf("topic %s has no partitions", c.topic)
	}
	return nil
}

func (c *Consumer) Close() error { return c.reader.Close() }

func header(message kafka.Message, name string) string {
	for _, item := range message.Headers {
		if item.Key == name {
			return string(item.Value)
		}
	}
	return ""
}

func nonNegativeMilliseconds(duration time.Duration) int64 {
	if duration < 0 {
		return 0
	}
	return duration.Milliseconds()
}
