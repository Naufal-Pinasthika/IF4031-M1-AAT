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
	reader  *kafka.Reader
	brokers []string
	topic   string
	store   *store.Store
	logger  *slog.Logger
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
			continue
		}

		correlationID := header(message, "correlation_id")
		c.logger.Info("kafka_fetch_completed", "correlation_id", correlationID, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "latency_ms", time.Since(started).Milliseconds())
		envelope, err := event.Decode(message.Value)
		if err != nil {
			c.logger.Error("event_rejected", "error", err, "correlation_id", correlationID, "partition", message.Partition, "offset", message.Offset)
			c.commit(ctx, message, correlationID)
			continue
		}
		if envelope.CorrelationID != "" {
			correlationID = envelope.CorrelationID
		}

		messageID := message.Topic + ":" + strconv.Itoa(message.Partition) + ":" + strconv.FormatInt(message.Offset, 10)
		delivered, err := c.store.Deliver(messageID, envelope)
		if err != nil {
			c.logger.Error("notification_delivery_failed", "error", err, "correlation_id", correlationID, "message_id", messageID)
			continue
		}
		c.logger.Info("notification_delivered", "correlation_id", correlationID, "message_id", messageID, "hazard_id", envelope.Data.HazardID, "delivered", delivered)
		c.commit(ctx, message, correlationID)
	}
}

func (c *Consumer) commit(ctx context.Context, message kafka.Message, correlationID string) {
	started := time.Now()
	if err := c.reader.CommitMessages(ctx, message); err != nil {
		c.logger.Error("kafka_commit_failed", "error", err, "correlation_id", correlationID, "partition", message.Partition, "offset", message.Offset, "latency_ms", time.Since(started).Milliseconds())
		return
	}
	c.logger.Info("kafka_commit_completed", "correlation_id", correlationID, "partition", message.Partition, "offset", message.Offset, "latency_ms", time.Since(started).Milliseconds())
}

func (c *Consumer) Probe(ctx context.Context) error {
	if len(c.brokers) == 0 {
		return fmt.Errorf("no Kafka broker configured")
	}
	connection, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
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
