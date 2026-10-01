package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
)

type EventEnvelope struct {
	EventType     string            `json:"event_type"`
	SchemaVersion int               `json:"schema_version"`
	CorrelationID string            `json:"correlation_id"`
	PublishedAt   time.Time         `json:"published_at"`
	Data          model.HazardEvent `json:"data"`
}

type Publisher interface {
	Publish(ctx context.Context, envelope EventEnvelope) error
	Close() error
}

type KafkaPublisher struct {
	writer *kafka.Writer
	topic  string
}

func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one Kafka broker is required")
	}
	if topic == "" {
		return nil, fmt.Errorf("Kafka topic is required")
	}

	return &KafkaPublisher{
		topic: topic,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 50 * time.Millisecond,
			Async:        false,
		},
	}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, envelope EventEnvelope) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal Kafka event: %w", err)
	}

	key := []byte(envelope.Data.HazardID)
	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: payload,
		Time:  envelope.PublishedAt,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(envelope.EventType)},
			{Key: "schema_version", Value: []byte(fmt.Sprint(envelope.SchemaVersion))},
			{Key: "correlation_id", Value: []byte(envelope.CorrelationID)},
		},
	})
	if err != nil {
		return fmt.Errorf("publish Kafka on %s: %w", envelope.Data.HazardID, err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
