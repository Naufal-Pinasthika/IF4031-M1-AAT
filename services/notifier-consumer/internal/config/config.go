package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	ListenAddress string
	KafkaBrokers  []string
	KafkaTopic    string
	CriticalTopic string
	ConsumerGroup string
	StorePath     string
	DatabaseURL   string
	ReplicaID     string
	HealthTimeout time.Duration
}

func Load() (Config, error) {
	brokers := split(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	cfg := Config{
		ListenAddress: envOrDefault("NOTIFIER_LISTEN_ADDRESS", ":8086"),
		KafkaBrokers:  brokers,
		KafkaTopic:    envOrDefault("KAFKA_TOPIC", "hazard.events.v1"),
		CriticalTopic: strings.TrimSpace(os.Getenv("KAFKA_CRITICAL_TOPIC")),
		ConsumerGroup: envOrDefault("NOTIFIER_CONSUMER_GROUP", "field-notifier-v1"),
		StorePath:     envOrDefault("NOTIFIER_STORE_PATH", "/data/notifier.db"),
		DatabaseURL:   strings.TrimSpace(os.Getenv("NOTIFIER_DATABASE_URL")),
		ReplicaID:     strings.TrimSpace(os.Getenv("NOTIFIER_REPLICA_ID")),
		HealthTimeout: 2 * time.Second,
	}
	if cfg.KafkaTopic == "" || cfg.ConsumerGroup == "" || cfg.StorePath == "" {
		return Config{}, fmt.Errorf("Kafka topic, consumer group, and store path are required")
	}
	if cfg.CriticalTopic != "" && cfg.CriticalTopic == cfg.KafkaTopic {
		return Config{}, fmt.Errorf("critical Kafka topic must differ from general topic")
	}
	if cfg.ReplicaID != "" && (cfg.DatabaseURL == "" || cfg.CriticalTopic == "") {
		return Config{}, fmt.Errorf("redundant notifier requires a shared database and critical topic")
	}
	return cfg, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func split(value string) []string {
	parts := make([]string, 0)
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			parts = append(parts, item)
		}
	}
	return parts
}
