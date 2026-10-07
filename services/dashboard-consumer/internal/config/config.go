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
	ConsumerGroup string
	StorePath     string
	HealthTimeout time.Duration
}

func Load() (Config, error) {
	brokers := split(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	cfg := Config{
		ListenAddress: envOrDefault("DASHBOARD_LISTEN_ADDRESS", ":8085"),
		KafkaBrokers:  brokers,
		KafkaTopic:    envOrDefault("KAFKA_TOPIC", "hazard.events.v1"),
		ConsumerGroup: envOrDefault("DASHBOARD_CONSUMER_GROUP", "dashboard-updater-v1"),
		StorePath:     envOrDefault("DASHBOARD_STORE_PATH", "/data/dashboard.db"),
		HealthTimeout: 2 * time.Second,
	}
	if cfg.KafkaTopic == "" || cfg.ConsumerGroup == "" || cfg.StorePath == "" {
		return Config{}, fmt.Errorf("Kafka topic, consumer group, and store path are required")
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
