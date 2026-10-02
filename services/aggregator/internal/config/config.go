package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ListenAddress   string
	DatabaseURL     string
	BMKGURL         string
	BMKGAPIKey      string
	PVMBGURL        string
	PVMBGToken      string
	PollingInterval time.Duration
	HTTPTimeout     time.Duration
	MaxInFlight     int
	StartSince      time.Time
}

func Load() (Config, error) {
	pollingInterval, err := durationFromEnv("AGGREGATOR_POLL_INTERVAL_MS", 5000)
	if err != nil {
		return Config{}, err
	}
	httpTimeout, err := durationFromEnv("AGGREGATOR_HTTP_TIMEOUT_MS", 4000)
	if err != nil {
		return Config{}, err
	}
	maxInFlight, err := positiveIntFromEnv("AGGREGATOR_MAX_IN_FLIGHT", 64)
	if err != nil {
		return Config{}, err
	}

	startSince := time.Unix(0, 0).UTC()
	if raw := os.Getenv("AGGREGATOR_START_SINCE"); raw != "" {
		startSince, err = time.Parse(time.RFC3339, raw)
		if err != nil {
			return Config{}, fmt.Errorf("AGGREGATOR_START_SINCE must be RFC3339: %w", err)
		}
	}

	return Config{
		ListenAddress:   envOrDefault("AGGREGATOR_LISTEN_ADDRESS", ":8080"),
		DatabaseURL:     envOrDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/aggregator?sslmode=disable"),
		BMKGURL:         envOrDefault("BMKG_URL", "http://localhost:8081"),
		BMKGAPIKey:      os.Getenv("BMKG_API_KEY"),
		PVMBGURL:        envOrDefault("PVMBG_URL", "http://localhost:8082"),
		PVMBGToken:      os.Getenv("PVMBG_TOKEN"),
		PollingInterval: pollingInterval,
		HTTPTimeout:     httpTimeout,
		MaxInFlight:     maxInFlight,
		StartSince:      startSince,
	}, nil
}

func durationFromEnv(name string, defaultMS int) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return time.Duration(defaultMS) * time.Millisecond, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer in milliseconds", name)
	}
	return time.Duration(value) * time.Millisecond, nil
}

func positiveIntFromEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
