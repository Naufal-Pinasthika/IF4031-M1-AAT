package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// p1 storage endpoints names
type StorageUnit struct{ Name, DatabaseURL string }

// p1
type Config struct {
	StorageUnits    []StorageUnit
	StoreTimeout    time.Duration
	AuditToken      string
	ImportOnly      bool
	ListenAddress   string
	DatabaseURL     string
	KafkaBrokers    []string
	KafkaTopic      string
	BMKGURL         string
	BMKGAPIKey      string
	PVMBGURL        string
	PVMBGToken      string
	SatelliteURL    string
	SatelliteToken  string
	PollingInterval time.Duration
	HTTPTimeout     time.Duration
	MaxInFlight     int
	StartSince      time.Time
}

// p1
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
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	// p1 three units or explicit single-database compatibility.
	units := make([]StorageUnit, 0, 3)
	for i, key := range []string{"P1_STORAGE_A_URL", "P1_STORAGE_B_URL", "P1_STORAGE_C_URL"} {
		if url := strings.TrimSpace(os.Getenv(key)); url != "" {
			units = append(units, StorageUnit{Name: "store-" + string(rune('a'+i)), DatabaseURL: url})
		}
	}
	if len(units) > 0 && len(units) != 3 {
		return Config{}, fmt.Errorf("all three P1_STORAGE_*_URL values are required")
	}
	if len(units) == 0 {
		if databaseURL == "" {
			return Config{}, fmt.Errorf("DATABASE_URL or three P1 storage URLs are required")
		}
		units = append(units, StorageUnit{Name: "store-a", DatabaseURL: databaseURL})
	}
	timeout, err := durationFromEnv("P1_STORE_TIMEOUT_MS", 500)
	if err != nil {
		return Config{}, err
	}
	importOnly, err := strconv.ParseBool(envOrDefault("P1_IMPORT_ONLY", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("P1_IMPORT_ONLY must be boolean")
	}

	startSince := time.Unix(0, 0).UTC()
	if raw := os.Getenv("AGGREGATOR_START_SINCE"); raw != "" {
		startSince, err = time.Parse(time.RFC3339, raw)
		if err != nil {
			return Config{}, fmt.Errorf("AGGREGATOR_START_SINCE must be RFC3339: %w", err)
		}
	}

	return Config{
		StorageUnits: units, StoreTimeout: timeout, AuditToken: os.Getenv("P1_AUDIT_TOKEN"), ImportOnly: importOnly,
		ListenAddress:   envOrDefault("AGGREGATOR_LISTEN_ADDRESS", ":8083"),
		DatabaseURL:     databaseURL,
		KafkaBrokers:    splitCommaSeparated(os.Getenv("KAFKA_BROKERS"), "localhost:9092"),
		KafkaTopic:      envOrDefault("KAFKA_TOPIC", "hazard.events.v1"),
		BMKGURL:         envOrDefault("BMKG_URL", "http://localhost:8081"),
		BMKGAPIKey:      os.Getenv("BMKG_API_KEY"),
		PVMBGURL:        envOrDefault("PVMBG_URL", "http://localhost:8082"),
		PVMBGToken:      os.Getenv("PVMBG_TOKEN"),
		SatelliteURL:    envOrDefault("SATELLITE_URL", "https://satellite.naufarrel.dev"),
		SatelliteToken:  os.Getenv("SATELLITE_TOKEN"),
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

func splitCommaSeparated(raw, fallback string) []string {
	if raw == "" {
		raw = fallback
	}

	values := make([]string, 0)
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}
