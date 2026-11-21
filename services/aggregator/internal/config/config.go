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
	StorageUnits         []StorageUnit
	StoreTimeout         time.Duration
	AuditToken           string
	ImportOnly           bool
	ListenAddress        string
	DatabaseURL          string
	KafkaBrokers         []string
	KafkaTopic           string
	KafkaCriticalTopic   string
	BMKGURL              string
	BMKGAPIKey           string
	PVMBGURL             string
	PVMBGToken           string
	PVMBGAdapterVersion  int
	SatelliteURL         string
	SatelliteToken       string
	CoordinationURL      string
	CoordinationTimeout  time.Duration
	InstanceID           string
	LeaseTTL             time.Duration
	CoastalURL           string
	CoastalToken         string
	PollingInterval      time.Duration
	CoastalInterval      time.Duration
	HTTPTimeout          time.Duration
	CoastalTimeout       time.Duration
	CoastalLeaseTTL      time.Duration
	CoastalMaxStaleness  time.Duration
	CircuitFailures      int
	CircuitOpenDuration  time.Duration
	MaxInFlight          int
	ResponseSlots        int
	SharedReadProjection bool
	FieldReservedSlots   int
	ReadCacheTTL         time.Duration
	StartSince           time.Time
}

// p1
func Load() (Config, error) {
	adapterVersion, err := positiveIntFromEnv("PVMBG_ADAPTER_VERSION", 1)
	if err != nil || adapterVersion > 2 {
		return Config{}, fmt.Errorf("PVMBG_ADAPTER_VERSION must be 1 or 2")
	}
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
	responseSlots, err := positiveIntFromEnv("AGGREGATOR_RESPONSE_SLOTS", 128)
	if err != nil || responseSlots > 256 {
		return Config{}, fmt.Errorf("AGGREGATOR_RESPONSE_SLOTS must be between 1 and 256")
	}
	sharedProjection, err := strconv.ParseBool(envOrDefault("AGGREGATOR_SHARED_READ_PROJECTION", "true"))
	if err != nil {
		return Config{}, fmt.Errorf("AGGREGATOR_SHARED_READ_PROJECTION must be boolean")
	}
	fieldReserve, err := strconv.Atoi(envOrDefault("AGGREGATOR_FIELD_RESERVED_SLOTS", strconv.Itoa(maxInFlight/4)))
	if err != nil || fieldReserve < 0 || fieldReserve >= maxInFlight {
		return Config{}, fmt.Errorf("AGGREGATOR_FIELD_RESERVED_SLOTS must be nonnegative and below AGGREGATOR_MAX_IN_FLIGHT")
	}
	readCacheMS, err := strconv.Atoi(envOrDefault("AGGREGATOR_READ_CACHE_TTL_MS", "100"))
	if err != nil || readCacheMS < 0 || readCacheMS > 1000 {
		return Config{}, fmt.Errorf("AGGREGATOR_READ_CACHE_TTL_MS must be between 0 and 1000")
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
	coordinationTimeout, err := durationFromEnv("COORDINATION_TIMEOUT_MS", 2000)
	if err != nil {
		return Config{}, err
	}
	leaseTTL, err := durationFromEnv("AGGREGATOR_LEASE_TTL_MS", 15000)
	if err != nil {
		return Config{}, err
	}
	if leaseTTL <= httpTimeout*2 {
		return Config{}, fmt.Errorf("AGGREGATOR_LEASE_TTL_MS must exceed twice AGGREGATOR_HTTP_TIMEOUT_MS")
	}
	if coordinationTimeout >= leaseTTL/3 {
		return Config{}, fmt.Errorf("COORDINATION_TIMEOUT_MS must be below one third of AGGREGATOR_LEASE_TTL_MS")
	}
	coastalInterval, err := durationFromEnv("COASTAL_POLL_INTERVAL_MS", 500)
	if err != nil {
		return Config{}, err
	}
	coastalTimeout, err := durationFromEnv("COASTAL_HTTP_TIMEOUT_MS", 1000)
	if err != nil {
		return Config{}, err
	}
	coastalMaxStaleness, err := durationFromEnv("COASTAL_MAX_STALENESS_MS", 30000)
	if err != nil {
		return Config{}, err
	}
	coastalLeaseTTL, err := durationFromEnv("COASTAL_LEASE_TTL_MS", 4000)
	if err != nil {
		return Config{}, err
	}
	if coastalLeaseTTL <= coastalTimeout+coordinationTimeout || coordinationTimeout*3 >= coastalLeaseTTL*2 {
		return Config{}, fmt.Errorf("COASTAL_LEASE_TTL_MS must cover coastal HTTP and coordination deadlines with renewal headroom")
	}
	circuitFailures, err := positiveIntFromEnv("COASTAL_CIRCUIT_FAILURES", 3)
	if err != nil {
		return Config{}, err
	}
	circuitOpenDuration, err := durationFromEnv("COASTAL_CIRCUIT_OPEN_MS", 2000)
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
	instanceID := strings.TrimSpace(os.Getenv("AGGREGATOR_INSTANCE_ID"))
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}
	if instanceID == "" {
		return Config{}, fmt.Errorf("AGGREGATOR_INSTANCE_ID or hostname is required")
	}
	coordinationURL := strings.TrimSpace(os.Getenv("COORDINATION_DATABASE_URL"))
	if coordinationURL == "" {
		coordinationURL = databaseURL
	}
	if coordinationURL == "" {
		return Config{}, fmt.Errorf("COORDINATION_DATABASE_URL is required when DATABASE_URL is empty")
	}

	return Config{
		StorageUnits:         units,
		StoreTimeout:         timeout,
		CoordinationURL:      coordinationURL,
		CoordinationTimeout:  coordinationTimeout,
		AuditToken:           os.Getenv("P1_AUDIT_TOKEN"),
		ImportOnly:           importOnly,
		InstanceID:           instanceID,
		LeaseTTL:             leaseTTL,
		ListenAddress:        envOrDefault("AGGREGATOR_LISTEN_ADDRESS", ":8083"),
		DatabaseURL:          databaseURL,
		KafkaBrokers:         splitCommaSeparated(os.Getenv("KAFKA_BROKERS"), "localhost:9092"),
		KafkaTopic:           envOrDefault("KAFKA_TOPIC", "hazard.events.v1"),
		KafkaCriticalTopic:   strings.TrimSpace(os.Getenv("KAFKA_CRITICAL_TOPIC")),
		BMKGURL:              envOrDefault("BMKG_URL", "http://localhost:8081"),
		BMKGAPIKey:           os.Getenv("BMKG_API_KEY"),
		PVMBGURL:             envOrDefault("PVMBG_URL", "http://localhost:8082"),
		PVMBGToken:           os.Getenv("PVMBG_TOKEN"),
		PVMBGAdapterVersion:  adapterVersion,
		SatelliteURL:         envOrDefault("SATELLITE_URL", "https://satellite.naufarrel.dev"),
		SatelliteToken:       os.Getenv("SATELLITE_TOKEN"),
		CoastalURL:           envOrDefault("COASTAL_URL", "http://sensor-pantai:8080"),
		CoastalToken:         os.Getenv("PANTAI_AUTH_TOKEN"),
		PollingInterval:      pollingInterval,
		CoastalInterval:      coastalInterval,
		HTTPTimeout:          httpTimeout,
		CoastalTimeout:       coastalTimeout,
		CoastalLeaseTTL:      coastalLeaseTTL,
		CoastalMaxStaleness:  coastalMaxStaleness,
		CircuitFailures:      circuitFailures,
		CircuitOpenDuration:  circuitOpenDuration,
		MaxInFlight:          maxInFlight,
		ResponseSlots:        responseSlots,
		SharedReadProjection: sharedProjection,
		FieldReservedSlots:   fieldReserve,
		ReadCacheTTL:         time.Duration(readCacheMS) * time.Millisecond,
		StartSince:           startSince,
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
