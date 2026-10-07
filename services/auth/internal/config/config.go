package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type SeedClient struct {
	ID     string
	Secret string
	Scopes []string
}

type Config struct {
	ListenAddress  string
	DatabaseURL    string
	Issuer         string
	Audience       string
	KeyID          string
	PrivateKeyPEM  []byte
	AccessTokenTTL time.Duration
	RefreshTTL     time.Duration
	MaxInFlight    int
	Clients        []SeedClient
}

func Load() (Config, error) {
	accessTTL, err := secondsFromEnv("AUTH_ACCESS_TOKEN_TTL_SECONDS", 60)
	if err != nil {
		return Config{}, err
	}
	refreshTTL, err := secondsFromEnv("AUTH_REFRESH_TOKEN_TTL_SECONDS", 86400)
	if err != nil {
		return Config{}, err
	}
	maxInFlight, err := positiveIntFromEnv("AUTH_MAX_IN_FLIGHT", 32)
	if err != nil {
		return Config{}, err
	}

	privateKey, err := privateKeyFromEnv()
	if err != nil {
		return Config{}, err
	}

	clients := []SeedClient{
		{ID: os.Getenv("MEDIA_CLIENT_ID"), Secret: os.Getenv("MEDIA_CLIENT_SECRET"), Scopes: []string{"hazards:read:summary"}},
		{ID: os.Getenv("FIELD_CLIENT_ID"), Secret: os.Getenv("FIELD_CLIENT_SECRET"), Scopes: []string{"hazards:read:summary", "hazards:read:raw"}},
		{ID: os.Getenv("OPS_CLIENT_ID"), Secret: os.Getenv("OPS_CLIENT_SECRET"), Scopes: []string{"hazards:read:summary", "hazards:read:raw"}},
	}
	if err := validateClients(clients); err != nil {
		return Config{}, err
	}

	cfg := Config{
		ListenAddress:  envOrDefault("AUTH_LISTEN_ADDRESS", ":8084"),
		DatabaseURL:    os.Getenv("AUTH_DATABASE_URL"),
		Issuer:         envOrDefault("AUTH_JWT_ISSUER", "bnpb-auth-service"),
		Audience:       envOrDefault("AUTH_JWT_AUDIENCE", "bnpb-client-api"),
		KeyID:          envOrDefault("AUTH_JWT_KEY_ID", "bnpb-auth-2026-01"),
		PrivateKeyPEM:  privateKey,
		AccessTokenTTL: accessTTL,
		RefreshTTL:     refreshTTL,
		MaxInFlight:    maxInFlight,
		Clients:        clients,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("AUTH_DATABASE_URL is required")
	}
	return cfg, nil
}

func privateKeyFromEnv() ([]byte, error) {
	raw := strings.TrimSpace(os.Getenv("AUTH_JWT_PRIVATE_KEY_B64"))
	if raw == "" {
		return nil, errors.New("AUTH_JWT_PRIVATE_KEY_B64 is required")
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("AUTH_JWT_PRIVATE_KEY_B64 must be standard base64: %w", err)
	}
	return decoded, nil
}

func validateClients(clients []SeedClient) error {
	ids := make(map[string]bool, len(clients))
	secrets := make(map[string]bool, len(clients))
	for _, client := range clients {
		if strings.TrimSpace(client.ID) == "" || client.Secret == "" {
			return errors.New("all downstream client IDs and secrets are required")
		}
		if len(client.Secret) < 24 {
			return fmt.Errorf("secret for downstream client %q must be at least 24 characters", client.ID)
		}
		if ids[client.ID] {
			return fmt.Errorf("downstream client ID %q is duplicated", client.ID)
		}
		if secrets[client.Secret] {
			return errors.New("downstream client secrets must be different")
		}
		ids[client.ID] = true
		secrets[client.Secret] = true
	}
	return nil
}

func secondsFromEnv(name string, fallback int) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return time.Duration(fallback) * time.Second, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return time.Duration(value) * time.Second, nil
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
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
