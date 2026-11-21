package config

import (
	"strings"
	"testing"
)

func TestFieldReserveConfigurationIsBounded(t *testing.T) {
	t.Setenv("AUTH_MAX_IN_FLIGHT", "32")
	for _, raw := range []string{"-1", "32", "33", "invalid"} {
		t.Setenv("AUTH_FIELD_RESERVED_SLOTS", raw)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "AUTH_FIELD_RESERVED_SLOTS") {
			t.Fatalf("invalid reserve %q did not fail early: %v", raw, err)
		}
	}
}
