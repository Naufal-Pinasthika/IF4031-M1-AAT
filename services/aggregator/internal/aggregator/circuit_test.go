package aggregator

import (
	"testing"
	"time"
)

func TestCircuitBreakerLifecycle(t *testing.T) {
	now := time.Unix(100, 0)
	circuit := NewCircuitBreaker(2, 5*time.Second)
	if !circuit.Allow(now) {
		t.Fatal("closed circuit must allow requests")
	}
	circuit.Failure(now)
	if !circuit.Allow(now) {
		t.Fatal("failure threshold has not been reached")
	}
	circuit.Failure(now)
	if circuit.Allow(now.Add(time.Second)) {
		t.Fatal("open circuit must reject before cooldown")
	}
	if !circuit.Allow(now.Add(5 * time.Second)) {
		t.Fatal("one half-open probe must be allowed")
	}
	if circuit.Allow(now.Add(5 * time.Second)) {
		t.Fatal("parallel half-open probes must be rejected")
	}
	circuit.Success()
	if circuit.State() != "closed" || !circuit.Allow(now) {
		t.Fatal("successful probe must close circuit")
	}
}

func TestCircuitBreakerFailedProbeReopens(t *testing.T) {
	now := time.Unix(200, 0)
	circuit := NewCircuitBreaker(1, time.Second)
	circuit.Failure(now)
	if !circuit.Allow(now.Add(time.Second)) {
		t.Fatal("probe should be allowed after cooldown")
	}
	circuit.Failure(now.Add(time.Second))
	if circuit.Allow(now.Add(1500 * time.Millisecond)) {
		t.Fatal("failed probe must reopen circuit")
	}
}
