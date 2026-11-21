package aggregator

import (
	"sync"
	"time"
)

type CircuitBreaker struct {
	mu               sync.Mutex
	failureThreshold int
	openDuration     time.Duration
	failures         int
	state            string
	openUntil        time.Time
	halfOpenProbe    bool
}

func NewCircuitBreaker(failureThreshold int, openDuration time.Duration) *CircuitBreaker {
	return &CircuitBreaker{failureThreshold: failureThreshold, openDuration: openDuration, state: "closed"}
}

func (c *CircuitBreaker) Allow(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case "open":
		if now.Before(c.openUntil) {
			return false
		}
		c.state = "half_open"
		c.halfOpenProbe = true
		return true
	case "half_open":
		if c.halfOpenProbe {
			return false
		}
		c.halfOpenProbe = true
		return true
	default:
		return true
	}
}

func (c *CircuitBreaker) Success() {
	c.mu.Lock()
	c.failures = 0
	c.state = "closed"
	c.openUntil = time.Time{}
	c.halfOpenProbe = false
	c.mu.Unlock()
}

func (c *CircuitBreaker) Failure(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.halfOpenProbe = false
	if c.state == "half_open" {
		c.state = "open"
		c.openUntil = now.Add(c.openDuration)
		return
	}
	c.failures++
	if c.failures >= c.failureThreshold {
		c.state = "open"
		c.openUntil = now.Add(c.openDuration)
	}
}

func (c *CircuitBreaker) State() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// An internal coordination failure is not evidence of a source failure. A
// half-open permit must still be released so a later probe can run.
func (c *CircuitBreaker) Abandon(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == "half_open" {
		c.state = "open"
		c.openUntil = now.Add(c.openDuration)
		c.halfOpenProbe = false
	}
}
