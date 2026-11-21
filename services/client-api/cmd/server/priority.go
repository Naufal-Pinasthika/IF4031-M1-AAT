package main

import "sync"

// PriorityLimiter permanently reserves part of the concurrency budget for
// field clients. Regular traffic cannot consume the reserve, while field
// traffic may use both the shared and reserved capacity.
type PriorityLimiter struct {
	mu         sync.Mutex
	maximum    int
	regularMax int
	total      int
	regular    int
}

func NewPriorityLimiter(maximum, fieldReserve int) *PriorityLimiter {
	return &PriorityLimiter{maximum: maximum, regularMax: maximum - fieldReserve}
}

func (l *PriorityLimiter) Acquire(priority bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.maximum {
		return false
	}
	if !priority && l.regular >= l.regularMax {
		return false
	}
	l.total++
	if !priority {
		l.regular++
	}
	return true
}

func (l *PriorityLimiter) Release(priority bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total > 0 {
		l.total--
	}
	if !priority && l.regular > 0 {
		l.regular--
	}
}
