package main

import "testing"

func TestPriorityLimiterPreservesFieldReserve(t *testing.T) {
	limiter := NewPriorityLimiter(4, 2)
	if !limiter.Acquire(false) || !limiter.Acquire(false) {
		t.Fatal("regular traffic should use shared capacity")
	}
	if limiter.Acquire(false) {
		t.Fatal("regular traffic must not consume field reserve")
	}
	if !limiter.Acquire(true) || !limiter.Acquire(true) {
		t.Fatal("field traffic should use reserved capacity")
	}
	if limiter.Acquire(true) {
		t.Fatal("total capacity must remain bounded")
	}
}

func TestPriorityLimiterReleasesCorrectClass(t *testing.T) {
	limiter := NewPriorityLimiter(2, 1)
	if !limiter.Acquire(false) {
		t.Fatal("regular acquisition failed")
	}
	limiter.Release(false)
	if !limiter.Acquire(false) {
		t.Fatal("released regular slot was not reusable")
	}
}
