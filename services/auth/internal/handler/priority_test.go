package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFieldWaitIsBoundedAndCancellationReleasesWaiter(t *testing.T) {
	active, waiting := make(chan struct{}, 1), make(chan struct{}, 1)
	active <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- acquireField(ctx, active, waiting) }()
	deadline := time.Now().Add(time.Second)
	for len(waiting) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("Field did not enter bounded waiting room")
		}
		time.Sleep(time.Millisecond)
	}
	if acquireField(t.Context(), active, waiting) {
		t.Fatal("full waiting room admitted another caller")
	}
	cancel()
	select {
	case admitted := <-done:
		if admitted || len(waiting) != 0 || len(active) != 1 {
			t.Fatal("canceled waiter entered handler or leaked admission capacity")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not end Field wait")
	}
	<-active
	if acquireField(ctx, active, waiting) || len(active) != 0 {
		t.Fatal("already canceled request consumed available capacity")
	}
}

func TestFieldAuthenticationCannotBeStarvedOrSpoofed(t *testing.T) {
	entered, release := make(chan struct{}, 3), make(chan struct{})
	h := LimitField(3, 1, "field", "test-only-field-secret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(200)
	}))
	defer close(release)
	request := func(id, secret, path string) *http.Request {
		r := httptest.NewRequest("POST", path, nil)
		r.SetBasicAuth(id, secret)
		r.Header.Set("X-Request-Priority", "field")
		return r
	}
	for i := 0; i < 2; i++ {
		go h.ServeHTTP(httptest.NewRecorder(), request("media", "media-secret", "/v1/token"))
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("regular slot unavailable")
		}
	}
	for _, r := range []*http.Request{request("field", "wrong", "/v1/token"), request("media", "media-secret", "/v1/token"), request("field", "test-only-field-secret", "/health")} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 429 {
			t.Fatalf("untrusted classification consumed reserve: %d", w.Code)
		}
	}
	go h.ServeHTTP(httptest.NewRecorder(), request("field", "test-only-field-secret", "/v1/token/refresh"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("valid Field refresh was starved")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request("field", "test-only-field-secret", "/v1/token"))
	if w.Code != 429 {
		t.Fatal("global concurrency bound exceeded")
	}
}

func TestFieldBurstWaitsForActiveAuthenticationToFinish(t *testing.T) {
	entered, release := make(chan struct{}, 8), make(chan struct{})
	h := LimitField(4, 1, "field", "field-secret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(200)
	}))
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	call := func(id string, done chan int) {
		r := httptest.NewRequest("POST", "/v1/token", nil)
		r.SetBasicAuth(id, id+"-secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		done <- w.Code
	}
	done := make(chan int, 7)
	for i := 0; i < 4; i++ {
		id := "media"
		if i == 3 {
			id = "field"
		}
		go call(id, done)
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("initial active slot not acquired")
		}
	}
	for i := 0; i < 3; i++ {
		go call("field", done)
	}
	select {
	case status := <-done:
		t.Fatalf("short Field burst was dropped before active work could finish: %d", status)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	released = true
	for i := 0; i < 7; i++ {
		select {
		case status := <-done:
			if status != 200 {
				t.Fatalf("queued Field authentication failed: %d", status)
			}
		case <-time.After(time.Second):
			t.Fatal("bounded Field burst did not drain")
		}
	}
}
