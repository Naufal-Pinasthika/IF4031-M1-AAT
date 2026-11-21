package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGracefulShutdownWaitsForInflightResponse(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte("finished"))
	}))
	defer server.Close()
	responseDone := make(chan error, 1)
	go func() {
		response, err := server.Client().Get(server.URL)
		if err == nil {
			defer response.Body.Close()
			var body []byte
			body, err = io.ReadAll(response.Body)
			if string(body) != "finished" && err == nil {
				err = io.ErrUnexpectedEOF
			}
		}
		responseDone <- err
	}()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	done := shutdownHTTP(ctx, server.Config, time.Second)
	cancel()
	select {
	case err := <-done:
		close(release)
		t.Fatalf("drain returned before inflight completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-responseDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
