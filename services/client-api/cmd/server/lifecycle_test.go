package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestShutdownWaitsForActiveClientRequest(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = io.WriteString(w, "complete")
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	drained := shutdownHTTP(ctx, server.Config, time.Second)
	response := make(chan error, 1)
	go func() {
		r, err := server.Client().Get(server.URL)
		if err == nil {
			defer r.Body.Close()
			var body []byte
			body, err = io.ReadAll(r.Body)
			if err == nil && string(body) != "complete" {
				err = io.ErrUnexpectedEOF
			}
		}
		response <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-drained:
		close(release)
		t.Fatalf("shutdown returned before the active request completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
}
