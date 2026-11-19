package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestShutdownWaitsForRealInflightResponse(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("first-"))
		w.(http.Flusher).Flush()
		close(started)
		<-release
		w.Write([]byte("last"))
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	draining := make(chan struct{})
	done := shutdownHTTP(ctx, server, func() { close(draining) }, time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	<-started
	cancel()
	<-draining
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before response finished: %v", err)
	default:
	}
	close(release)
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "first-last" {
		t.Fatalf("response truncated: %q %v", body, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineClosesBlockedConnections(t *testing.T) {
	started := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("first"))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	go server.Serve(listener)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := shutdownHTTP(ctx, server, func() {}, 20*time.Millisecond)
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err == nil {
		t.Fatal("blocked body not force-closed after deadline")
	}
}
