package main

import (
	"context"
	"net/http"
	"time"
)

// Serve returns when Shutdown closes the listener, BEFORE active responses
// finish. Main must await this channel before letting the process exit.
func shutdownHTTP(ctx context.Context, server *http.Server, draining func(), timeout time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() {
		<-ctx.Done()
		draining()
		drain, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		err := server.Shutdown(drain)
		if err != nil {
			_ = server.Close()
		}
		done <- err
	}()
	return done
}
