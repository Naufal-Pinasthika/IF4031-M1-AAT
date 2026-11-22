package main

import (
	"context"
	"net/http"
	"time"
)

func shutdownHTTP(ctx context.Context, server *http.Server, timeout time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() {
		<-ctx.Done()
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
