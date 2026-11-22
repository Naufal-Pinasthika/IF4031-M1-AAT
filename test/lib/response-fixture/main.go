package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	var cfg struct {
		Address string
		Key     string
		Body    []byte
	}
	if err := json.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		panic("invalid fixture configuration")
	}
	if len(cfg.Body) == 0 || len(cfg.Body) > 1<<20 || cfg.Key == "" || !json.Valid(cfg.Body) {
		panic("fixture requires bounded JSON and authentication")
	}
	listener, err := net.Listen("tcp4", cfg.Address+":0")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/events", func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Aggregator-Key")), []byte(cfg.Key)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(cfg.Body)))
		w.Header().Set("X-Correlation-ID", r.Header.Get("X-Correlation-ID"))
		_, _ = w.Write(cfg.Body)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second}
	go func() {
		time.Sleep(90 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"address": listener.Addr().String(), "pid": os.Getpid()})
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
