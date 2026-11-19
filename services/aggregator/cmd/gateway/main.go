package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/gateway"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/runtimeprofile"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/telemetry"
)

var buildVersion = "development"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "aggregator-gateway")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	proxy := gateway.New(logger, os.Getenv("AGGREGATOR_API_KEY"))
	responseSlots, replicas := 128, 2
	if raw := os.Getenv("AGGREGATOR_RESPONSE_SLOTS"); raw != "" {
		responseSlots, _ = strconv.Atoi(raw)
	}
	if raw := os.Getenv("GATEWAY_REPLICAS"); raw != "" {
		replicas, _ = strconv.Atoi(raw)
	}
	if err := proxy.ConfigureBackendBudget(responseSlots, replicas); err != nil {
		logger.Error("gateway_budget_invalid", "error", err)
		os.Exit(1)
	}
	name := os.Getenv("AGGREGATOR_BACKEND_DNS")
	if name == "" {
		name = "aggregator-backends"
	}
	go proxy.Discover(ctx, name)
	if err := runtimeprofile.Start(ctx, logger, os.Getenv("GATEWAY_PROFILE_ENABLED") == "1", buildVersion); err != nil {
		logger.Error("gateway_profile_failed", "error", err)
		os.Exit(1)
	}
	server := &http.Server{Addr: ":8083", Handler: telemetry.Wrap(proxy, true), ConnState: proxy.ConnectionState, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	done := shutdownHTTP(ctx, server, proxy.BeginDrain, 15*time.Second)
	serveErr := server.ListenAndServe()
	if err := serveErr; err != nil && err != http.ErrServerClosed {
		logger.Error("gateway_failed", "error", err)
	}
	stop()
	if err := <-done; err != nil {
		logger.Error("gateway_drain_failed", "error", err)
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		os.Exit(1)
	}
}
