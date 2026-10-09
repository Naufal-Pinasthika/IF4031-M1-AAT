package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/aggregator"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/broker"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/status"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "aggregator")
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration_invalid", "error", err)
		os.Exit(1)
	}

	// poller/aggregator init
	client := aggregator.NewClient(cfg.HTTPTimeout, logger)
	poller := aggregator.New(client, cfg, logger)
	startupCtx := correlation.WithID(context.Background(), "startup")
	eventStore, err := store.New(startupCtx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("store_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	publisher, err := broker.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		logger.Error("publisher_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()
	tracker := status.NewTracker()
	service := aggregator.NewAggregator(poller, eventStore, publisher, cfg, logger, tracker)
	apiHandler := handler.New(eventStore, tracker)
	limiter := handler.NewLimiter(cfg.MaxInFlight)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go service.Run(ctx)

	// setup endpoints
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", handler.GetHealth)
	mux.HandleFunc("GET /internal/events", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetEvents)))
	mux.HandleFunc("GET /internal/events/{id}", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetEventByID)))
	mux.HandleFunc("GET /internal/source-status", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetSourceStatus)))

	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           handler.Logging(logger, mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("aggregator_server_started", "address", cfg.ListenAddress)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("aggregator_server_stopped", "error", err)
		os.Exit(1)
	}
}
