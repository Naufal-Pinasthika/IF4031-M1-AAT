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
	satellite := aggregator.NewSatelliteConsumer(cfg.SatelliteURL, cfg.SatelliteToken, logger)
	startupCtx := correlation.WithID(context.Background(), "startup")
	// p1: routing and bounded I/O across storage units.
	eventStore, err := store.New(startupCtx, cfg.StorageUnits, cfg.StoreTimeout, logger)
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
	service := aggregator.NewAggregator(poller, satellite, eventStore, publisher, cfg, logger, tracker)
	apiHandler := handler.New(eventStore, tracker)
	// p1
	p1Handler := handler.NewP1(eventStore, cfg.AuditToken)
	limiter := handler.NewLimiter(cfg.MaxInFlight)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// p1: recovery continues even when live polling is disabled.
	go eventStore.RunRecovery(ctx)
	if !cfg.ImportOnly {
		go service.Run(ctx)
	}

	// setup endpoints
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", handler.GetHealth)
	mux.HandleFunc("GET /internal/events", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetEvents)))
	mux.HandleFunc("GET /internal/events/{id}", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetEventByID)))
	mux.HandleFunc("GET /internal/source-status", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetSourceStatus)))

	// p1: audit has its own bearer credential and structured error contract.
	mux.HandleFunc("GET /internal/audit/events", p1Handler.AuditAuth(p1Handler.GetAuditEvents))
	mux.HandleFunc("GET /internal/audit/checksum", p1Handler.AuditAuth(p1Handler.GetAuditChecksum))
	mux.HandleFunc("GET /internal/audit/location", p1Handler.AuditAuth(p1Handler.GetAuditLocation))
	mux.HandleFunc("POST /internal/import/events", handler.AggregatorAuth(p1Handler.ImportEvents))
	mux.HandleFunc("GET /internal/storage-stats", handler.AggregatorAuth(p1Handler.GetStorageStats))
	mux.HandleFunc("GET /internal/analytics", handler.AggregatorAuth(p1Handler.GetAnalytics))
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
