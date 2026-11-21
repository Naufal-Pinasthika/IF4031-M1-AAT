package main

import (
	"context"
	"fmt"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/telemetry"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/aggregator"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/broker"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/handler"
	metric "github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/metrics"
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
	coordinator, err := store.NewCoordinator(startupCtx, cfg.CoordinationURL, cfg.CoordinationTimeout, logger, cfg.StorageUnits...)
	if err != nil {
		logger.Error("coordination_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer coordinator.Close()

	publisher, err := broker.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		logger.Error("publisher_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()
	tracker := status.NewTracker()
	tracker.EnableSharedHealth(3*time.Second + cfg.CoordinationTimeout)
	service := aggregator.NewAggregator(poller, satellite, eventStore, coordinator, publisher, cfg, logger, tracker)
	fillReader := handler.NewLimitedReader(eventStore, cfg.MaxInFlight, cfg.FieldReservedSlots)
	readSide := handler.NewCachedReader(fillReader, cfg.ReadCacheTTL, 2*cfg.StoreTimeout+100*time.Millisecond, logger)
	var projection *store.ReadProjection
	var projectionErr error
	if cfg.SharedReadProjection {
		projection, projectionErr = store.NewReadProjection(startupCtx, cfg.CoordinationURL)
	}
	if projectionErr != nil {
		logger.Warn("read_projection_unavailable", "error", projectionErr)
	} else if projection != nil {
		defer projection.Close()
		readSide.SetProjection(projection)
	}
	apiHandler := handler.New(readSide, tracker)
	// p1
	p1Handler := handler.NewP1(eventStore, coordinator, cfg.AuditToken)
	limiter := handler.NewPriorityLimiter(cfg.ResponseSlots, cfg.ResponseSlots/4)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := startProfiling(ctx, logger); err != nil {
		logger.Error("profiling_initialization_failed", "error", err)
		os.Exit(1)
	}
	// p1: recovery continues even when live polling is disabled.
	var workers sync.WaitGroup
	workers.Go(func() { tracker.RunSharedHealth(ctx, coordinator, time.Second) })
	workers.Go(func() { eventStore.RunRecovery(ctx, coordinator, cfg.InstanceID, cfg.LeaseTTL) })
	workers.Go(func() { service.RunDeliveries(ctx) })
	workers.Go(func() { service.RunInbox(ctx) })
	if !cfg.ImportOnly {
		workers.Go(func() { service.Run(ctx) })
	}

	// setup endpoints
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", handler.GetHealth)
	mux.HandleFunc("GET /version", versionInfo(buildVersion))
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		readyCtx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := eventStore.Ping(readyCtx); err != nil {
			http.Error(w, `{"status":"not_ready","dependency":"canonical_store"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		metric.Default.WritePrometheus(w)
		handler.WriteReadMetrics(w)
		if projection != nil {
			projection.WriteMetrics(w)
		}
		metricCtx, cancel := context.WithTimeout(context.Background(), cfg.CoordinationTimeout)
		defer cancel()
		if stream, err := coordinator.SatelliteStats(metricCtx); err == nil {
			for _, key := range []string{"committed_offset", "lag_offsets", "late_detections", "singleton_detections", "fires", "pending_projections", "p95_ingress_to_canonical_ms", "latency_samples"} {
				if stream[key] != nil {
					_, _ = fmt.Fprintf(w, "bnpb_satellite_%s %v\n", key, stream[key])
				}
			}
		}
	})
	mux.HandleFunc("GET /internal/events", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetEvents)))
	mux.HandleFunc("GET /internal/events/{id}", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetEventByID)))
	mux.HandleFunc("GET /internal/source-status", handler.AggregatorAuth(limiter.Wrap(apiHandler.GetSourceStatus)))
	mux.HandleFunc("GET /internal/polling", handler.AggregatorAuth(limiter.Wrap(p1Handler.GetPolling)))
	mux.HandleFunc("GET /internal/unprocessed", handler.AggregatorAuth(limiter.Wrap(p1Handler.GetAuditUnprocessed)))
	mux.HandleFunc("POST /internal/field-reports", handler.AggregatorAuth(p1Handler.PostFieldReport))
	mux.HandleFunc("GET /internal/field-reports/{id}", handler.AggregatorAuth(p1Handler.GetFieldReport))
	mux.HandleFunc("GET /internal/unprocessed/{id}", handler.AggregatorAuth(handler.OperationsOnly(p1Handler.GetInboxDetails)))
	mux.HandleFunc("POST /internal/unprocessed/replay", handler.AggregatorAuth(handler.OperationsOnly(p1Handler.ReplayInbox)))
	mux.HandleFunc("GET /internal/satellite-status", handler.AggregatorAuth(handler.OperationsOnly(p1Handler.GetSatelliteStats)))

	// p1: audit has its own bearer credential and structured error contract.
	mux.HandleFunc("GET /internal/audit/events", p1Handler.AuditAuth(p1Handler.GetAuditEvents))
	mux.HandleFunc("GET /internal/audit/checksum", p1Handler.AuditAuth(p1Handler.GetAuditChecksum))
	mux.HandleFunc("GET /internal/audit/location", p1Handler.AuditAuth(p1Handler.GetAuditLocation))
	mux.HandleFunc("GET /internal/audit/unprocessed", p1Handler.AuditAuth(p1Handler.GetAuditUnprocessed))
	mux.HandleFunc("POST /internal/import/events", handler.AggregatorAuth(p1Handler.ImportEvents))
	mux.HandleFunc("GET /internal/storage-stats", handler.AggregatorAuth(p1Handler.GetStorageStats))
	mux.HandleFunc("GET /internal/analytics", handler.AggregatorAuth(p1Handler.GetAnalytics))
	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           withBuildVersion(telemetry.Wrap(handler.Logging(logger, mux), true), buildVersion),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	shutdownDone := shutdownHTTP(ctx, server, 10*time.Second)

	logger.Info("aggregator_server_started", "address", cfg.ListenAddress, "build_version", buildVersion)
	serveErr := server.ListenAndServe()
	stop()
	if err := <-shutdownDone; err != nil {
		logger.Warn("http_drain_incomplete", "error", err)
	}
	workers.Wait()
	if serveErr != nil && serveErr != http.ErrServerClosed {
		logger.Error("aggregator_server_stopped", "error", serveErr)
		os.Exit(1)
	}
}
