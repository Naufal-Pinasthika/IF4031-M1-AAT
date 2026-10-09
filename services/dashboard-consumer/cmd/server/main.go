package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/dashboard-consumer/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/dashboard-consumer/internal/consumer"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/dashboard-consumer/internal/httpapi"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/dashboard-consumer/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "dashboard-consumer")
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration_invalid", "error", err)
		os.Exit(1)
	}
	eventStore, err := store.Open(cfg.StorePath)
	if err != nil {
		logger.Error("store_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer eventStore.Close()

	streamConsumer := consumer.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.ConsumerGroup, eventStore, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go streamConsumer.Run(ctx)

	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           httpapi.New(eventStore, streamConsumer, logger, cfg.HealthTimeout),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		_ = streamConsumer.Close()
	}()

	logger.Info("dashboard_consumer_started", "address", cfg.ListenAddress, "topic", cfg.KafkaTopic, "consumer_group", cfg.ConsumerGroup)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("dashboard_consumer_server_stopped", "error", err)
		os.Exit(1)
	}
}
