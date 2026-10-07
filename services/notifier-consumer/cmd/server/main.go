package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/consumer"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/httpapi"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "notifier-consumer")
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	notificationStore, err := store.Open(cfg.StorePath)
	if err != nil {
		log.Fatal(err)
	}
	defer notificationStore.Close()

	streamConsumer := consumer.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.ConsumerGroup, notificationStore, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go streamConsumer.Run(ctx)

	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           httpapi.New(notificationStore, streamConsumer, logger, cfg.HealthTimeout),
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

	logger.Info("notifier_consumer_started", "address", cfg.ListenAddress, "topic", cfg.KafkaTopic, "consumer_group", cfg.ConsumerGroup)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
