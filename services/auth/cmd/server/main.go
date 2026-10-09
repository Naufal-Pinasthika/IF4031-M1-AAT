package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/correlation"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/service"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/store"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/token"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "auth")
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration_invalid", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx := correlation.WithID(ctx, "startup")
	authStore, err := store.New(startupCtx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("store_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer authStore.Close()
	if err := authStore.SeedClients(startupCtx, cfg.Clients); err != nil {
		logger.Error("client_seed_failed", "error", err)
		os.Exit(1)
	}

	signer, err := token.NewSigner(cfg.PrivateKeyPEM, cfg.Issuer, cfg.Audience, cfg.KeyID, cfg.AccessTokenTTL)
	if err != nil {
		logger.Error("token_signer_initialization_failed", "error", err)
		os.Exit(1)
	}
	authService := service.New(authStore, signer, cfg.RefreshTTL)
	authHandler := handler.New(authService, signer, authStore, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/token", authHandler.Token)
	mux.HandleFunc("POST /v1/token/refresh", authHandler.Refresh)
	mux.HandleFunc("GET /.well-known/jwks.json", authHandler.JWKS)
	mux.HandleFunc("GET /health", authHandler.Health)

	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           handler.Logging(logger, handler.Limit(cfg.MaxInFlight, mux)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("auth_server_started", "address", cfg.ListenAddress)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("auth_server_stopped", "error", err)
		os.Exit(1)
	}
}
