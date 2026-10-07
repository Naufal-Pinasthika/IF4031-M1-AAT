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

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/service"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/store"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/token"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authStore, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer authStore.Close()
	if err := authStore.SeedClients(ctx, cfg.Clients); err != nil {
		log.Fatal(err)
	}

	signer, err := token.NewSigner(cfg.PrivateKeyPEM, cfg.Issuer, cfg.Audience, cfg.KeyID, cfg.AccessTokenTTL)
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
}
