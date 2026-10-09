package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/data"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/bmkg/internal/middleware"
)

func durationMSFromEnv(name string, fallbackMS int) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return time.Duration(fallbackMS) * time.Millisecond, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer in milliseconds", name)
	}
	return time.Duration(value) * time.Millisecond, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "bmkg")
	eventInterval, err := durationMSFromEnv("BMKG_EVENT_INTERVAL_MS", 10000)
	if err != nil {
		logger.Error("configuration_invalid", "error", err)
		os.Exit(1)
	}
	go data.StartGenerator(context.Background(), eventInterval)

	// init chi router
	r := chi.NewRouter()

	// init logger
	r.Use(middleware.RequestLogger("bmkg"))

	// define endpoints
	r.Get("/health", handler.GetHealth)
	r.Group(func(protected chi.Router) {
		// validate auth key & apply delay
		protected.Use(middleware.BMKGAuth)
		protected.Use(middleware.ApplyDelay)

		protected.Get("/seismic-events", handler.GetSesimicEvents)
		protected.Get("/tsunami-warnings", handler.GetTsunamiWarnings)
	})

	logger.Info("bmkg_server_started", "address", ":8081", "event_interval_ms", eventInterval.Milliseconds())
	if err := http.ListenAndServe(":8081", r); err != nil {
		logger.Error("bmkg_server_stopped", "error", err)
		os.Exit(1)
	}
}
