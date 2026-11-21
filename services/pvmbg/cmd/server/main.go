package main

import (
	"context"
	"fmt"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/telemetry"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/data"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/middleware"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

func requiredDurationMS(name string) (time.Duration, error) {
	raw := os.Getenv(name)

	if raw == "" {
		return 0, fmt.Errorf("%s is not configured", name)
	}

	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer in milliseconds", name)
	}

	return time.Duration(ms) * time.Millisecond, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "pvmbg")
	slog.SetDefault(logger)
	pvmbgToken := os.Getenv("PVMBG_TOKEN")
	if pvmbgToken == "" {
		logger.Error("configuration_invalid", "error", "PVMBG_TOKEN is not configured")
		os.Exit(1)
	}
	minDelay, err := requiredDurationMS("PVMBG_DELAY_MIN_MS")
	if err != nil {
		logger.Error("configuration_invalid", "error", err)
		os.Exit(1)
	}
	maxDelay, err := requiredDurationMS("PVMBG_DELAY_MAX_MS")
	if err != nil {
		logger.Error("configuration_invalid", "error", err)
		os.Exit(1)
	}
	eventInterval, err := requiredDurationMS("PVMBG_EVENT_INTERVAL_MS")
	if err != nil {
		logger.Error("configuration_invalid", "error", err)
		os.Exit(1)
	}

	if minDelay > maxDelay {
		logger.Error("configuration_invalid", "error", "PVMBG_DELAY_MIN_MS cannot be greater than PVMBG_DELAY_MAX_MS")
		os.Exit(1)
	}
	go data.StartGenerator(context.Background(), eventInterval)

	// init chi router
	r := chi.NewRouter()

	// init logger
	r.Use(middleware.RequestLogger("pvmbg"))

	// init state
	serviceState := state.NewServiceState()
	if address := os.Getenv("PVMBG_ADMIN_LISTEN_ADDRESS"); address != "" {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			logger.Error("admin_listener_failed", "error", err)
			os.Exit(1)
		}
		admin := &http.Server{Handler: telemetry.Wrap(newAdminRouter(serviceState), false),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
		go func() {
			logger.Info("admin_listener_started", "address", address)
			if err := admin.Serve(listener); err != nil && err != http.ErrServerClosed {
				logger.Error("admin_listener_stopped", "error", err)
			}
		}()
	}

	volcanicHandler := &handler.VolcanicHandler{State: serviceState}
	adminHandler := &handler.AdminHandler{State: serviceState}

	// define endpoints
	r.Get("/health", handler.GetHealth)

	r.Route("/", func(r chi.Router) {
		r.Use(middleware.PVMBGAuth(pvmbgToken))
		// admin enddpoint
		r.Post("/admin/schema-version", adminHandler.ToggleSchemaVersion)
		r.Post("/admin/outage", adminHandler.ToggleOutage)

		r.With(middleware.SimulatedDelay(serviceState, minDelay, maxDelay)).
			Get("/volcanic-reports", volcanicHandler.GetReports)
	})

	logger.Info("pvmbg_server_started", "address", ":8082", "delay_min_ms", minDelay.Milliseconds(), "delay_max_ms", maxDelay.Milliseconds(), "event_interval_ms", eventInterval.Milliseconds())
	if err := http.ListenAndServe(":8082", telemetry.Wrap(r, false)); err != nil {
		logger.Error("pvmbg_server_stopped", "error", err)
		os.Exit(1)
	}
}
