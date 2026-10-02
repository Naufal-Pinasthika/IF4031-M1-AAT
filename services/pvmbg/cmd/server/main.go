package main

import (
	"context"
	"log"
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

func requiredDurationMS(name string) time.Duration {
	raw := os.Getenv(name)

	if raw == "" {
		log.Fatalf("%s is not configured", name)
	}

	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		log.Fatalf("%s must be a positive integer in milliseconds", name)
	}

	return time.Duration(ms) * time.Millisecond
}

func main() {
	//init token
	pvmbgToken := os.Getenv("PVMBG_TOKEN")
	if pvmbgToken == "" {
		log.Fatal("PVMBG_TOKEN is not configured")
	}
	minDelay := requiredDurationMS("PVMBG_DELAY_MIN_MS")
	maxDelay := requiredDurationMS("PVMBG_DELAY_MAX_MS")
	eventInterval := requiredDurationMS("PVMBG_EVENT_INTERVAL_MS")

	if minDelay > maxDelay {
		log.Fatal("PVMBG_DELAY_MIN_MS cannot be greater than PVMBG_DELAY_MAX_MS")
	}
	go data.StartGenerator(context.Background(), eventInterval)

	// init chi router
	r := chi.NewRouter()

	// init logger
	r.Use(middleware.RequestLogger("pvmbg"))

	// init state
	serviceState := state.NewServiceState()

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

	// listen to port 8082
	log.Println("PVMBG server running at :8082")
	err := http.ListenAndServe(":8082", r)

	if err != nil {
		log.Fatal(err)
	}

}
