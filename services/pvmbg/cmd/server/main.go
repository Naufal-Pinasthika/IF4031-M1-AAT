package main

import (
	"go/token"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/middleware"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

func main()  {
	// init chi router
	r := chi.NewRouter()

	serviceState := state.NewServiceState()

	volcanicHandler := &handler.VolcanicHandler{State: serviceState}
	adminHandler := &handler.AdminHandler{State: serviceState}

	// define endpoints
	r.Get("/health", handler.GetHealth)

	r.Route("/", func(r chi.Router) {
		r.Use(middleware.PVMBGAuth(token))
		// admin enddpoint
		r.Post("/admin/schema-version", adminHandler.ToggleSchemaVersion)
		r.Post("/admin/outage", adminHandler.ToggleOutage)

		r.With(middleware.SimulatedDelay(500*time.Millisecond, 3*time.Second)).
		Get("/volcanic-reports", volcanicHandler.GetReports)
	})


	// listen to port 8082
	log.Println("PVMBG server running at :8082")
	err := http.ListenAndServe(":8081", r)

	if err != nil {
		log.Fatal(err)
	}


}