package main

import (
	"net/http"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/handler"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/middleware"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
	"github.com/go-chi/chi/v5"
)

// Opt-in control listener. Kubernetes permits this port only from Tester Agent;
// the normal data/admin listener keeps its independent Bearer authentication.
func newAdminRouter(serviceState *state.ServiceState) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestLogger("pvmbg-admin"))
	h := &handler.AdminHandler{State: serviceState}
	r.Get("/health", handler.GetHealth)
	r.Post("/admin/schema-version", h.ToggleSchemaVersion)
	r.Post("/admin/outage", h.ToggleOutage)
	return r
}
