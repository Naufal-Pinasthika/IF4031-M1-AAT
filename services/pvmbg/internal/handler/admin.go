package handler

import (
	"encoding/json"
	"net/http"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

type AdminHandler struct {
	State *state.ServiceState
}

func (h *AdminHandler) ToggleSchemaVersion(
	w http.ResponseWriter,
	r *http.Request,
) {
	enabled := h.State.ToggleSchemaVersion()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"schema_v2_enabled": enabled,
	})
}

func (h *AdminHandler) ToggleOutage(
	w http.ResponseWriter,
	r *http.Request,
) {
	enabled := h.State.ToggleOutage()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"outage_enabled": enabled,
	})
}