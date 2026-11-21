package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

type AdminHandler struct {
	State *state.ServiceState
}

func (h *AdminHandler) ToggleSchemaVersion(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	if err != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	if len(raw) > 0 {
		var request struct {
			Mode string `json:"mode"`
		}
		if json.Unmarshal(raw, &request) != nil {
			http.Error(w, "invalid mode", 400)
			return
		}
		switch request.Mode {
		case "legacy", "additive", "rename", "type-change", "missing", "truncated", "deep", "invalid-utf8", "oversized":
		default:
			http.Error(w, "unsupported mode", 400)
			return
		}
		h.State.SetSchemaMode(request.Mode)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"mode": request.Mode})
		return
	}
	enabled := h.State.ToggleSchemaVersion()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"schema_v2_enabled": enabled})
}

func (h *AdminHandler) ToggleOutage(w http.ResponseWriter, r *http.Request) {
	enabled := h.State.ToggleOutage()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"outage_enabled": enabled})
}
