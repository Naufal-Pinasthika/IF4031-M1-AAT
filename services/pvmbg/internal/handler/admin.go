package handler

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"

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
			Mode    string          `json:"mode"`
			Version json.RawMessage `json:"version"`
		}
		if json.Unmarshal(raw, &request) != nil {
			http.Error(w, "invalid mode", 400)
			return
		}
		if len(request.Version) > 0 {
			if request.Mode != "" {
				http.Error(w, "specify mode or version, not both", 400)
				return
			}
			var version string
			if json.Unmarshal(request.Version, &version) != nil {
				version = string(request.Version)
			}
			switch strings.TrimPrefix(version, "v") {
			case "1":
				request.Mode = "legacy"
			case "2":
				request.Mode = "additive"
			case "3":
				request.Mode = "rename"
			default:
				http.Error(w, "unsupported schema version", 400)
				return
			}
		}
		switch request.Mode {
		case "legacy", "additive", "rename", "type-change", "missing", "truncated", "deep", "invalid-utf8", "oversized":
		default:
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(raw, &fields)
			names := make([]string, 0, len(fields))
			for name := range fields {
				names = append(names, name)
			}
			sort.Strings(names)
			mode := request.Mode
			if len(mode) > 64 {
				mode = mode[:64]
			}
			slog.Warn("schema_mode_rejected", "correlation_id", w.Header().Get("X-Correlation-ID"), "requested_mode", mode, "body_fields", names)
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
