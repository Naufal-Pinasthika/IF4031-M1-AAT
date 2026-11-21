package handler

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

func TestExplicitSchemaVersionsAndModes(t *testing.T) {
	for _, tc := range []struct {
		body, mode string
		status     int
	}{
		{`{"version":1}`, "legacy", 200},
		{`{"version":2}`, "additive", 200},
		{`{"version":"v2"}`, "additive", 200},
		{`{"version":"2"}`, "additive", 200},
		{`{"version":3}`, "rename", 200},
		{`{"mode":"type-change"}`, "type-change", 200},
		{`{"version":99}`, "", 400},
		{`{"version":2,"mode":"legacy"}`, "", 400},
		{`{"mode":"unknown"}`, "", 400},
	} {
		t.Run(tc.body, func(t *testing.T) {
			s := state.NewServiceState()
			h := &AdminHandler{State: s}
			for i := 0; i < 2; i++ {
				w := httptest.NewRecorder()
				h.ToggleSchemaVersion(w, httptest.NewRequest("POST", "/admin/schema-version", strings.NewReader(tc.body)))
				if w.Code != tc.status || s.SchemaMode() != tc.mode {
					t.Fatalf("status=%d mode=%q", w.Code, s.SchemaMode())
				}
			}
		})
	}
}
