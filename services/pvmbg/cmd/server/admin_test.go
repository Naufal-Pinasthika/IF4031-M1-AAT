package main

import (
	"net/http/httptest"
	"testing"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/pvmbg/internal/state"
)

func TestAdminListenerChangesSharedStateWithoutExposingData(t *testing.T) {
	s := state.NewServiceState()
	h := newAdminRouter(s)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/admin/schema-version", nil))
	if w.Code != 200 || !s.IsSchemaV2() {
		t.Fatalf("schema control failed: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/admin/outage", nil))
	if w.Code != 200 || !s.IsOutage() {
		t.Fatalf("outage control failed: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/volcanic-reports?since=2026-01-01T00:00:00Z", nil))
	if w.Code != 404 {
		t.Fatal("control listener exposed the credential-protected data API")
	}
}
