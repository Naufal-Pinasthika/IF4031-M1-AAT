package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardServesSelfContainedPageWithoutCaching(t *testing.T) {
	response := httptest.NewRecorder()
	serveDashboard(response, httptest.NewRequest("GET", "/dashboard", nil))
	if response.Code != 200 || response.Header().Get("Content-Type") != "text/html; charset=utf-8" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected dashboard response: %d %v", response.Code, response.Header())
	}
	body := response.Body.String()
	for _, marker := range []string{`id="token" type="password"`, `id="from"`, `id="to"`, `id="chart"`, `id="severity"`, `id="areas"`, `aria-live="polite"`, "Data parsial", "Data lama", "replication_degraded"} {
		if !strings.Contains(body, marker) {
			t.Errorf("dashboard missing required control or status: %s", marker)
		}
	}
	for _, marker := range []string{`<script src=`, `<link rel="stylesheet"`, "localStorage", "sessionStorage"} {
		if strings.Contains(body, marker) {
			t.Errorf("dashboard must not depend on external assets or persist bearer tokens: %s", marker)
		}
	}
}
