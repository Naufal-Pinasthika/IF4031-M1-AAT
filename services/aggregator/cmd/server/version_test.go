package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBuildVersionPreservesStatusAndBody(t *testing.T) {
	for _, status := range []int{200, 401, 429, 503} {
		handler := withBuildVersion(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("unchanged"))
		}), "regression-v3")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
		if response.Code != status || response.Body.String() != "unchanged" || response.Header().Get("X-AAT-Build-Version") != "regression-v3" {
			t.Fatalf("version middleware changed response: %+v", response)
		}
	}
}

func TestVersionEndpointReportsActualBuild(t *testing.T) {
	response := httptest.NewRecorder()
	versionInfo("release-12")(response, httptest.NewRequest(http.MethodGet, "/version", nil))
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["version"] != "release-12" || body["service"] != "aggregator" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("invalid version information: %s (%v)", response.Body, err)
	}
}
