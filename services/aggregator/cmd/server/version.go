package main

import (
	"encoding/json"
	"net/http"
)

// Set by the image build. Exposing the serving binary version lets operators
// distinguish old/new responses during a rollout rather than trusting a tag.
var buildVersion = "development"

func withBuildVersion(next http.Handler, version string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-AAT-Build-Version", version)
		next.ServeHTTP(w, r)
	})
}

func versionInfo(version string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{"service": "aggregator", "version": version})
	}
}
