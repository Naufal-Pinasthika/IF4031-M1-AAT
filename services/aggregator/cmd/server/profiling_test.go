package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProfilingDisabledByDefault(t *testing.T) {
	t.Setenv("AGGREGATOR_PROFILE_ENABLED", "")
	if err := startProfiling(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
}

func TestProfilingSurfaceIsBounded(t *testing.T) {
	h := profilingHandler()
	for _, target := range []string{"/cpu", "/cpu?seconds=0", "/cpu?seconds=31", "/cpu?seconds=-1"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", target, nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d", target, w.Code)
		}
	}
	for _, target := range []string{"/debug/pprof/", "/debug/pprof/heap", "/cmdline"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", target, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("unexpected diagnostic surface %s", target)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/runtime", nil))
	var state map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state["gomaxprocs"].(float64) < 1 {
		t.Fatal(state)
	}
}

func TestCPUProfileCompletesAndReleasesProfiler(t *testing.T) {
	h := profilingHandler()
	for range 2 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/cpu?seconds=1", nil))
		body := w.Body.Bytes()
		if w.Code != 200 || len(body) < 2 || body[0] != 0x1f || body[1] != 0x8b {
			t.Fatalf("invalid CPU profile: status=%d length=%d", w.Code, len(body))
		}
	}
}
