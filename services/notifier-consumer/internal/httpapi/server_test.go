package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/notifier-consumer/internal/store"
)

type blockedProbe struct{ release chan struct{} }

func (p blockedProbe) Probe(context.Context) error { <-p.release; return nil }

type countedProbe struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (p *countedProbe) Probe(context.Context) error {
	if p.calls.Add(1) == 1 {
		close(p.entered)
	}
	<-p.release
	return nil
}

func TestOverlappingHealthChecksShareSuccessfulProbe(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	probe := &countedProbe{entered: make(chan struct{}), release: make(chan struct{})}
	server := &Server{store: s, probe: probe, healthTimeout: time.Second}
	first := server.startProbe()
	<-probe.entered
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if server.startProbe() != first {
				t.Error("created another probe")
			}
		}()
	}
	wg.Wait()
	close(probe.release)
	<-first.done
	if first.err != nil || probe.calls.Load() != 1 {
		t.Fatalf("probe failed or duplicated: %v %d", first.err, probe.calls.Load())
	}
}

func TestHealthTimeoutBoundsHandlerAndOutstandingWork(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "health.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	probe := blockedProbe{release: make(chan struct{})}
	defer close(probe.release)
	handler := New(s, probe, slog.New(slog.NewTextHandler(io.Discard, nil)), 50*time.Millisecond)
	for i := 0; i < 20; i++ {
		started := time.Now()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/health", nil))
		if response.Code != 503 || time.Since(started) > 300*time.Millisecond {
			t.Fatalf("probe not bounded: %d %v", response.Code, time.Since(started))
		}
	}
}

func TestNotificationsValidatePagination(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "http.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)
	for _, path := range []string{"/notifications?limit=0", "/notifications?limit=1001", "/notifications?limit=no", "/notifications?cursor=!"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 400 {
			t.Fatalf("%s: %d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/notifications", nil))
	var page store.NotificationPage
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &page) != nil || page.Notifications == nil || len(page.Notifications) != 0 || page.NextCursor != "" {
		t.Fatalf("unexpected empty page: %d %s", response.Code, response.Body.String())
	}
}
