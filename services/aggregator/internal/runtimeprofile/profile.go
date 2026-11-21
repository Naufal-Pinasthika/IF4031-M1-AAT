package runtimeprofile

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"
)

// Profiling is opt-in and never registered on the business/default mux. The
// fixed loopback bind requires pod exec (or an authenticated Kubernetes tunnel).
// No cmdline, heap, environment or goroutine dump endpoint is exposed.
func Start(ctx context.Context, logger *slog.Logger, enabled bool, build string) error {
	if !enabled {
		return nil
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:6060")
	if err != nil {
		return err
	}
	server := &http.Server{Handler: Handler(build), ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout: 35 * time.Second, IdleTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("profiling_server_stopped", "error", err)
		}
	}()
	logger.Info("profiling_enabled", "address", listener.Addr().String())
	return nil
}

func Handler(build string) http.Handler {
	mux := http.NewServeMux()
	var active sync.Mutex
	mux.HandleFunc("GET /cpu", func(w http.ResponseWriter, r *http.Request) {
		seconds, err := strconv.Atoi(r.URL.Query().Get("seconds"))
		if err != nil || seconds < 1 || seconds > 30 {
			http.Error(w, "seconds must be between 1 and 30", http.StatusBadRequest)
			return
		}
		if !active.TryLock() {
			http.Error(w, "profile already active", http.StatusConflict)
			return
		}
		defer active.Unlock()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "no-store")
		if err := pprof.StartCPUProfile(w); err != nil {
			http.Error(w, "CPU profiler unavailable", http.StatusConflict)
			return
		}
		defer pprof.StopCPUProfile()
		timer := time.NewTimer(time.Duration(seconds) * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("GET /runtime", func(w http.ResponseWriter, _ *http.Request) {
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		read := func(path string) string { value, _ := os.ReadFile(path); return string(value) }
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"time": time.Now().UTC(), "build": build, "go_version": runtime.Version(),
			"total_alloc_bytes": memory.TotalAlloc, "mallocs_total": memory.Mallocs,
			"heap_alloc_bytes": memory.HeapAlloc, "gc_cycles_total": memory.NumGC,
			"gc_pause_nanos_total": memory.PauseTotalNs,
			"gomaxprocs":           runtime.GOMAXPROCS(0), "num_cpu": runtime.NumCPU(),
			"goroutines": runtime.NumGoroutine(), "cpu_max": read("/sys/fs/cgroup/cpu.max"),
			"cpu_stat": read("/sys/fs/cgroup/cpu.stat"), "memory_current": read("/sys/fs/cgroup/memory.current"),
		})
	})
	return mux
}
