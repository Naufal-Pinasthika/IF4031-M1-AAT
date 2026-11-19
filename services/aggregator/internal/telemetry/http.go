// Package telemetry supplies identical, bounded HTTP metrics to each independent service.
package telemetry

import (
	"fmt"
	"net/http"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

var bounds = []float64{.001, .005, .01, .025, .05, .1, .25, .3, .5, 1, 2.5, 5, 10}

type Registry struct {
	mu      sync.Mutex
	counts  [600]uint64
	buckets []uint64
	sum     float64
	total   uint64
}
type writer struct {
	http.ResponseWriter
	status int
}

func (w *writer) WriteHeader(n int) {
	if w.status == 0 {
		w.status = n
		w.ResponseWriter.WriteHeader(n)
	}
}
func (w *writer) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *writer) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *writer) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Wrap measures ALL business responses, including controlled rejection. It does
// not infer whole-service metrics from sampled logs or label arbitrary URLs.
func Wrap(next http.Handler, native bool) http.Handler {
	r := &Registry{buckets: make([]uint64, len(bounds))}
	return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/metrics" && q.Method == "GET" {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			if native {
				next.ServeHTTP(w, q)
			}
			r.write(w)
			return
		}
		started := time.Now()
		out := &writer{ResponseWriter: w}
		next.ServeHTTP(out, q)
		if q.URL.Path == "/health" || q.URL.Path == "/ready" || q.URL.Path == "/healthz" {
			return
		}
		status := out.status
		if status == 0 {
			status = 200
		}
		if status < 100 || status >= 600 {
			status = 500
		}
		elapsed := time.Since(started).Seconds()
		r.mu.Lock()
		defer r.mu.Unlock()
		r.counts[status]++
		r.total++
		r.sum += elapsed
		for i, b := range bounds {
			if elapsed <= b {
				r.buckets[i]++
			}
		}
	})
}
func (r *Registry) write(w http.ResponseWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintln(w, "# TYPE aat_http_requests_total counter")
	for s, n := range r.counts {
		if n > 0 {
			fmt.Fprintf(w, "aat_http_requests_total{status=%q} %d\n", fmt.Sprint(s), n)
		}
	}
	fmt.Fprintln(w, "# TYPE aat_http_request_duration_seconds histogram")
	for i, b := range bounds {
		fmt.Fprintf(w, "aat_http_request_duration_seconds_bucket{le=%q} %d\n", fmt.Sprint(b), r.buckets[i])
	}
	fmt.Fprintf(w, "aat_http_request_duration_seconds_bucket{le=\"+Inf\"} %d\naat_http_request_duration_seconds_count %d\naat_http_request_duration_seconds_sum %g\n", r.total, r.total, r.sum)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Fprintf(w, "aat_go_heap_bytes %d\naat_go_goroutines %d\n", mem.HeapAlloc, runtime.NumGoroutine())
	fmt.Fprintf(w, "aat_go_runtime_memory_bytes %d\naat_go_memory_limit_bytes %d\n", mem.Sys-mem.HeapReleased, debug.SetMemoryLimit(-1))
}
