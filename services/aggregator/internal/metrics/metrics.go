package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

var Default = New()

type Registry struct {
	mu             sync.RWMutex
	leaders        map[string]float64
	polls          map[string]uint64
	httpRequests   map[string]uint64
	circuit        string
	invalid        uint64
	critical       uint64
	coastalLatency histogram
	httpLatency    histogram
}

type histogram struct {
	bounds []float64
	count  []uint64
	sum    float64
	total  uint64
}

func New() *Registry {
	return &Registry{
		leaders:        make(map[string]float64),
		polls:          make(map[string]uint64),
		httpRequests:   make(map[string]uint64),
		circuit:        "closed",
		coastalLatency: newHistogram([]float64{50, 100, 250, 500, 1000, 2500, 5000}),
		httpLatency:    newHistogram([]float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500}),
	}
}

func newHistogram(bounds []float64) histogram {
	return histogram{bounds: bounds, count: make([]uint64, len(bounds))}
}

func (h *histogram) observe(value float64) {
	h.total++
	h.sum += value
	for index, bound := range h.bounds {
		if value <= bound {
			h.count[index]++
		}
	}
}

func (r *Registry) Leader(source string, leader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if leader {
		r.leaders[source] = 1
	} else {
		r.leaders[source] = 0
	}
}

func (r *Registry) Poll(source, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.polls[source+"\x00"+result]++
}

func (r *Registry) Circuit(state string) {
	r.mu.Lock()
	r.circuit = state
	r.mu.Unlock()
}

func (r *Registry) Invalid(count int) {
	if count <= 0 {
		return
	}
	r.mu.Lock()
	r.invalid += uint64(count)
	r.mu.Unlock()
}

func (r *Registry) Critical() {
	r.mu.Lock()
	r.critical++
	r.mu.Unlock()
}

func (r *Registry) CoastalLatency(duration time.Duration) {
	r.mu.Lock()
	r.coastalLatency.observe(float64(duration.Milliseconds()))
	r.mu.Unlock()
}

func (r *Registry) HTTP(method, path string, status int, duration time.Duration) {
	r.mu.Lock()
	r.httpRequests[fmt.Sprintf("%s\x00%s\x00%d", method, path, status)]++
	r.httpLatency.observe(float64(duration.Milliseconds()))
	r.mu.Unlock()
}

func (r *Registry) WritePrometheus(w io.Writer) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fmt.Fprintln(w, "# TYPE aggregator_source_leader gauge")
	for _, key := range sortedFloatKeys(r.leaders) {
		fmt.Fprintf(w, "aggregator_source_leader{source=%q} %g\n", key, r.leaders[key])
	}
	fmt.Fprintln(w, "# TYPE aggregator_polls_total counter")
	for _, key := range sortedUintKeys(r.polls) {
		parts := splitKey(key, 2)
		fmt.Fprintf(w, "aggregator_polls_total{source=%q,result=%q} %d\n", parts[0], parts[1], r.polls[key])
	}
	fmt.Fprintln(w, "# TYPE aggregator_coastal_circuit_state gauge")
	for _, state := range []string{"closed", "open", "half_open"} {
		value := 0
		if r.circuit == state {
			value = 1
		}
		fmt.Fprintf(w, "aggregator_coastal_circuit_state{state=%q} %d\n", state, value)
	}
	fmt.Fprintf(w, "# TYPE aggregator_coastal_invalid_total counter\naggregator_coastal_invalid_total %d\n", r.invalid)
	fmt.Fprintf(w, "# TYPE aggregator_coastal_critical_total counter\naggregator_coastal_critical_total %d\n", r.critical)
	writeHistogram(w, "aggregator_coastal_batch_duration_ms", r.coastalLatency)
	fmt.Fprintln(w, "# TYPE aggregator_http_requests_total counter")
	for _, key := range sortedUintKeys(r.httpRequests) {
		parts := splitKey(key, 3)
		fmt.Fprintf(w, "aggregator_http_requests_total{method=%q,path=%q,status=%q} %d\n", parts[0], parts[1], parts[2], r.httpRequests[key])
	}
	writeHistogram(w, "aggregator_http_request_duration_ms", r.httpLatency)
}

func writeHistogram(w io.Writer, name string, value histogram) {
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	for index, bound := range value.bounds {
		fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, fmt.Sprintf("%g", bound), value.count[index])
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, value.total)
	fmt.Fprintf(w, "%s_sum %g\n%s_count %d\n", name, value.sum, name, value.total)
}

func sortedFloatKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedUintKeys(values map[string]uint64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func splitKey(value string, expected int) []string {
	result := make([]string, 0, expected)
	start := 0
	for index := 0; index < len(value) && len(result) < expected-1; index++ {
		if value[index] == 0 {
			result = append(result, value[start:index])
			start = index + 1
		}
	}
	result = append(result, value[start:])
	for len(result) < expected {
		result = append(result, "")
	}
	return result
}
