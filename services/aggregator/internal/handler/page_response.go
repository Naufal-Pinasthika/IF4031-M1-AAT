package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/status"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

// Match the existing HTTP contract, including false/null fields omitted by the
// storage type's JSON tags. Serialization belongs to the HTTP read side, not P1.
type eventPageFields struct {
	Events              []model.HazardEvent `json:"events"`
	Partial             bool                `json:"partial"`
	UnavailableUnits    []string            `json:"unavailable_units"`
	NextCursor          *string             `json:"next_cursor"`
	ReplicationDegraded bool                `json:"replication_degraded"`
	LaggingUnits        []string            `json:"lagging_units"`
}

func marshalEventPage(page store.EventPage) ([]byte, error) {
	return json.Marshal(eventPageFields{page.Events, page.Partial, page.UnavailableUnits,
		page.NextCursor, page.ReplicationDegraded, page.LaggingUnits})
}

var pageBuffers = sync.Pool{New: func() any { buffer := make([]byte, 0, 4096); return &buffer }}

// Reuse only immutable page bytes from the existing bounded, 100ms read cache.
// Source health is encoded anew for EVERY response, even on a cache hit. No
// response-specific headers, identity, correlation ID or Audit result is cached.
func writeEventPage(w http.ResponseWriter, page store.EventPage, encoded []byte, sources map[string]status.SourceStatus, requests ...*http.Request) {
	var err error
	if encoded == nil {
		encoded, err = marshalEventPage(page)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to encode hazard events")
			return
		}
	}
	health, err := json.Marshal(sources)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode source health")
		return
	}
	buffer := pageBuffers.Get().(*[]byte)
	defer func() {
		// Do not retain exceptional giant dynamic-attribute responses in the pool.
		if cap(*buffer) <= 1<<20 {
			*buffer = (*buffer)[:0]
			pageBuffers.Put(buffer)
		}
	}()
	body := append((*buffer)[:0], encoded[:len(encoded)-1]...)
	body = append(body, `,"sources":`...)
	body = append(body, health...)
	body = append(body, '}', '\n')
	*buffer = body
	w.Header().Set("Content-Type", "application/json")
	if len(requests) > 0 && compressedPage(w, requests[0], body) {
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
