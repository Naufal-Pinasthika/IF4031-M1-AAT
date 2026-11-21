package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/status"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

func responseFixture() store.EventPage {
	now := time.Date(2026, 11, 19, 12, 0, 0, 123000000, time.UTC)
	page := store.EventPage{ReadStatus: store.ReadStatus{UnavailableUnits: []string{}}}
	for i := range 20 {
		page.Events = append(page.Events, model.HazardEvent{HazardID: fmt.Sprint(i), Source: model.SourceSENSORPANTAI,
			SourceRefID: fmt.Sprint(i), HazardType: model.HazardTypeCOASTAL, Severity: model.SeveritySIAGA,
			AreaName: "coast <script> \"\\\n", OccuredAt: now, IngestedAt: now,
			Attributes: map[string]any{"dynamic": strings.Repeat("x", 300), "nested": map[string]any{"new_field": []any{true, 3.14, nil, "},\"sources\":fake"}}}})
	}
	return page
}

func oldPageResponse(page store.EventPage, sources map[string]status.SourceStatus) map[string]any {
	return map[string]any{"events": page.Events, "partial": page.Partial, "unavailable_units": page.UnavailableUnits,
		"next_cursor": page.NextCursor, "replication_degraded": page.ReplicationDegraded,
		"lagging_units": page.LaggingUnits, "sources": sources}
}

func TestEncodedPagePreservesHTTPContract(t *testing.T) {
	page := responseFixture()
	cursor := "opaque<>&cursor"
	page.NextCursor = &cursor
	page.Partial, page.ReplicationDegraded = true, true
	page.UnavailableUnits, page.LaggingUnits = []string{"store-a"}, []string{"store-b"}
	for _, page := range []store.EventPage{page, {}, {Events: []model.HazardEvent{}, ReadStatus: store.ReadStatus{UnavailableUnits: []string{}}}} {
		for _, preencoded := range []bool{false, true} {
			sources := map[string]status.SourceStatus{"BMKG": {State: "UP"}}
			var wire []byte
			if preencoded {
				wire, _ = marshalEventPage(page)
			}
			out := httptest.NewRecorder()
			writeEventPage(out, page, wire, sources)
			want, _ := json.Marshal(oldPageResponse(page, sources))
			var gotValue, wantValue any
			if err := json.Unmarshal(out.Body.Bytes(), &gotValue); err != nil {
				t.Fatal(err)
			}
			_ = json.Unmarshal(want, &wantValue)
			if !reflect.DeepEqual(gotValue, wantValue) {
				t.Fatalf("changed response: %s", out.Body)
			}
			if out.Header().Get("Content-Length") != strconv.Itoa(out.Body.Len()) || !strings.HasSuffix(out.Body.String(), "\n") {
				t.Fatal("invalid framing")
			}
		}
	}
}

func TestCachedPageDoesNotCacheSourceHealth(t *testing.T) {
	var calls atomic.Int32
	c := NewCachedReader(readerFunc(func(context.Context, store.EventFilter) (store.EventPage, error) {
		calls.Add(1)
		return responseFixture(), nil
	}), time.Second, time.Second)
	tracker := status.NewTracker()
	h := New(c, tracker)
	tracker.Success("BMKG", time.Now())
	read := func() string {
		w := httptest.NewRecorder()
		h.GetEvents(w, httptest.NewRequest("GET", "/internal/events?limit=20", nil))
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		var response struct {
			Sources map[string]status.SourceStatus `json:"sources"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response.Sources["BMKG"].State
	}
	if read() != "UP" {
		t.Fatal("missing live health")
	}
	tracker.Failure("BMKG", time.Now())
	if read() != "STALE" || calls.Load() != 1 {
		t.Fatal("source health was cached or page missed cache")
	}
}

func TestConcurrentEncodedPagesRemainImmutable(t *testing.T) {
	c := NewCachedReader(readerFunc(func(context.Context, store.EventFilter) (store.EventPage, error) {
		return responseFixture(), nil
	}), time.Second, time.Second)
	_, wire, err := c.listEventsEncoded(t.Context(), store.EventFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	original := string(wire)
	var workers sync.WaitGroup
	for range 64 {
		workers.Go(func() {
			page, wire, err := c.listEventsEncoded(t.Context(), store.EventFilter{Limit: 20})
			if err != nil {
				t.Error(err)
				return
			}
			w := httptest.NewRecorder()
			writeEventPage(w, page, wire, map[string]status.SourceStatus{"BMKG": {State: "UP"}})
			if !json.Valid(w.Body.Bytes()) {
				t.Error("corrupt JSON")
			}
		})
	}
	workers.Wait()
	if string(wire) != original {
		t.Fatal("shared representation mutated")
	}
}

func TestEncodingFailureDoesNotSendSuccess(t *testing.T) {
	page := responseFixture()
	page.Events[0].Latitude = math.NaN()
	w := httptest.NewRecorder()
	writeEventPage(w, page, nil, nil)
	if w.Code != 500 || !json.Valid(w.Body.Bytes()) {
		t.Fatal("invalid encoding reported success")
	}
}

type benchmarkWriter struct{ header http.Header }

func (w benchmarkWriter) Header() http.Header         { return w.header }
func (w benchmarkWriter) WriteHeader(int)             {}
func (w benchmarkWriter) Write(b []byte) (int, error) { return len(b), nil }

func BenchmarkEventPageResponse(b *testing.B) {
	page := responseFixture()
	sources := map[string]status.SourceStatus{"BMKG": {State: "UP"}, "PVMBG": {State: "UP"}, "SENSOR_PANTAI": {State: "STALE"}}
	wire, _ := marshalEventPage(page)
	b.Run("previous-per-request-encoding", func(b *testing.B) {
		w := benchmarkWriter{http.Header{}}
		b.ReportAllocs()
		for b.Loop() {
			writeJSON(w, 200, oldPageResponse(page, sources))
		}
	})
	b.Run("immutable-page-live-health", func(b *testing.B) {
		w := benchmarkWriter{http.Header{}}
		b.ReportAllocs()
		for b.Loop() {
			writeEventPage(w, page, wire, sources)
		}
	})
}
