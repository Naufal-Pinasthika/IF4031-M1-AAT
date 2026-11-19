package handler

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

func TestCompressionNegotiationAndUnchangedPayload(t *testing.T) {
	for _, test := range []struct {
		header string
		want   bool
	}{
		{"gzip", true}, {"GZIP; q=0.5", true}, {"br, *;q=1", true},
		{"gzip;q=0, *;q=1", false}, {"gzip;q=invalid", false}, {"br", false}, {"", false},
	} {
		t.Run(test.header, func(t *testing.T) {
			if acceptsGzip(test.header) != test.want {
				t.Fatal("incorrect negotiation")
			}
		})
	}
	body := []byte(strings.Repeat(`{"hazard_id":"same-data"}`, 100))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	if !compressedPage(w, r, body) {
		t.Fatal("response not compressed")
	}
	z, err := gzip.NewReader(w.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	got, err := io.ReadAll(z)
	if err != nil || string(got) != string(body) {
		t.Fatal("compression changed payload")
	}
	if w.Header().Get("Vary") != "Accept-Encoding" || w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal("missing negotiation headers")
	}
}

func TestCacheHitDoesNotConsumeFillBudget(t *testing.T) {
	r := NewLimitedReader(readerFunc(func(context.Context, store.EventFilter) (store.EventPage, error) { return store.EventPage{}, nil }), 2, 1)
	c := NewCachedReader(r, time.Second, time.Second)
	if _, err := c.ListEvents(t.Context(), store.EventFilter{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	release, err := r.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err = c.ListEvents(t.Context(), store.EventFilter{Limit: 1}); err != nil {
		t.Fatalf("cached answer took DB slot: %v", err)
	}
	if _, err = c.ListEvents(t.Context(), store.EventFilter{Limit: 2}); !errors.Is(err, store.ErrStorageBusy) {
		t.Fatalf("fill exceeded DB budget: %v", err)
	}
	if _, err = c.ListEvents(store.WithReadPriority(t.Context(), true), store.EventFilter{Limit: 3}); err != nil {
		t.Fatalf("Field reserve lost: %v", err)
	}
}

type shortProjection struct{ calls int }

func (p *shortProjection) ReadPage(ctx context.Context, _ string, _ time.Duration, fill func(context.Context) (store.EventPage, error)) (store.EventPage, time.Time, error) {
	p.calls++
	page, err := fill(ctx)
	return page, time.Now().Add(10 * time.Millisecond), err
}
func TestProjectionDoesNotRenewItsRemainingTTL(t *testing.T) {
	p := &shortProjection{}
	c := NewCachedReader(readerFunc(func(context.Context, store.EventFilter) (store.EventPage, error) { return store.EventPage{}, nil }), time.Second, time.Second)
	c.SetProjection(p)
	_, _ = c.ListEvents(t.Context(), store.EventFilter{})
	time.Sleep(20 * time.Millisecond)
	_, _ = c.ListEvents(t.Context(), store.EventFilter{})
	if p.calls != 2 {
		t.Fatal("L1 restarted projection's original TTL")
	}
}

func TestResponseAdmissionStaysBoundedForCacheHits(t *testing.T) {
	l := NewPriorityLimiter(4, 1)
	for range 3 {
		if !l.acquire(false) {
			t.Fatal("unexpected rejection")
		}
	}
	called := false
	next := l.Wrap(func(http.ResponseWriter, *http.Request) { called = true })
	w := httptest.NewRecorder()
	next(w, httptest.NewRequest("GET", "/", nil))
	if called || w.Code != 429 {
		t.Fatal("unbounded cache fast path")
	}
}

func TestCompressionBudgetFallsBackWithoutChangingResponse(t *testing.T) {
	for range cap(compressionSlots) {
		compressionSlots <- struct{}{}
	}
	defer func() {
		for range cap(compressionSlots) {
			<-compressionSlots
		}
	}()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	if compressedPage(w, r, []byte(strings.Repeat("bounded", 200))) || w.Header().Get("Content-Encoding") != "" {
		t.Fatal("compression escaped its own memory budget")
	}
}
