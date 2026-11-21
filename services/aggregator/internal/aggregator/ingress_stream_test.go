package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestM2SchemaPolicyAndPoisonIsolation(t *testing.T) {
	base := `{"report_id":"p3-1","volcano_id":"MERAPI","alert_level":"Siaga","eruption_count_24h":2,"ash_column_height_m":10,"reported_at":"2026-11-20T00:00:00Z","confidence_level":0.9}`
	for _, version := range []int{1, 2} {
		e, err := DecodeVolcanic([]byte(base), version)
		if err != nil || e.SourceRefID != "p3-1" {
			t.Fatal(e, err)
		}
	}
	renamed := strings.Replace(base, `"report_id"`, `"report_ref"`, 1)
	typed := strings.Replace(base, `"eruption_count_24h":2`, `"eruption_count_24h":"2"`, 1)
	for _, raw := range []string{renamed, typed} {
		if _, err := DecodeVolcanic([]byte(raw), 1); err == nil {
			t.Fatal("v1 guessed incompatible schema")
		}
		if _, err := DecodeVolcanic([]byte(raw), 2); err != nil {
			t.Fatal(err)
		}
	}
	bad := [][]byte{[]byte(`[{`), []byte{'"', 255, '"'}, []byte(strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100)), []byte(strings.Repeat(" ", 2<<20)), []byte(strings.Replace(base, `"alert_level":"Siaga",`, "", 1))}
	for _, raw := range bad {
		if _, err := DecodeVolcanic(raw, 2); err == nil {
			t.Fatal("invalid payload accepted")
		}
		if _, err := DecodeVolcanic([]byte(base), 2); err != nil {
			t.Fatal("poison blocked next message", err)
		}
	}
}

func TestM2StreamReconnectCommitsSequencedHeartbeatButNotFailedHandler(t *testing.T) {
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Query().Get("from"))
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Query().Get("from") == "0" {
			fmt.Fprint(w, "event: heartbeat\ndata: {}\n\nevent: detection\nid: 0\ndata: {}\n\nevent: heartbeat\nid: 1\ndata: {\"offset\":1}\n\nevent: update\nid: 2\ndata: {}\n\n")
		} else {
			fmt.Fprint(w, "event: update\nid: 2\ndata: {}\n\n")
		}
	}))
	defer server.Close()
	stream := NewSatelliteStream(server.URL, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	var committed []int64
	offset, err := stream.consume(context.Background(), -1, func(e SatelliteStreamEvent) error {
		if e.Offset == 2 {
			return fmt.Errorf("crash before commit")
		}
		committed = append(committed, e.Offset)
		return nil
	})
	if err == nil || offset != 1 || fmt.Sprint(committed) != "[0 1]" {
		t.Fatalf("lost sequenced heartbeat or acknowledged failed handler: %d %v %v", offset, err, committed)
	}
	next, _ := stream.consume(context.Background(), offset, func(e SatelliteStreamEvent) error {
		if e.Offset != 2 {
			t.Fatal(e)
		}
		return nil
	})
	if next != 2 || len(requested) != 2 || requested[1] != "2" {
		t.Fatal(next, requested)
	}
	if _, err := buildSatelliteEvent("detection", "garbage", []string{`{}`}); err == nil {
		t.Fatal("missing offset accepted")
	}
	if event, err := buildSatelliteEvent("detection", "", []string{`{"offset":0}`}); err != nil || event.Offset != 0 {
		t.Fatal("zero envelope offset rejected", event, err)
	}
}

func TestM2RepeatedAcceleratedFrames(t *testing.T) {
	for round := 0; round < 8; round++ {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for i := 1; i <= 1000; i++ {
				raw, _ := json.Marshal(map[string]int{"offset": i})
				fmt.Fprintf(w, "event: detection\nid: %d\ndata: %s\n\n", i, raw)
			}
		}))
		stream := NewSatelliteStream(server.URL, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
		count := 0
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		offset, _ := stream.consume(ctx, 0, func(e SatelliteStreamEvent) error {
			count++
			if e.Offset != int64(count) {
				t.Fatal("out of order")
			}
			return nil
		})
		cancel()
		server.Close()
		if count != 1000 || offset != 1000 {
			t.Fatal(count, offset)
		}
	}
}

func TestM2SatelliteBatchFlushesIdleTail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "event: detection\nid: 0\ndata: {}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	consumer := NewSatelliteConsumer(server.URL, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	called := false
	err := consumer.ConsumeBatches(ctx, -1, func(batch []SatelliteStreamEvent) error {
		if len(batch) != 1 || batch[0].Offset != 0 || batch[0].ReceivedAt.IsZero() {
			t.Error("tail or ingress timestamp missing", batch)
		}
		called = true
		cancel()
		return nil
	})
	if err != nil || !called {
		t.Fatal("idle tail was not flushed", called, err)
	}
}

func TestM2SatelliteFailedBatchReloadsDurableOffset(t *testing.T) {
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Query().Get("from"))
		for i := 0; i < 80; i++ {
			fmt.Fprintf(w, "event: heartbeat\nid: %d\ndata: {}\n\n", i)
		}
	}))
	defer server.Close()
	consumer := NewSatelliteConsumer(server.URL, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	failure := fmt.Errorf("batch commit failed")
	err := consumer.ConsumeBatches(ctx, -1, func(batch []SatelliteStreamEvent) error {
		if len(batch) < 1 || len(batch) > 16 {
			t.Error("unbounded batch", len(batch))
		}
		return failure
	})
	if err != failure {
		t.Fatal("processing failure did not stop speculative reader", err)
	}
	err = consumer.ConsumeBatches(ctx, -1, func(batch []SatelliteStreamEvent) error {
		if batch[0].Offset != 0 {
			t.Error("uncommitted queued frames skipped", batch[0].Offset)
		}
		return failure
	})
	if err != failure || len(requested) != 2 || requested[0] != "0" || requested[1] != "0" {
		t.Fatal("restarted from speculative instead of durable offset", requested, err)
	}
}
