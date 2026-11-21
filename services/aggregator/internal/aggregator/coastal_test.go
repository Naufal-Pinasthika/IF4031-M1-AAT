package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoastalRequiredFieldsDoNotDefaultToZero(t *testing.T) {
	valid := []byte(`{"observation_id":"obs-1","station_id":"stn-1","observed_at":"2026-11-18T12:00:00Z","sea_level_anomaly_m":0,"wave_height_m":0,"quality":"ok","critical":false}`)
	required := []string{"observation_id", "station_id", "observed_at", "sea_level_anomaly_m", "wave_height_m", "quality", "critical"}
	var item CoastalObservation
	if err := decodeCoastalObject(valid, &item, required...); err != nil {
		t.Fatalf("valid zero measurements rejected: %v", err)
	}
	for _, name := range required {
		for _, null := range []bool{false, true} {
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(valid, &fields)
			delete(fields, name)
			if null {
				fields[name] = json.RawMessage("null")
			}
			raw, _ := json.Marshal(fields)
			if err := decodeCoastalObject(raw, &item, required...); err == nil {
				t.Errorf("accepted missing/null field %s (null=%v)", name, null)
			}
		}
	}
}

func TestCoastalClientSeparatesInvalidObservation(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/stations":
			_, _ = io.WriteString(w, `{"items":[{"station_id":"stn-1","name":"Padang","province":"Sumatera Barat","latitude":-0.9,"longitude":100.3}]}`)
		case "/v1/observations":
			_, _ = fmt.Fprintf(w, `{"generated_at":%q,"items":[{"observation_id":"obs-1","station_id":"stn-1","observed_at":%q,"sea_level_anomaly_m":0.4,"wave_height_m":1.8,"quality":"ok","critical":true},{"observation_id":"obs-bad","station_id":"missing","observed_at":%q,"sea_level_anomaly_m":0.1,"wave_height_m":1,"quality":"ok","critical":false}],"next_cursor":"next"}`,
				now.Format(time.RFC3339), now.Add(-time.Second).Format(time.RFC3339), now.Format(time.RFC3339))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewCoastalClient(time.Second, 3, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	page, err := client.FetchPage(t.Context(), server.URL, "secret", "", "corr", nil)
	if err != nil {
		t.Fatalf("FetchPage returned error: %v", err)
	}
	if len(page.Items) != 1 || !page.Items[0].Critical || len(page.Invalid) != 1 || page.NextCursor != "next" {
		t.Fatalf("unexpected page: %+v", page)
	}
}

func TestCoastalClientOpensCircuitOnPersistedClockStall(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = io.WriteString(w, `{"items":[{"station_id":"stn-1","name":"Padang","province":"Sumatera Barat","latitude":-0.9,"longitude":100.3}]}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"generated_at":%q,"items":[],"next_cursor":null}`, now.Add(-time.Hour).Format(time.RFC3339))
	}))
	defer server.Close()

	client := NewCoastalClient(time.Second, 1, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := client.FetchPage(t.Context(), server.URL, "secret", "", "corr", func(_ context.Context, _ time.Time) error { return errors.New("source_clock_stalled") })
	if err == nil {
		t.Fatal("stale response must fail")
	}
	if client.CircuitState() != "open" {
		t.Fatalf("stale response should open circuit, got %s", client.CircuitState())
	}
}

// Envelope validation must not mistake an absent/null collection for a valid
// empty page and commit its cursor, silently skipping unvalidated source data.
func TestCoastalEnvelopeRequiresObservationCollection(t *testing.T) {
	for _, body := range []string{
		`{"generated_at":"2026-11-18T12:00:00Z","next_cursor":"advance"}`,
		`{"generated_at":"2026-11-18T12:00:00Z","items":null,"next_cursor":"advance"}`,
		`{"generated_at":"2026-11-18T12:00:00Z","items":{},"next_cursor":"advance"}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			client := NewCoastalClient(time.Second, 3, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
			_, err := client.fetchObservations(t.Context(), server.URL, "test", "", "contract-test")
			var protocol *CoastalProtocolError
			if !errors.As(err, &protocol) || string(protocol.Payload) != body {
				t.Fatalf("invalid envelope must be quarantinable without advancing cursor: %v", err)
			}
		})
	}
}

func TestSlowFailureStillGetsFullCircuitCooldown(t *testing.T) {
	var observations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = io.WriteString(w, `{"items":[{"station_id":"stn-1","name":"Padang","province":"Sumatera Barat","latitude":-0.9,"longitude":100.3}]}`)
			return
		}
		observations.Add(1)
		time.Sleep(100 * time.Millisecond)
		http.Error(w, "unavailable", 503)
	}))
	defer server.Close()
	client := NewCoastalClient(time.Second, 1, 50*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := client.FetchPage(t.Context(), server.URL, "test", "", "slow", nil); err == nil {
		t.Fatal("failed call succeeded")
	}
	if _, err := client.FetchPage(t.Context(), server.URL, "test", "", "probe", nil); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("cooldown elapsed during slow call: %v", err)
	}
	if observations.Load() != 1 {
		t.Fatalf("unhealthy source called %d times", observations.Load())
	}
}
