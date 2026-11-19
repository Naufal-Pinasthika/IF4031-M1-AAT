package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/model"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

type coastalAdmissionRecorder struct {
	SourceCoordinator
	accepted      []model.HazardEvent
	parked        int
	cursor        string
	quarantineErr error
	admissionErr  error
}

func (r *coastalAdmissionRecorder) Quarantine(context.Context, string, string, []byte, bool) error {
	r.parked++
	return r.quarantineErr
}
func (r *coastalAdmissionRecorder) AcceptBatch(_ context.Context, _, _ string, _ int64, events []model.HazardEvent, _ string) error {
	if r.admissionErr == nil {
		r.accepted = append(r.accepted, events...)
	}
	return r.admissionErr
}

func TestCoastalClockFailurePreservesValidCriticalWithoutDeclaringRecovery(t *testing.T) {
	for _, reason := range []string{store.ErrSourceClockStale.Error(), store.ErrSourceClockRegression.Error()} {
		t.Run(reason, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/stations" {
					_, _ = io.WriteString(w, `{"items":[{"station_id":"station","name":"Station","province":"Region","latitude":0,"longitude":0}]}`)
				} else {
					// Do not invent an allowed-skew bound against an unhealthy
					// generated_at clock; both timestamp fields obey the contract.
					_, _ = io.WriteString(w, `{"generated_at":"2026-11-19T00:00:00Z","items":[{"observation_id":"critical","station_id":"station","observed_at":"2026-11-20T00:00:00Z","sea_level_anomaly_m":1,"wave_height_m":2,"quality":"ok","critical":true}],"next_cursor":"opaque-critical"}`)
				}
			}))
			defer server.Close()
			client := NewCoastalClient(time.Second, 1, time.Second, logger)
			page, fetchErr := client.FetchPage(t.Context(), server.URL, "component-test", "", "clock-fault", func(context.Context, time.Time) error {
				return &CoastalProtocolError{Reason: reason, Retryable: true}
			})
			if fetchErr == nil || len(page.Items) != 1 || client.CircuitState() != "open" {
				t.Fatalf("critical page hidden or bad clock declared healthy: items=%d err=%v circuit=%s", len(page.Items), fetchErr, client.CircuitState())
			}
			for _, failure := range []string{"none", "quarantine", "admission"} {
				r := &coastalAdmissionRecorder{}
				if failure == "quarantine" {
					r.quarantineErr = errors.New("quarantine storage unavailable")
				}
				if failure == "admission" {
					r.admissionErr = store.ErrLeaseLost
				}
				a := &Aggregator{coastal: client, coordinator: r, logger: logger, config: config.Config{InstanceID: "component-test"}}
				if err := a.handleCoastalPage(t.Context(), page, fetchErr, "clock-fault", 1); err == nil {
					t.Fatal("unhealthy source reported successful poll")
				}
				if failure == "none" {
					if len(r.accepted) != 1 || r.cursor != "opaque-critical" || r.parked != 1 {
						t.Fatal("valid critical record lost")
					}
				} else if len(r.accepted) != 0 || r.cursor != "" {
					t.Fatalf("%s failure advanced cursor", failure)
				}
			}
			if _, err := client.FetchPage(t.Context(), server.URL, "component-test", "", "probe", nil); !errors.Is(err, ErrCircuitOpen) {
				t.Fatal("critical admission bypassed the open breaker")
			}
		})
	}
}
func (r *coastalAdmissionRecorder) AdvanceOpaqueCursor(_ context.Context, _, _ string, _ int64, cursor string) error {
	r.cursor = cursor
	return nil
}

// Property-style component test, NOT a substitute for the official black-box
// image/E2E. Randomness changes IDs/order/invalid field types, never prod logic.
func TestCoastalRandomizedMixedPagesPreserveAllValidCriticalItems(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	if value := os.Getenv("P5_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("replay with P5_TEST_SEED=%d", seed)
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	var response atomic.Value
	response.Store([]byte(`{}`))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer component-test-only" {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.URL.Path == "/v1/stations" {
			_, _ = io.WriteString(w, `{"items":[{"station_id":"station","name":"Station","province":"Region","latitude":0,"longitude":0}]}`)
		} else {
			_, _ = w.Write(response.Load().([]byte))
		}
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for round := range 200 {
		var rows []any
		expected := make(map[string]bool)
		for i := range 7 {
			id := fmt.Sprintf("random-%x-%d-%d", rng.Uint64(), round, i)
			critical := i%2 == 0
			expected[id] = critical
			rows = append(rows, map[string]any{"observation_id": id, "station_id": "station", "observed_at": "2026-11-19T00:00:00Z",
				"sea_level_anomaly_m": 0, "wave_height_m": rng.Float64(), "quality": "ok", "critical": critical, "additive_field": rng.IntN(100)})
		}
		for i := range 993 {
			item := map[string]any{"observation_id": fmt.Sprintf("invalid-%d-%d", round, i), "station_id": "station", "observed_at": "2026-11-19T00:00:00Z",
				"sea_level_anomaly_m": 0.2, "wave_height_m": 1, "quality": "ok", "critical": false}
			switch rng.IntN(9) {
			case 0:
				delete(item, "critical")
			case 1:
				item["critical"] = "true"
			case 2:
				item["sea_level_anomaly_m"] = "not a measurement"
			case 3:
				item["wave_height_m"] = -1
			case 4:
				item["station_id"] = []string{"station"}
			case 5:
				item["observed_at"] = "not a timestamp"
			case 6:
				item["quality"] = nil
			case 7:
				item["observation_id"] = " "
			case 8:
				item["station_id"] = "unknown"
			}
			rows = append(rows, item)
		}
		rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
		body, err := json.Marshal(map[string]any{"generated_at": "2026-11-19T00:00:01Z", "items": rows, "next_cursor": "opaque-next"})
		if err != nil {
			t.Fatal(err)
		}
		response.Store(body)
		client := NewCoastalClient(time.Second, 3, time.Second, logger)
		page, err := client.FetchPage(t.Context(), server.URL, "component-test-only", "", "randomized", nil)
		if err != nil || len(page.Items) != 7 || len(page.Invalid) != 993 {
			t.Fatalf("seed=%d round=%d valid=%d invalid=%d err=%v", seed, round, len(page.Items), len(page.Invalid), err)
		}
		recorder := &coastalAdmissionRecorder{}
		a := &Aggregator{coastal: client, coordinator: recorder, logger: logger, config: config.Config{InstanceID: "component-test"}}
		if err := a.processCoastal(t.Context(), page, "randomized", 1); err != nil {
			t.Fatal(err)
		}
		if recorder.parked != 993 || recorder.cursor != "opaque-next" || len(recorder.accepted) != 7 {
			t.Fatalf("seed=%d round=%d invalid items leaked or checkpoint mismatch", seed, round)
		}
		for _, event := range recorder.accepted {
			critical, exists := expected[event.SourceRefID]
			if !exists || event.Attributes["critical"] != critical {
				t.Fatalf("seed=%d round=%d critical item lost/corrupted: %+v", seed, round, event)
			}
			delete(expected, event.SourceRefID)
		}
		if len(expected) != 0 {
			t.Fatalf("seed=%d round=%d missing valid items: %v", seed, round, expected)
		}
	}
}

func TestCoastalMalformedPageDoesNotPreventLaterValidPage(t *testing.T) {
	cases := []string{
		`{"generated_at":"2026-11-19T00:00:01Z","items":[`,
		`{"generated_at":"not a date","items":[]}`,
		strings.Repeat("[", 11000) + strings.Repeat("]", 11000),
		strings.Repeat("x", coastalResponseLimit+1),
		`{"generated_at":"2026-11-19T00:00:01Z","items":null,"next_cursor":"must-not-advance"}`,
	}
	for index, body := range cases {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					_, _ = io.WriteString(w, body)
				} else {
					_, _ = io.WriteString(w, `{"generated_at":"2026-11-19T00:00:01Z","items":[],"next_cursor":"valid-next"}`)
				}
			}))
			defer server.Close()
			client := NewCoastalClient(time.Second, 3, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
			page, err := client.fetchObservations(t.Context(), server.URL, "component-test", "", "malformed")
			if err == nil || page.NextCursor != "" || len(page.Items) != 0 {
				t.Fatalf("bad envelope accepted: %v", err)
			}
			page, err = client.fetchObservations(t.Context(), server.URL, "component-test", "", "recovery")
			if err != nil || page.NextCursor != "valid-next" {
				t.Fatalf("valid page after malformed input was lost: %v", err)
			}
		})
	}
}
