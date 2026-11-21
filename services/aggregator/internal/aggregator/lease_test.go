package aggregator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/store"
)

type renewalCoordinator struct {
	SourceCoordinator
	renewals int
}

func TestCoastalFailoverBudgetDoesNotShortenOtherSources(t *testing.T) {
	a := &Aggregator{config: config.Config{LeaseTTL: 15 * time.Second, CoastalLeaseTTL: 4 * time.Second}}
	if a.sourceLeaseTTL("SENSOR_PANTAI") != 4*time.Second || a.sourceLeaseTTL("BMKG") != 15*time.Second || a.sourceLeaseTTL("PVMBG") != 15*time.Second {
		t.Fatal("source lease isolation failed")
	}
}

type pollCoordinator struct {
	SourceCoordinator
	begin                 func() (store.PollAttempt, bool, error)
	finished              error
	finishCalls, released int
	validations           atomic.Int32
}

func (c *pollCoordinator) BeginPoll(context.Context, string, string, int64, time.Duration) (store.PollAttempt, bool, error) {
	return c.begin()
}
func (c *pollCoordinator) FinishPoll(_ context.Context, _ store.PollAttempt, _ time.Duration, err error) error {
	c.finished = err
	c.finishCalls++
	return nil
}
func (c *pollCoordinator) ReleaseLease(context.Context, string, string, int64) error {
	c.released++
	return nil
}
func (c *pollCoordinator) RenewLease(context.Context, string, string, int64, time.Duration) error {
	return nil
}
func (c *pollCoordinator) ValidatePoll(context.Context, string, string, int64, int64) error {
	c.validations.Add(1)
	return nil
}

func TestPollCycleDefersWithoutCallingSource(t *testing.T) {
	c := &pollCoordinator{begin: func() (store.PollAttempt, bool, error) { return store.PollAttempt{RetryAfter: time.Second}, false, nil }}
	a := &Aggregator{coordinator: c}
	ran, delay, err := a.pollCycle(t.Context(), "BMKG", time.Second, 1, func(context.Context, int64) error {
		t.Fatal("source called before shared cadence allowed it")
		return nil
	})
	if err != nil || ran || delay != time.Second || c.finishCalls != 0 {
		t.Fatalf("deferred: %v %v %v", ran, delay, err)
	}
}

func TestAmbiguousBeginRetiresEpochInsteadOfRenewingAbandonedWork(t *testing.T) {
	failure := errors.New("commit response unavailable")
	c := &pollCoordinator{begin: func() (store.PollAttempt, bool, error) { return store.PollAttempt{}, false, failure }}
	a := &Aggregator{coordinator: c, config: config.Config{CoordinationTimeout: time.Second}}
	_, _, err := a.pollCycle(t.Context(), "BMKG", time.Second, 1, func(context.Context, int64) error { t.Fatal("unconfirmed begin sent HTTP"); return nil })
	if !errors.Is(err, failure) || c.released != 1 {
		t.Fatalf("abandoned epoch retained: %v releases=%d", err, c.released)
	}
}

func TestDurablePollIdentityReachesBothBMKGEndpoints(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("X-Correlation-ID") != "poll-BMKG-7" || r.Header.Get("X-Poll-ID") != "7" || r.Header.Get("X-Poll-Holder") != "owner" || r.Header.Get("X-Poll-Epoch") != "3" {
			t.Error("missing durable polling identity")
		}
		_, _ = io.WriteString(w, "[]")
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &pollCoordinator{begin: func() (store.PollAttempt, bool, error) {
		return store.PollAttempt{ID: 7, Source: "BMKG", Holder: "owner", Token: 3}, true, nil
	}}
	cfg := config.Config{BMKGURL: server.URL, InstanceID: "owner", LeaseTTL: 15 * time.Second, CoordinationTimeout: time.Second}
	a := &Aggregator{coordinator: c, config: cfg, logger: logger}
	p := New(NewClient(time.Second, logger), cfg, logger)
	ran, _, err := a.pollCycle(t.Context(), "BMKG", time.Second, 3, func(ctx context.Context, _ int64) error { _, err := p.PollBMKG(ctx, time.Unix(0, 0)); return err })
	if err != nil || !ran || requests.Load() != 2 || c.validations.Load() != 2 || c.finishCalls != 1 || c.finished != nil {
		t.Fatalf("cycle not fully guarded: requests=%d validations=%d finish=%d error=%v", requests.Load(), c.validations.Load(), c.finishCalls, err)
	}
}

func TestLeaseTransportRechecksCancellationAfterValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx = context.WithValue(ctx, requestGuardKey{}, requestGuard(func(context.Context) error { cancel(); return nil }))
	upstream := &countingTransport{}
	r, _ := http.NewRequestWithContext(ctx, "GET", "http://upstream/test", nil)
	if _, err := (leaseTransport{next: upstream}).RoundTrip(r); !errors.Is(err, context.Canceled) || upstream.calls != 0 {
		t.Fatalf("canceled guard sent HTTP: %v", err)
	}
}

func TestSourceClientDoesNotFollowRedirectsOrReuseConnections(t *testing.T) {
	var redirected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/target", 302)
			return
		}
		redirected.Add(1)
	}))
	defer server.Close()
	client := sourceHTTPClient(time.Second)
	response, err := client.Get(server.URL + "/start")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 302 || redirected.Load() != 0 {
		t.Fatal("source redirect silently issued extra HTTP")
	}
	transport := client.Transport.(leaseTransport).next.(*http.Transport)
	if !transport.DisableKeepAlives || transport.Protocols.HTTP2() {
		t.Fatal("source transport allows hidden reused-connection replay")
	}
}

func (c *renewalCoordinator) RenewLease(context.Context, string, string, int64, time.Duration) error {
	c.renewals++
	if c.renewals == 3 {
		return store.ErrLeaseLost
	}
	return nil
}

func TestHeartbeatContinuesDuringBatchAndCancelsOnLostEpoch(t *testing.T) {
	coordinator := &renewalCoordinator{}
	a := &Aggregator{coordinator: coordinator, config: config.Config{LeaseTTL: 30 * time.Millisecond, InstanceID: "pod"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, stop := a.leaseContext(t.Context(), "BMKG", 7)
	defer stop()
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), store.ErrLeaseLost) {
			t.Fatalf("lost cause: %v", context.Cause(ctx))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch was not canceled after lease loss")
	}
}

type countingTransport struct{ calls int }

func (t *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls++
	return &http.Response{StatusCode: 200, Body: io.NopCloser(http.NoBody), Header: make(http.Header)}, nil
}

func TestLeaseTransportRechecksEachRequestAndBlocksStaleUpstreamCall(t *testing.T) {
	validations := 0
	guard := requestGuard(func(context.Context) error {
		validations++
		if validations > 1 {
			return store.ErrLeaseLost
		}
		return nil
	})
	ctx := context.WithValue(t.Context(), requestGuardKey{}, guard)
	upstream := &countingTransport{}
	client := &http.Client{Transport: leaseTransport{next: upstream}}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://bmkg/seismic-events", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "http://bmkg/tsunami-warnings", nil)
	if _, err := client.Do(request); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale second request: %v", err)
	}
	if upstream.calls != 1 {
		t.Fatalf("upstream received %d requests", upstream.calls)
	}
}
