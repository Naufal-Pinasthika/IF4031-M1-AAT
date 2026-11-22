package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAggregatorClientRecoversFromSilentGatewayBeforeCallerTimeout(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		if r.Header.Get("X-Correlation-ID") != "same-request" || r.URL.RawQuery != "limit=20" {
			t.Error("failover changed request identity or query")
		}
		_, _ = io.WriteString(w, `{"events":[],"partial":false}`)
	}))
	defer upstream.Close()
	client := newAggregatorClient(4)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL+"/internal/events?limit=20", nil)
	r.Header.Set("X-Correlation-ID", "same-request")
	began := time.Now()
	response, err := client.Do(r)
	if err != nil {
		t.Fatalf("safe read did not fail over before the caller deadline: %v", err)
	}
	defer response.Body.Close()
	if calls.Load() != 2 || time.Since(began) > 3*time.Second {
		t.Fatalf("calls=%d elapsed=%v", calls.Load(), time.Since(began))
	}
}

func TestAggregatorFailoverDoesNotReplayWritesBodiesOrCanceledReads(t *testing.T) {
	for _, kind := range []string{"write", "read-body", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
			}))
			defer upstream.Close()
			client := newAggregatorClient(4)
			defer client.CloseIdleConnections()
			client.Transport.(*gatewayReadTransport).regular.ResponseHeaderTimeout = 50 * time.Millisecond
			method, body := http.MethodGet, io.Reader(nil)
			if kind == "write" {
				method = http.MethodPost
			}
			if kind != "canceled" {
				body = strings.NewReader(`{"report":"test"}`)
			}
			ctx := t.Context()
			if kind == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			r, _ := http.NewRequestWithContext(ctx, method, upstream.URL+"/internal/events", body)
			r.Header.Set("Idempotency-Key", "must-not-trigger-write-replay")
			if _, err := client.Do(r); err == nil || calls.Load() != 1 {
				t.Fatalf("calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestAggregatorFailoverPreservesResponsesAndDoesNotReplayPartialBodies(t *testing.T) {
	for _, status := range []int{200, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Length", "100")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "{")
			}))
			defer upstream.Close()
			client := newAggregatorClient(4)
			defer client.CloseIdleConnections()
			response, err := client.Get(upstream.URL + "/internal/events")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if _, err := io.ReadAll(response.Body); err == nil {
				t.Fatal("partial body was not exposed to the caller")
			}
			if calls.Load() != 1 || response.StatusCode != status || response.Header.Get("Retry-After") != "1" {
				t.Fatalf("replayed or changed application response: calls=%d status=%d", calls.Load(), response.StatusCode)
			}
		})
	}
}

func TestAnalyticsRetainsLongerHeaderBudget(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, "{}")
	}))
	defer upstream.Close()
	client := newAggregatorClient(4)
	defer client.CloseIdleConnections()
	transport := client.Transport.(*gatewayReadTransport)
	transport.regular.ResponseHeaderTimeout = 10 * time.Millisecond
	response, err := client.Get(upstream.URL + "/internal/analytics")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
}

func TestReadRetrySelectsDifferentResolvedGateway(t *testing.T) {
	var badCalls, goodCalls atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { badCalls.Add(1); <-r.Context().Done() }))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodCalls.Add(1)
		if r.Host != "gateways.test:8083" || r.URL.RawQuery != "limit=20" || r.Header.Get("X-Correlation-ID") != "avoid-dead-gateway" {
			t.Error("retry changed logical target or request identity")
		}
		io.WriteString(w, "survivor")
	}))
	defer good.Close()
	client := newAggregatorClient(4)
	defer client.CloseIdleConnections()
	transport := client.Transport.(*gatewayReadTransport)
	transport.regular.ResponseHeaderTimeout = 30 * time.Millisecond
	transport.lookup = func(ctx context.Context, name string) ([]string, error) {
		return []string{"127.0.0.1", "127.0.0.2"}, nil
	}
	transport.regular.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		target := strings.TrimPrefix(bad.URL, "http://")
		if strings.HasPrefix(address, "127.0.0.2:") {
			target = strings.TrimPrefix(good.URL, "http://")
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}
	r, _ := http.NewRequestWithContext(t.Context(), "GET", "http://gateways.test:8083/internal/events?limit=20", nil)
	r.Header.Set("X-Correlation-ID", "avoid-dead-gateway")
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if badCalls.Load() != 1 || goodCalls.Load() != 1 {
		t.Fatalf("retry returned to failed address: bad=%d good=%d", badCalls.Load(), goodCalls.Load())
	}
}
