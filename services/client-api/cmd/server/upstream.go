package main

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

type gatewayReadTransport struct {
	regular   *http.Transport
	analytics *http.Transport
}

func newAggregatorClient(maxInFlight int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 250 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = 2 * time.Second
	transport.MaxIdleConns = maxInFlight
	transport.MaxIdleConnsPerHost = maxInFlight
	transport.MaxConnsPerHost = maxInFlight
	analytics := transport.Clone()
	// Analytical scans retain their existing overall 30-second client budget.
	analytics.ResponseHeaderTimeout = 25 * time.Second
	return &http.Client{Transport: &gatewayReadTransport{transport, analytics}, Timeout: 30 * time.Second}
}

func (t *gatewayReadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	transport := t.regular
	if r.URL.Path == "/internal/analytics" {
		transport = t.analytics
	}
	response, err := transport.RoundTrip(r)
	var network net.Error
	if err == nil || r.Context().Err() != nil ||
		(r.Method != http.MethodGet && r.Method != http.MethodHead) ||
		(r.Body != nil && r.Body != http.NoBody) || !errors.As(err, &network) {
		return response, err
	}
	// No response headers were received. Only side-effect-free reads are
	// replayable; never replay writes, application responses or partial bodies.
	// Reconnect so headless Service DNS can try another gateway pod address.
	transport.CloseIdleConnections()
	slog.Warn("client_gateway_read_failover", "correlation_id", r.Header.Get("X-Correlation-ID"),
		"path", r.URL.Path, "error", err)
	return transport.RoundTrip(r.Clone(r.Context()))
}

func (t *gatewayReadTransport) CloseIdleConnections() {
	t.regular.CloseIdleConnections()
	t.analytics.CloseIdleConnections()
}
