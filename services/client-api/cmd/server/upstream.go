package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

type gatewayReadTransport struct {
	regular   *http.Transport
	analytics *http.Transport
	lookup    func(context.Context, string) ([]string, error)
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
	return &http.Client{Transport: &gatewayReadTransport{regular: transport, analytics: analytics, lookup: net.DefaultResolver.LookupHost}, Timeout: 30 * time.Second}
}

func (t *gatewayReadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	transport := t.regular
	if r.URL.Path == "/internal/analytics" {
		transport = t.analytics
	}
	var peer atomic.Pointer[string]
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if address, _, err := net.SplitHostPort(info.Conn.RemoteAddr().String()); err == nil {
			peer.Store(&address)
		}
	}}
	response, err := transport.RoundTrip(r.Clone(httptrace.WithClientTrace(r.Context(), trace)))
	var network net.Error
	if err == nil || r.Context().Err() != nil ||
		(r.Method != http.MethodGet && r.Method != http.MethodHead) ||
		(r.Body != nil && r.Body != http.NoBody) || !errors.As(err, &network) {
		return response, err
	}
	// No response headers were received. Only side-effect-free reads are
	// replayable; never replay writes, application responses or partial bodies.
	// Pin a distinct resolved address when possible. Repeating a headless DNS
	// dial does not guarantee a different pod and can hit the same failed gateway.
	transport.CloseIdleConnections()
	retry := r.Clone(r.Context())
	failed := ""
	if address := peer.Load(); address != nil {
		failed = *address
	}
	var dial *net.OpError
	if failed == "" && errors.As(err, &dial) && dial.Addr != nil {
		failed, _, _ = net.SplitHostPort(dial.Addr.String())
	}
	if r.URL.Scheme == "http" && net.ParseIP(r.URL.Hostname()) == nil {
		lookupCtx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		addresses, lookupErr := t.lookup(lookupCtx, r.URL.Hostname())
		cancel()
		if lookupErr == nil && len(addresses) <= 32 {
			for _, address := range addresses {
				if net.ParseIP(address) != nil && address != failed {
					port := r.URL.Port()
					if port == "" {
						port = "80"
					}
					retry.URL.Host = net.JoinHostPort(address, port)
					break
				}
			}
		}
	}
	slog.Warn("client_gateway_read_failover", "correlation_id", r.Header.Get("X-Correlation-ID"),
		"path", r.URL.Path, "failed_peer", failed, "retry_target", retry.URL.Host, "error", err)
	return transport.RoundTrip(retry)
}

func (t *gatewayReadTransport) CloseIdleConnections() {
	t.regular.CloseIdleConnections()
	t.analytics.CloseIdleConnections()
}
