package consumer

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestProbeStopsWhenBrokerNeverAnswersMetadata(t *testing.T) {
	for _, deadline := range []bool{true, false} {
		t.Run(map[bool]string{true: "deadline", false: "cancellation"}[deadline], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					accepted <- conn
				}
			}()
			ctx, cancel := context.WithCancel(t.Context())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
			}
			defer cancel()
			c := &Consumer{brokers: []string{listener.Addr().String()}, topic: "test"}
			done := make(chan error, 1)
			go func() { done <- c.Probe(ctx) }()
			select {
			case conn := <-accepted:
				defer conn.Close()
			case <-time.After(time.Second):
				t.Fatal("broker was not connected")
			}
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("unresponsive broker reported ready")
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("probe outlived cancellation/deadline")
			}
		})
	}
}
