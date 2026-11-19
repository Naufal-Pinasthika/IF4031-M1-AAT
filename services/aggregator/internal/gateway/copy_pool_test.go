package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type discardResponse struct{ header http.Header }

func (w discardResponse) Header() http.Header         { return w.header }
func (w discardResponse) WriteHeader(int)             {}
func (w discardResponse) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkProxyCopy(b *testing.B) {
	payload := []byte(strings.Repeat("0123456789", 1024))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(payload) }))
	defer server.Close()
	for _, pooled := range []bool{false, true} {
		name := "unpooled"
		if pooled {
			name = "pooled"
		}
		b.Run(name, func(b *testing.B) {
			g := testGateway()
			backend := g.newBackend(strings.TrimPrefix(server.URL, "http://"))
			backend.proxy.BufferPool = nil
			if pooled {
				backend.proxy.BufferPool = newCopyPool()
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					r := httptest.NewRequest("GET", "/internal/events?limit=20", nil)
					backend.proxy.ServeHTTP(discardResponse{header: make(http.Header)}, r)
				}
			})
		})
	}
}

func TestCopyPoolRetentionAndBufferSizeAreBounded(t *testing.T) {
	p := newCopyPool()
	p.Put(make([]byte, 64<<10))
	if len(p.buffers) != 0 {
		t.Fatal("oversized buffer retained")
	}
	for range 300 {
		p.Put(make([]byte, 32<<10))
	}
	if len(p.buffers) != 256 {
		t.Fatal("pool exceeds retention limit")
	}
	if len(p.Get()) != 32<<10 {
		t.Fatal("wrong copy buffer size")
	}
}
