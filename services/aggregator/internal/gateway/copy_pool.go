package gateway

// ReverseProxy otherwise allocates a new 32-KiB copy buffer per response.
// Retention is strictly bounded at 8 MiB for the entire gateway, not per host.
// Misses allocate transient buffers; they never wait or limit throughput.
type copyPool struct{ buffers chan []byte }

func newCopyPool() *copyPool { return &copyPool{buffers: make(chan []byte, 256)} }
func (p *copyPool) Get() []byte {
	select {
	case b := <-p.buffers:
		return b
	default:
		return make([]byte, 32<<10)
	}
}
func (p *copyPool) Put(b []byte) {
	if cap(b) != 32<<10 {
		return
	}
	select {
	case p.buffers <- b[:32<<10]:
	default:
	}
}
