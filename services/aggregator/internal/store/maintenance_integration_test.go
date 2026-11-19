package store

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/aggregator/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A local, test-owned TCP fault boundary around the isolated PostgreSQL
// fixture. It never targets application databases or Kubernetes resources.
type databaseProxy struct {
	mu          sync.Mutex
	listener    net.Listener
	upstream    string
	online      bool
	connections map[net.Conn]bool
	workers     sync.WaitGroup
}

func newDatabaseProxy(t *testing.T, databaseURL string) (*databaseProxy, string) {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &databaseProxy{listener: listener, upstream: parsed.Host, online: true, connections: make(map[net.Conn]bool)}
	p.workers.Go(func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if !p.online {
				p.mu.Unlock()
				client.Close()
				continue
			}
			p.connections[client] = true
			p.mu.Unlock()
			p.workers.Go(func() {
				defer client.Close()
				defer func() { p.mu.Lock(); delete(p.connections, client); p.mu.Unlock() }()
				server, err := net.DialTimeout("tcp", p.upstream, time.Second)
				if err != nil {
					return
				}
				defer server.Close()
				p.mu.Lock()
				if !p.online {
					p.mu.Unlock()
					return
				}
				p.connections[server] = true
				p.mu.Unlock()
				defer func() { p.mu.Lock(); delete(p.connections, server); p.mu.Unlock() }()
				done := make(chan struct{})
				go func() { defer close(done); io.Copy(server, client); server.Close() }()
				io.Copy(client, server)
				client.Close()
				<-done
			})
		}
	})
	t.Cleanup(func() { listener.Close(); p.setOnline(false); p.workers.Wait() })
	parsed.Host = listener.Addr().String()
	return p, parsed.String()
}

func (p *databaseProxy) setOnline(online bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.online = online
	if !online {
		for conn := range p.connections {
			conn.Close()
		}
	}
}

func TestPostgresNetworkOutageIsPartialAndRecoversWithoutRepairingReadyVolume(t *testing.T) {
	_, databaseURL := integrationCoordinator(t)
	proxy, proxyURL := newDatabaseProxy(t, databaseURL)
	s, err := New(t.Context(), []config.StorageUnit{{Name: "store-a", DatabaseURL: proxyURL}}, 500*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(t.Context(), &e, 10); err != nil {
		t.Fatal(err)
	}
	// Pre-open the independent observation connection before the fault.
	s.refreshUnitStates(t.Context())
	proxy.setOnline(false)
	s.refreshUnitStates(t.Context())
	if !s.readStatus().Partial {
		t.Fatal("real outage remained globally healthy")
	}
	page, err := s.ListEvents(t.Context(), EventFilter{Limit: 10})
	if err == nil && !page.Partial {
		t.Fatal("unavailable database reported complete")
	}
	proxy.setOnline(true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.refreshUnitStates(t.Context())
		if !s.readStatus().Partial {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restored ready volume did not recover")
		}
		time.Sleep(10 * time.Millisecond)
	}
	page, err = s.ListEvents(t.Context(), EventFilter{Limit: 10})
	if err != nil || page.Partial || len(page.Events) != 1 || page.Events[0].HazardID != e.HazardID {
		t.Fatalf("data did not return after outage: %+v %v", page, err)
	}
	var status string
	if err := s.units[0].pool.QueryRow(t.Context(), "SELECT status FROM p1_metadata WHERE singleton=true").Scan(&status); err != nil || status != "ready" {
		t.Fatalf("network recovery changed durable readiness: %s %v", status, err)
	}
}

func TestPostgresMaintenanceBudgetDoesNotConsumeLiveOrProbePools(t *testing.T) {
	_, databaseURL := integrationCoordinator(t)
	parsed, _ := url.Parse(databaseURL)
	query := parsed.Query()
	query.Set("pool_max_conns", "4")
	parsed.RawQuery = query.Encode()
	s, err := New(t.Context(), []config.StorageUnit{{Name: "store-a", DatabaseURL: parsed.String()}}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	u := s.units[0]
	if u.maintenance == nil || u.pool.Config().MaxConns+u.maintenance.Config().MaxConns != 4 {
		t.Fatal("maintenance pool increased the original connection ceiling")
	}
	held, err := u.maintenance.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(t.Context(), &e, 10); err != nil {
		t.Fatalf("repair blocked live projection: %v", err)
	}
	page, err := s.ListEvents(t.Context(), EventFilter{Limit: 10})
	if err != nil || page.Partial || len(page.Events) != 1 {
		t.Fatalf("repair blocked read: %+v %v", page, err)
	}
	s.refreshUnitStates(t.Context())
	if s.readStatus().Partial {
		t.Fatal("repair blocked independent probe")
	}
	ctx, cancel := context.WithTimeout(maintenanceContext(t.Context()), 50*time.Millisecond)
	defer cancel()
	_, err = s.copyBatch(ctx, u, 0, "")
	if err == nil || !budgetError(err) {
		t.Fatalf("maintenance escaped its own pool: %v", err)
	}
	// Ordinary reads cannot use the reserved maintenance connection either.
	var regular []*pgxpool.Conn
	defer func() {
		for _, conn := range regular {
			conn.Release()
		}
	}()
	for range u.pool.Config().MaxConns {
		conn, err := u.pool.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		regular = append(regular, conn)
	}
	held.Release()
	ctx2, cancel2 := context.WithTimeout(maintenanceContext(t.Context()), time.Second)
	defer cancel2()
	if _, err = s.copyBatch(ctx2, u, 0, ""); err != nil {
		t.Fatalf("regular load consumed maintenance reserve: %v", err)
	}
}

func TestPostgresRecoveryDoesNotRecopyDurablyReadyVolumeAfterLocalFailure(t *testing.T) {
	c, databaseURL := integrationCoordinator(t)
	s, err := New(t.Context(), []config.StorageUnit{{Name: "store-a", DatabaseURL: databaseURL}}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := testHazard()
	e.IngestedAt = time.Now().UTC()
	if err := s.UpsertAccepted(t.Context(), &e, 10); err != nil {
		t.Fatal(err)
	}
	// Turn an unnecessary full repair into an unmistakable fixture failure.
	_, err = c.pool.Exec(t.Context(), `CREATE FUNCTION reject_unnecessary_recovery() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.status='recovering' THEN RAISE EXCEPTION 'ready volume must not be recopied'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER protect_ready_volume BEFORE UPDATE ON p1_metadata FOR EACH ROW EXECUTE FUNCTION reject_unnecessary_recovery()`)
	if err != nil {
		t.Fatal(err)
	}
	s.markDown(s.units[0]) // stale local observation; persistent data is intact
	s.recoverCycle(maintenanceContext(t.Context()))
	if s.readStatus().Partial || s.units[0].copied != 0 {
		t.Fatal("local failure caused a full copy or stranded a durably ready volume")
	}
	page, err := s.ListEvents(t.Context(), EventFilter{Limit: 10})
	if err != nil || page.Partial || len(page.Events) != 1 || page.Events[0].HazardID != e.HazardID {
		t.Fatalf("ready data did not remain available: %+v %v", page, err)
	}
}
