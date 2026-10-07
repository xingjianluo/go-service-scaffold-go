package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/runtime"
)

// probeFast bounds any facade operation that must stay non-blocking while a
// slow reload is preparing; the gated preparation is held far longer.
const probeFast = 200 * time.Millisecond

// rewriteConfig rewrites the booted config file to a new generation.
func rewriteConfig(t *testing.T, path, pw string, port int) {
	t.Helper()
	content := fmt.Sprintf(`{"service.name":"svc","db.password":%q,"server.port":%d}`, pw, port)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAppSlowReloadObservableAndNonBlocking drives the full Boot -> slow
// Reload -> serve -> Shutdown facade. Business guarantees verified end to end:
// during slow preparation RuntimeStatus returns immediately with
// Reloading=true while the old version still serves; new Acquire calls return
// instantly on the old generation and Release never hangs; an in-flight
// request completes on the old plugins; after publication new requests see the
// new generation; a canceled (timeout) preparation clears the flag and keeps
// the old state serving.
func TestAppSlowReloadObservableAndNonBlocking(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "pw1")

	var hold atomic.Pointer[chan struct{}]
	var started atomic.Pointer[chan struct{}]
	var gatePort atomic.Int64
	base := newBuild(t, -1, &orderTracker{})
	wrapped := func(ctx context.Context, c *config.Config) (*runtime.Snapshot, error) {
		port := c.GetInt("server.port")
		if int64(port) == gatePort.Load() {
			if s := started.Load(); s != nil {
				close(*s)
			}
			if h := hold.Load(); h != nil {
				select {
				case <-*h:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		return base(ctx, c)
	}

	a := NewWithOptions(config.NewLoader(appSchema(), path, "APP"), wrapped, Options{
		MaxRetainedGenerations: 4,
		DrainTimeout:           2 * time.Second,
		ShutdownWait:           time.Second,
	})
	if err := a.Boot(context.Background()); err != nil {
		t.Fatalf("Boot: %v", err)
	}

	// In-flight request on v1 before the reload.
	leaseA := a.Acquire()
	if leaseA == nil {
		t.Fatal("Acquire after boot must succeed")
	}
	oldB := leaseA.Snapshot().Plugins[1].(*lifecyclePlugin)

	// Prepare a slow reload to port 9090 (v2).
	h := make(chan struct{})
	s := make(chan struct{})
	hold.Store(&h)
	started.Store(&s)
	gatePort.Store(9090)
	rewriteConfig(t, path, "pw2", 9090)
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- a.Reload(context.Background()) }()
	<-s

	// 1) Runtime status observable immediately during preparation.
	stch := make(chan runtime.ManagerStatus, 1)
	go func() { stch <- a.RuntimeStatus() }()
	select {
	case st := <-stch:
		if !st.Reloading {
			t.Fatal("RuntimeStatus must report Reloading=true during preparation")
		}
		if st.ActiveVersion != 1 {
			t.Fatalf("old v1 must remain active during preparation, got v%d", st.ActiveVersion)
		}
		log.Printf("INPUT slow reload v1->v2 held preparing via facade\n"+
			"JUDGEMENT RuntimeStatus returned immediately reloading=%t active=v%d",
			st.Reloading, st.ActiveVersion)
	case <-time.After(probeFast):
		t.Fatal("RuntimeStatus blocked behind slow reload")
	}

	// 2) New requests are served instantly on the old generation; Snapshot()
	//    (best-effort read) also keeps showing the old port.
	if snap := a.Snapshot(); snap == nil || snap.Config.GetInt("server.port") != 8080 {
		t.Fatalf("Snapshot() must keep serving old 8080 during preparation, got %+v", snap)
	}
	for i := 0; i < 10; i++ {
		lch := make(chan *runtime.Lease, 1)
		go func() { lch <- a.Acquire() }()
		select {
		case l := <-lch:
			if port := l.Snapshot().Config.GetInt("server.port"); port != 8080 {
				t.Fatalf("new request during preparation must see old 8080, got %d", port)
			}
			rch := make(chan struct{})
			go func() { l.Release(); close(rch) }()
			select {
			case <-rch:
			case <-time.After(probeFast):
				t.Fatal("Release blocked behind slow reload")
			}
		case <-time.After(probeFast):
			t.Fatal("Acquire blocked behind slow reload")
		}
	}
	if oldB.isClosed() {
		t.Fatal("in-flight request A's old plugin must stay open during preparation")
	}
	log.Printf("JUDGEMENT 10 new requests served immediately on old 8080; A in flight; old plugin open=%t",
		!oldB.isClosed())

	// 3) Publish; new requests switch to 9090; A still finishes on old v1.
	close(h)
	if err := <-reloadDone; err != nil {
		t.Fatalf("slow reload should succeed after gate release: %v", err)
	}
	lb := a.Acquire()
	if port := lb.Snapshot().Config.GetInt("server.port"); port != 9090 {
		t.Fatalf("new request after publication must see 9090, got %d", port)
	}
	lb.Release()
	if port := leaseA.Snapshot().Config.GetInt("server.port"); port != 8080 {
		t.Fatalf("in-flight A must finish on old 8080, got %d", port)
	}
	leaseA.Release()
	if !waitClosed(oldB, 2*time.Second) {
		t.Fatal("old plugin must be closed after in-flight A released")
	}
	log.Printf("JUDGEMENT post-publication new requests on 9090; A finished on old 8080 and its plugin closed")

	// 4) A subsequent CANCELED (timeout) preparation must clear the flag and
	//    keep the currently served 9090 generation intact.
	h2 := make(chan struct{})
	s2 := make(chan struct{})
	hold.Store(&h2)
	started.Store(&s2)
	gatePort.Store(9091)
	rewriteConfig(t, path, "pw3", 9091)
	ctx, cancel := context.WithCancel(context.Background())
	timedDone := make(chan error, 1)
	go func() { timedDone <- a.Reload(ctx) }()
	<-s2
	if !a.RuntimeStatus().Reloading {
		t.Fatal("must report reloading during the second (canceled) preparation")
	}
	cancel()
	if err := <-timedDone; err == nil {
		t.Fatal("canceled reload must return an error")
	}
	st := a.RuntimeStatus()
	if st.Reloading {
		t.Fatal("Reloading flag must clear after a canceled reload")
	}
	if snap := a.Snapshot(); snap.Config.GetInt("server.port") != 9090 {
		t.Fatalf("canceled reload must keep serving 9090, got %d", snap.Config.GetInt("server.port"))
	}
	close(h2)
	log.Printf("JUDGEMENT canceled slow reload: flag cleared, served port still 9090, error returned")

	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// waitClosed polls a plugin until closed or the deadline.
func waitClosed(p *lifecyclePlugin, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if p.isClosed() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return p.isClosed()
}
