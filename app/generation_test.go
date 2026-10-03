package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/runtime"
)

// newRetirementApp boots an App with generation retirement enabled, using the
// existing newBuild harness (no poison port) and returns the app + config path.
func newRetirementApp(t *testing.T, maxRetired int, drainTO time.Duration, port int) (*App, string, *orderTracker) {
	t.Helper()
	path := writeAppConfig(t, "svc", port, "pw")
	tracker := &orderTracker{}
	a := New(config.NewLoader(appSchema(), path, "APP"),
		newBuild(t, -1, tracker),
		WithGenerationRetirement(maxRetired, drainTO))
	if err := a.Boot(context.Background()); err != nil {
		t.Fatalf("Boot: %v", err)
	}
	return a, path, tracker
}

func rewrite(t *testing.T, path string, port int) {
	t.Helper()
	content := fmt.Sprintf(`{"service.name":"svc","db.password":"pw-%d","server.port":%d}`, port, port)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAppInFlightLeaseSurvivesReload is the end-to-end version of the core
// requirement: an in-flight user keeps the entire old generation; a new user
// immediately sees the new one; old plugins close only after Release.
func TestAppInFlightLeaseSurvivesReload(t *testing.T) {
	a, path, _ := newRetirementApp(t, 2, 0, 8080)

	lease, err := a.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	oldSnap := lease.Snapshot()
	oldB := oldSnap.Plugins[1].(*lifecyclePlugin)
	log.Printf("INPUT in-flight user pinned generation v%d port=8080", lease.GenerationID())

	rewrite(t, path, 9090)
	if err := a.Reload(context.Background()); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// New user sees the new generation immediately.
	lease2, err := a.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if got := lease2.Snapshot().Config.GetInt("server.port"); got != 9090 {
		t.Fatalf("new user port=%d, want 9090", got)
	}
	if lease2.GenerationID() <= lease.GenerationID() {
		t.Fatalf("new user must get a newer generation: %d vs %d",
			lease2.GenerationID(), lease.GenerationID())
	}

	// Old user still operates on a complete old generation.
	if got := oldSnap.Config.GetInt("server.port"); got != 8080 {
		t.Fatalf("in-flight snapshot mutated to port=%d", got)
	}
	if oldB.isClosed() {
		t.Fatal("old generation plugin closed while still in use")
	}
	st := a.RuntimeStatus()
	log.Printf("JUDGEMENT active=v%d retiring=%d (v%d leases=%d)",
		st.ActiveID, st.CapacityUsed, lease.GenerationID(), st.Retiring[0].LeasesInUse)
	if st.CapacityUsed != 1 || st.Retiring[0].LeasesInUse != 1 {
		t.Fatalf("expected exactly one retiring generation with 1 lease: %+v", st)
	}

	lease.Release()
	if !oldB.isClosed() {
		t.Fatal("old plugin must be closed after the in-flight user releases")
	}
	st = a.RuntimeStatus()
	if st.CapacityUsed != 0 {
		t.Fatalf("no retiring generations expected after release: %+v", st)
	}
	r := st.RecentReclaims[len(st.RecentReclaims)-1]
	if r.Outcome != runtime.ReclaimDrained || r.CloseErr != nil {
		t.Fatalf("old generation should drain cleanly: %+v", r)
	}
	lease2.Release()
	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestAppCapacityRejectionEndToEnd verifies the capacity wall at the facade:
// a refused reload returns the distinguishable ReloadCapacityExceeded, the
// current state and all held leases stay usable, and reloads work again once
// capacity frees.
func TestAppCapacityRejectionEndToEnd(t *testing.T) {
	a, path, _ := newRetirementApp(t, 1, 0, 8080)
	l1, _ := a.Acquire() // pins v1
	rewrite(t, path, 9090)
	if err := a.Reload(context.Background()); err != nil {
		t.Fatalf("first reload: %v", err)
	}
	rewrite(t, path, 9091)
	err := a.Reload(context.Background())
	var re *runtime.ReloadError
	if !errors.As(err, &re) || re.Kind != runtime.ReloadCapacityExceeded || !re.OldKept {
		t.Fatalf("expected ReloadCapacityExceeded old-kept, got %v", err)
	}
	log.Printf("JUDGEMENT over-capacity reload refused at app boundary: %v", err)

	// Active state still serves v2; held v1 still works; plugins open.
	cur, _ := a.Acquire()
	if got := cur.Snapshot().Config.GetInt("server.port"); got != 9090 {
		t.Fatalf("active state should stay 9090, got %d", got)
	}
	if got := l1.Snapshot().Config.GetInt("server.port"); got != 8080 {
		t.Fatalf("held state should stay 8080, got %d", got)
	}
	if l1.Snapshot().Plugins[1].(*lifecyclePlugin).isClosed() {
		t.Fatal("held old plugins must not be force-closed")
	}
	cur.Release()

	l1.Release() // frees capacity
	if err := a.Reload(context.Background()); err != nil {
		t.Fatalf("reload after capacity free: %v", err)
	}
	cur2, _ := a.Acquire()
	if got := cur2.Snapshot().Config.GetInt("server.port"); got != 9091 {
		t.Fatalf("after unblock, active port should be 9091, got %d", got)
	}
	log.Printf("JUDGEMENT reload unblocked after release, active port=%d",
		cur2.Snapshot().Config.GetInt("server.port"))
	cur2.Release()
	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestAppShutdownDrainAndForce covers both shutdown paths at the facade.
func TestAppShutdownDrainAndForce(t *testing.T) {
	t.Run("drains_in_flight", func(t *testing.T) {
		a, _, _ := newRetirementApp(t, 1, 5*time.Second, 8080)
		l, _ := a.Acquire()
		plug := l.Snapshot().Plugins[1].(*lifecyclePlugin)
		done := make(chan error, 1)
		go func() { done <- a.ShutdownCtx(context.Background()) }()
		for !a.RuntimeStatus().ShuttingDown {
			time.Sleep(time.Millisecond)
		}
		if plug.isClosed() {
			t.Fatal("plugin closed before the in-flight lease released")
		}
		log.Printf("INPUT shutdown waiting on 1 lease; releasing")
		l.Release()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown did not complete after drain")
		}
		if !plug.isClosed() {
			t.Fatal("plugin must be closed after drained shutdown")
		}
		found := false
		for _, r := range a.RuntimeStatus().RecentReclaims {
			if r.Outcome == runtime.ReclaimDrained {
				found = true
			}
		}
		if !found {
			t.Fatal("expected a drained reclaim record")
		}
	})

	t.Run("forces_after_timeout", func(t *testing.T) {
		a, _, _ := newRetirementApp(t, 1, 30*time.Millisecond, 8080)
		l, _ := a.Acquire()
		plug := l.Snapshot().Plugins[1].(*lifecyclePlugin)
		start := time.Now()
		if err := a.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		log.Printf("JUDGEMENT shutdown returned after %v despite held lease", time.Since(start))
		if !plug.isClosed() {
			t.Fatal("plugin must be force-closed after drain timeout")
		}
		found := false
		for _, r := range a.RuntimeStatus().RecentReclaims {
			if r.Outcome == runtime.ReclaimForcedShutdown {
				found = true
			}
		}
		if !found {
			t.Fatal("expected a forced_shutdown reclaim record")
		}
		l.Release() // late release: safe, no double-close
	})
}

// TestAppRetirementDisabledByDefault confirms backward compatibility:
// without WithGenerationRetirement, Acquire is unavailable and the legacy
// immediate-teardown behavior is preserved.
func TestAppRetirementDisabledByDefault(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "pw")
	a := New(config.NewLoader(appSchema(), path, "APP"), newBuild(t, -1, &orderTracker{}))
	if err := a.Boot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Acquire(); err == nil {
		t.Fatal("Acquire must fail when retirement is not enabled")
	} else {
		log.Printf("JUDGEMENT legacy mode rejects Acquire: %v", err)
	}
	if st := a.RuntimeStatus(); st.Capacity != 0 || st.ActiveID != -1 {
		t.Fatalf("legacy mode status must be zero-value, got %+v", st)
	}

	old := a.Snapshot()
	oldB := old.Plugins[1].(*lifecyclePlugin)
	rewrite(t, path, 9090)
	if err := a.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !oldB.isClosed() {
		t.Fatal("legacy mode must close the displaced generation immediately")
	}
	if got := a.Snapshot().Config.GetInt("server.port"); got != 9090 {
		t.Fatalf("new generation must be served, got %d", got)
	}
	if err := a.Shutdown(); err != nil {
		t.Fatal(err)
	}
}
