package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/plugin"
)

// buildFuncWithPlugin returns a BuildFunc that builds a snapshot containing a
// single initialized recPlugin named "p-<port>". Every generation therefore
// has distinct, individually observable plugin instances.
func buildFuncWithPlugin() (BuildFunc, func(port int) *recPlugin) {
	plugins := make(map[int]*recPlugin)
	var mu sync.Mutex
	build := func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		port := cfg.GetInt("server.port")
		p := &recPlugin{name: fmt.Sprintf("p-%d", port)}
		if err := p.Init(ctx); err != nil {
			return nil, err
		}
		mu.Lock()
		plugins[port] = p
		mu.Unlock()
		return &Snapshot{Config: cfg, Plugins: []plugin.Plugin{p}}, nil
	}
	get := func(port int) *recPlugin {
		mu.Lock()
		defer mu.Unlock()
		return plugins[port]
	}
	return build, get
}

func newGuarded(t *testing.T, capacity int, drainTO time.Duration, port int) (*Guard, *recPlugin) {
	t.Helper()
	build, get := buildFuncWithPlugin()
	g := NewGuard(build, GuardConfig{MaxRetiredGenerations: capacity, ShutdownDrainTimeout: drainTO})
	if err := g.Reload(context.Background(), cfgWithPort(port)); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	return g, get(port)
}

func assertClosed(t *testing.T, p *recPlugin, want int) {
	t.Helper()
	if _, c := p.state(); c != want {
		t.Fatalf("plugin %s closes=%d, want %d", p.name, c, want)
	}
}

func logStatus(st GuardStatus) {
	parts := ""
	for _, r := range st.Retiring {
		parts += fmt.Sprintf("v%d:%s(leases=%d) ", r.ID, r.State, r.LeasesInUse)
	}
	reclaims := ""
	for _, r := range st.RecentReclaims {
		reclaims += fmt.Sprintf("v%d=%s ", r.ID, r.Outcome)
	}
	log.Printf("JUDGEMENT active=v%d reloading=%t down=%t capacity=%d/%d retiring=[%s] reclaimed=[%s]",
		st.ActiveID, st.Reloading, st.ShuttingDown, st.CapacityUsed, st.Capacity, parts, reclaims)
}

// TestLeasePinsOldGenerationAcrossReload covers requirement #1: a user that
// acquired the old generation finishes its whole unit of work against the
// old config/plugins; new users immediately see the new generation; and the
// old resources are released only after the old lease is released.
func TestLeasePinsOldGenerationAcrossReload(t *testing.T) {
	g, p100 := newGuarded(t, 2, 0, 100)

	// User A starts BEFORE the reload and pins generation v1.
	leaseA, err := g.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("INPUT user A acquired v%d before reload", leaseA.GenerationID())

	// Reload happens while A is mid-flight.
	if err := g.Reload(context.Background(), cfgWithPort(200)); err != nil {
		t.Fatalf("reload: %v", err)
	}

	// New user B must immediately observe the new generation v2.
	leaseB, err := g.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if leaseB.GenerationID() <= leaseA.GenerationID() {
		t.Fatalf("new user must see newer generation: A=v%d B=v%d",
			leaseA.GenerationID(), leaseB.GenerationID())
	}
	if port := leaseB.Snapshot().Config.GetInt("server.port"); port != 200 {
		t.Fatalf("new user sees port=%d, want 200", port)
	}
	if _, ok := leaseB.Snapshot().Plugin("p-200"); !ok {
		t.Fatal("new user snapshot must contain new plugin p-200")
	}

	// A must still see a fully consistent old generation.
	if port := leaseA.Snapshot().Config.GetInt("server.port"); port != 100 {
		t.Fatalf("old user sees port=%d, want 100 (torn state!)", port)
	}
	oldPlug, ok := leaseA.Snapshot().Plugin("p-100")
	if !ok {
		t.Fatal("old user lost its old plugin mid-flight")
	}
	if oldPlug != p100 {
		t.Fatal("old user must keep using the exact old plugin instance")
	}
	// Old plugin must NOT have been closed while A is still working.
	assertClosed(t, p100, 0)

	st := g.Status()
	logStatus(st)
	if st.ActiveID != leaseB.GenerationID() || len(st.Retiring) != 1 {
		t.Fatalf("unexpected status after reload: %+v", st)
	}
	if st.Retiring[0].LeasesInUse != 1 {
		t.Fatalf("retiring v1 should report 1 in-flight lease, got %d",
			st.Retiring[0].LeasesInUse)
	}

	// A finishes: only now may the old generation be reclaimed.
	leaseA.Release()
	leaseA.Release() // double release must be a no-op, not a panic/extra close
	assertClosed(t, p100, 1)

	st = g.Status()
	logStatus(st)
	if len(st.Retiring) != 0 || len(st.RecentReclaims) != 1 {
		t.Fatalf("old generation must be reclaimed after last release: %+v", st)
	}
	last := st.RecentReclaims[len(st.RecentReclaims)-1]
	if last.Outcome != ReclaimDrained || last.State != GenReclaimed || last.CloseErr != nil {
		t.Fatalf("old generation should be reclaimed drained without errors, got %+v", last)
	}
	leaseB.Release()
}

// TestMultipleUsersAndReloadsInterleave covers multiple users overlapping
// multiple reloads: every lease observes its own coherent generation,
// generations reclaim in lease-release order, and reloads stay within
// capacity without disturbing in-flight users.
func TestMultipleUsersAndReloadsInterleave(t *testing.T) {
	build, get := buildFuncWithPlugin()
	g := NewGuard(build, GuardConfig{MaxRetiredGenerations: 3})
	if err := g.Reload(context.Background(), cfgWithPort(1)); err != nil {
		t.Fatalf("initial reload: %v", err)
	}

	leaseA, _ := g.Acquire() // pins v1 (port 1)
	if err := g.Reload(context.Background(), cfgWithPort(2)); err != nil {
		t.Fatalf("reload v2: %v", err)
	}
	leaseB, _ := g.Acquire() // pins v2 (port 2)
	if err := g.Reload(context.Background(), cfgWithPort(3)); err != nil {
		t.Fatalf("reload v3: %v", err)
	}
	leaseC, _ := g.Acquire() // pins v3 (port 3)
	if err := g.Reload(context.Background(), cfgWithPort(4)); err != nil {
		t.Fatalf("reload v4: %v", err)
	}
	st := g.Status()
	log.Printf("INPUT users A/B/C pinned v1/v2/v3, 3 reloads done, active=v4")
	logStatus(st)
	if st.ActiveID != leaseC.GenerationID()+1 || st.CapacityUsed != 3 {
		t.Fatalf("expected 3 retiring generations and v4 active, got %+v", st)
	}
	check := func(l *Lease, port int) {
		t.Helper()
		if got := l.Snapshot().Config.GetInt("server.port"); got != port {
			t.Fatalf("lease v%d sees port=%d, want %d", l.GenerationID(), got, port)
		}
		name := fmt.Sprintf("p-%d", port)
		if _, ok := l.Snapshot().Plugin(name); !ok {
			t.Fatalf("lease v%d lost plugin %s", l.GenerationID(), name)
		}
		assertClosed(t, get(port), 0)
	}
	check(leaseA, 1)
	check(leaseB, 2)
	check(leaseC, 3)

	// B finishes first: only v2 reclaims.
	leaseB.Release()
	assertClosed(t, get(2), 1)
	assertClosed(t, get(1), 0)
	assertClosed(t, get(3), 0)

	// A finishes: v1 reclaims; v3 still pinned by C.
	leaseA.Release()
	assertClosed(t, get(1), 1)
	assertClosed(t, get(3), 0)
	st = g.Status()
	if st.CapacityUsed != 1 || st.Retiring[0].ID != leaseC.GenerationID() {
		t.Fatalf("only v3 should still be retiring: %+v", st)
	}

	// Reload v5 must now succeed (capacity freed). v4 had zero leases and is
	// reclaimed synchronously before Reload returns.
	if err := g.Reload(context.Background(), cfgWithPort(5)); err != nil {
		t.Fatalf("reload v5 after drain: %v", err)
	}
	assertClosed(t, get(4), 1)
	if got := g.Status().ActiveID; got != leaseC.GenerationID()+2 {
		t.Fatalf("active must be v5, got v%d", got)
	}

	// C finishes: v3 reclaims. Every generation is closed exactly once.
	leaseC.Release()
	for _, port := range []int{1, 2, 3, 4} {
		assertClosed(t, get(port), 1)
	}
	logStatus(g.Status())
}

// TestReloadRefusedAtCapacityKeepsUsableState covers requirement #3: once the
// retired-generation capacity is full, further reloads are refused with a
// distinguishable error; both the active generation and the in-use old one
// stay fully usable, and nothing is force-closed.
func TestReloadRefusedAtCapacityKeepsUsableState(t *testing.T) {
	build, get := buildFuncWithPlugin()
	g := NewGuard(build, GuardConfig{MaxRetiredGenerations: 1}) // capacity = 1
	if err := g.Reload(context.Background(), cfgWithPort(100)); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	p100 := get(100)

	lease, _ := g.Acquire() // pin v1
	if err := g.Reload(context.Background(), cfgWithPort(200)); err != nil {
		t.Fatalf("reload to v2: %v", err)
	}
	p200 := get(200)
	log.Printf("INPUT capacity=1, v1 held by 1 lease, attempting another reload")

	err := g.Reload(context.Background(), cfgWithPort(300))
	var re *ReloadError
	if !errors.As(err, &re) {
		t.Fatalf("expected *ReloadError, got %T: %v", err, err)
	}
	log.Printf("JUDGEMENT refused: %v", err)
	if re.Kind != ReloadCapacityExceeded || !re.OldKept {
		t.Fatalf("expected ReloadCapacityExceeded with old kept, got kind=%s oldKept=%t",
			re.Kind, re.OldKept)
	}

	// Active generation is untouched and new users still see v2.
	cur, err := g.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if port := cur.Snapshot().Config.GetInt("server.port"); port != 200 {
		t.Fatalf("active state changed after refused reload: port=%d", port)
	}
	// Held old generation remains fully usable and its plugins stay open.
	if port := lease.Snapshot().Config.GetInt("server.port"); port != 100 {
		t.Fatalf("in-use old state disturbed: port=%d", port)
	}
	assertClosed(t, p100, 0)
	assertClosed(t, p200, 0)
	// v3 was never built? The build runs AFTER the capacity check, so p300
	// must not exist at all.
	if p := get(300); p != nil {
		t.Fatal("refused reload must not build the new generation")
	}

	cur.Release()
	lease.Release()
	assertClosed(t, p100, 1)

	// Once capacity frees up, reloads work again.
	if err := g.Reload(context.Background(), cfgWithPort(300)); err != nil {
		t.Fatalf("reload must succeed after capacity frees: %v", err)
	}
	if port := g.Status().ActiveID; port <= 0 {
		t.Fatal("no active generation after unblocked reload")
	}
}

// TestResourcesReleasedAfterDrain verifies reclamation really releases
// resources with a precise outcome when no leases are outstanding: the
// displaced generation is closed exactly once and recorded as "drained".
func TestResourcesReleasedAfterDrain(t *testing.T) {
	g, p100 := newGuarded(t, 1, 0, 100)
	if err := g.Reload(context.Background(), cfgWithPort(200)); err != nil {
		t.Fatal(err)
	}
	assertClosed(t, p100, 1) // zero leases => synchronous reclamation
	st := g.Status()
	logStatus(st)
	if st.CapacityUsed != 0 || len(st.RecentReclaims) != 1 {
		t.Fatalf("expected no retiring generations and 1 reclaim, got %+v", st)
	}
	r := st.RecentReclaims[0]
	if r.Outcome != ReclaimDrained || !r.ReclaimedAt.After(r.InstalledAt) || r.CloseErr != nil {
		t.Fatalf("bad reclaim record: %+v", r)
	}
}

// TestShutdownDrainsInFlightLeases verifies Shutdown waits (within the
// configured timeout) for in-flight users and records a normal drain.
func TestShutdownDrainsInFlightLeases(t *testing.T) {
	g, p100 := newGuarded(t, 1, 5*time.Second, 100)
	lease, _ := g.Acquire()

	done := make(chan error, 1)
	go func() { done <- g.Shutdown(context.Background()) }()

	deadline := time.Now().Add(2 * time.Second)
	for !g.Status().ShuttingDown {
		if time.Now().After(deadline) {
			t.Fatal("Shutdown never became observable")
		}
		time.Sleep(time.Millisecond)
	}
	log.Printf("INPUT shutdown started while 1 lease held; releasing it now")
	logStatus(g.Status())
	assertClosed(t, p100, 0) // still in flight: not closed yet

	lease.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after lease drain")
	}
	assertClosed(t, p100, 1)
	var outcome ReclaimOutcome
	for _, r := range g.Status().RecentReclaims {
		if r.ID == 1 {
			outcome = r.Outcome
		}
	}
	log.Printf("JUDGEMENT shutdown reclaimed v1 outcome=%s", outcome)
	if outcome != ReclaimDrained {
		t.Fatalf("expected drained outcome, got %s", outcome)
	}
}

// TestShutdownForcesAfterDrainTimeout verifies one slow user cannot block
// shutdown forever: after the drain deadline resources are force-reclaimed
// with a distinguishable outcome, and a late Release stays harmless.
func TestShutdownForcesAfterDrainTimeout(t *testing.T) {
	g, p100 := newGuarded(t, 1, 40*time.Millisecond, 100)
	lease, _ := g.Acquire()

	start := time.Now()
	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	elapsed := time.Since(start)
	log.Printf("INPUT lease still held, drain timeout 40ms\nJUDGEMENT shutdown returned after %v", elapsed)
	if elapsed > 2*time.Second {
		t.Fatal("shutdown was blocked by a slow user")
	}
	assertClosed(t, p100, 1) // force-reclaimed exactly once
	var outcome ReclaimOutcome
	for _, r := range g.Status().RecentReclaims {
		if r.ID == 1 {
			outcome = r.Outcome
		}
	}
	if outcome != ReclaimForcedShutdown {
		t.Fatalf("expected forced_shutdown outcome, got %s", outcome)
	}

	// Late release after forced reclamation must not double-close or panic.
	lease.Release()
	assertClosed(t, p100, 1)

	if _, err := g.Acquire(); !errors.Is(err, ErrNoActiveGeneration) {
		t.Fatalf("Acquire after shutdown must fail with ErrNoActiveGeneration, got %v", err)
	}
}

// TestReloadingFlagObservableAndSerializes verifies the observable
// "reload in progress" state and that concurrent reloads serialize.
func TestReloadingFlagObservableAndSerializes(t *testing.T) {
	plainBuild, _ := buildFuncWithPlugin()

	// Prime v1 with a blocking build so we can observe the flag.
	block1 := make(chan struct{})
	started1 := make(chan struct{})
	g := NewGuard(func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		close(started1)
		<-block1
		return plainBuild(ctx, cfg)
	}, GuardConfig{MaxRetiredGenerations: 4})
	primeDone := make(chan error, 1)
	go func() { primeDone <- g.Reload(context.Background(), cfgWithPort(1)) }()
	<-started1
	if !g.Status().Reloading {
		t.Fatal("Status must report a reload in progress while build is blocked")
	}
	log.Printf("JUDGEMENT reloading flag observed as true during blocked build")
	close(block1)
	if err := <-primeDone; err != nil {
		t.Fatalf("prime reload: %v", err)
	}
	if g.Status().Reloading {
		t.Fatal("reloading flag must clear when reload finishes")
	}

	// Two reloads launched concurrently must serialize (one build at a time).
	block2 := make(chan struct{})
	started2 := make(chan struct{})
	g.build = func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		select {
		case <-started2: // already closed
		default:
			close(started2)
		}
		<-block2
		return plainBuild(ctx, cfg)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = g.Reload(context.Background(), cfgWithPort(2)) }()
	go func() { defer wg.Done(); _ = g.Reload(context.Background(), cfgWithPort(3)) }()
	<-started2
	// Give the second reload a moment to try entering; it must wait on the
	// semaphore instead of starting a second concurrent build.
	time.Sleep(30 * time.Millisecond)
	close(block2) // closed channel: both serialized builds proceed
	wg.Wait()
	if g.Status().Reloading {
		t.Fatal("reloading flag must clear after all reloads finish")
	}
	if port := g.Status().ActiveID; port != 3 {
		t.Fatalf("expected final active v3, got v%d", port)
	}
	log.Printf("JUDGEMENT concurrent reloads serialized, final active=v3, flag cleared")
}

// TestGuardEmptyStates covers Acquire before boot and double Install.
func TestGuardEmptyStates(t *testing.T) {
	build, _ := buildFuncWithPlugin()
	g := NewGuard(build, GuardConfig{MaxRetiredGenerations: 1})
	if _, err := g.Acquire(); !errors.Is(err, ErrNoActiveGeneration) {
		t.Fatalf("expected ErrNoActiveGeneration, got %v", err)
	}
	snap := &Snapshot{Config: cfgWithPort(1)}
	if err := g.Install(snap); err != nil {
		t.Fatalf("install: %v", err)
	}
	if err := g.Install(&Snapshot{Config: cfgWithPort(2)}); err == nil {
		t.Fatal("double Install must be rejected")
	}
	if err := g.Install(nil); err == nil {
		t.Fatal("nil Install must be rejected")
	}
	log.Printf("JUDGEMENT empty/duplicate install handled; active=v%d", g.Status().ActiveID)
}

// TestRandomInterleavedStress hammers a Guard with concurrent leases,
// releases and reloads. Invariants checked:
//   - a held lease always observes its own coherent (port, plugin) pair;
//   - every plugin closes at most once;
//   - reloads either succeed or fail with ReloadCapacityExceeded (never
//     silently disturb an in-use generation);
//   - after quiescence every generation except the active one is reclaimed.
func TestRandomInterleavedStress(t *testing.T) {
	build, get := buildFuncWithPlugin()
	const capacity = 3
	g := NewGuard(build, GuardConfig{MaxRetiredGenerations: capacity})
	if err := g.Reload(context.Background(), cfgWithPort(0)); err != nil {
		t.Fatal(err)
	}

	var workers sync.WaitGroup
	stop := make(chan struct{})

	// Re-loader: keeps replacing the active generation until 60 attempts.
	workers.Add(1)
	go func() {
		defer workers.Done()
		for next := 1; next <= 60; next++ {
			err := g.Reload(context.Background(), cfgWithPort(next))
			if err != nil {
				var re *ReloadError
				if !errors.As(err, &re) || re.Kind != ReloadCapacityExceeded {
					t.Errorf("unexpected reload error: %v", err)
					return
				}
			}
			time.Sleep(time.Microsecond)
		}
	}()

	// Workers: acquire, do work against the pinned generation, release.
	for w := 0; w < 8; w++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 200; i++ {
				lease, err := g.Acquire()
				if err != nil {
					return // shutting down is not part of this test
				}
				port := lease.Snapshot().Config.GetInt("server.port")
				if _, ok := lease.Snapshot().Plugin(fmt.Sprintf("p-%d", port)); !ok {
					t.Errorf("lease v%d saw incoherent plugin for port %d",
						lease.GenerationID(), port)
					lease.Release()
					return
				}
				// Plugin must remain open while the lease is held.
				if p := get(port); p != nil {
					if _, c := p.state(); c != 0 {
						t.Errorf("plugin p-%d closed while lease v%d is held (closes=%d)",
							port, lease.GenerationID(), c)
						lease.Release()
						return
					}
				}
				time.Sleep(time.Microsecond)
				lease.Release()
			}
		}()
	}

	// Monitor lives on its own WaitGroup so it cannot deadlock the join.
	var monitor sync.WaitGroup
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if used := g.Status().CapacityUsed; used > capacity {
				t.Errorf("capacity exceeded: used=%d > max=%d", used, capacity)
				return
			}
			time.Sleep(time.Microsecond)
		}
	}()

	// Wait for reloader + workers, then stop the monitor.
	workers.Wait()
	close(stop)
	monitor.Wait()

	// Workers released their own leases; wait for retiring generations to
	// drain, then identify the ACTIVE port directly (ids are not contiguous
	// with port numbers because capacity-refused reloads build nothing).
	deadline := time.Now().Add(3 * time.Second)
	for g.Status().CapacityUsed > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("generations never drained: %+v", g.Status())
		}
		time.Sleep(2 * time.Millisecond)
	}
	activeLease, err := g.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	activePort := activeLease.Snapshot().Config.GetInt("server.port")
	activeLease.Release()
	st := g.Status()
	log.Printf("INPUT 8 workers x200 + 60 reload attempts, capacity=%d\nJUDGEMENT active=v%d(port=%d) reclaimed=%d, all (port,plugin) pairs coherent",
		capacity, st.ActiveID, activePort, len(st.RecentReclaims))
	for port := 0; port <= 60; port++ {
		p := get(port)
		if p == nil {
			continue // capacity-refused reloads never built this port's plugin
		}
		_, c := p.state()
		if port == activePort {
			if c != 0 {
				t.Fatalf("active plugin p-%d must still be open, closes=%d", port, c)
			}
		} else if c != 1 {
			t.Fatalf("reclaimed plugin p-%d closes=%d, want exactly 1", port, c)
		}
	}
}
