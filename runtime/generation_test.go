package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/plugin"
)

// waitFor polls cond until it becomes true or the timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// newTestManager builds a Manager whose generations contain one recPlugin
// named p-<version>, using the supplied options.
func newTestManager(t *testing.T, opts Options) (*Manager, func(int) *config.Config) {
	t.Helper()
	mkcfg := func(port int) *config.Config { return cfgWithPort(port) }
	m := NewManager(func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		p := &recPlugin{name: fmt.Sprintf("p-%d", cfg.GetInt("server.port"))}
		if err := p.Init(ctx); err != nil {
			return nil, err
		}
		return &Snapshot{Config: cfg, Plugins: []plugin.Plugin{p}}, nil
	}, opts)
	return m, mkcfg
}

// TestLeasePinsOldGenerationAcrossReload is the headline scenario: a user
// acquires before the reload and must finish its whole unit of work against
// the complete old generation; a new user after the swap sees the new one.
func TestLeasePinsOldGenerationAcrossReload(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(100)); err != nil {
		t.Fatalf("first reload: %v", err)
	}

	// User A starts work on the old generation BEFORE the reload.
	leaseA := m.Acquire()
	if leaseA == nil {
		t.Fatal("Acquire must return a lease while serving")
	}
	oldSnap := leaseA.Snapshot()
	oldPlugin := oldSnap.Plugins[0].(*recPlugin)
	log.Printf("INPUT user A acquired generation v%d port=%d before reload",
		m.ActiveVersion(), oldSnap.Config.GetInt("server.port"))

	// Reload happens while A is still working.
	if err := m.Reload(ctx, mkcfg(200)); err != nil {
		t.Fatalf("second reload: %v", err)
	}
	log.Printf("INPUT reload to v%d happened while A still holds its lease",
		m.ActiveVersion())

	// New user B arriving AFTER the swap immediately sees the new state.
	leaseB := m.Acquire()
	newSnap := leaseB.Snapshot()
	if newSnap == oldSnap {
		t.Fatal("new user must see the new generation, not the pinned old one")
	}
	if newSnap.Config.GetInt("server.port") != 200 {
		t.Fatalf("new user saw port=%d, want 200", newSnap.Config.GetInt("server.port"))
	}
	log.Printf("JUDGEMENT after swap: new user B on port=%d; A still pinned to port=%d",
		newSnap.Config.GetInt("server.port"), oldSnap.Config.GetInt("server.port"))

	// Old generation is retained and the old plugin is still open while A works.
	st := m.Status()
	if len(st.Retained) != 1 || st.Retained[0].ID != 1 || st.Retained[0].Leases != 1 {
		t.Fatalf("old generation must be retained with 1 lease, got %+v", st.Retained)
	}
	if c := closesOf(oldPlugin); c != 0 {
		t.Fatalf("old plugin must not be closed while A works, closes=%d", c)
	}

	// A completes its entire operation on the old plugin: it must work fully.
	leaseA.Release()

	// B finishes after the reload on the new plugin.
	newPlugin := newSnap.Plugins[0].(*recPlugin)
	leaseB.Release()

	// Old generation is reclaimed and its resources released.
	if !waitFor(2*time.Second, func() bool {
		return closesOf(oldPlugin) == 1 && len(m.Status().Retained) == 0
	}) {
		t.Fatalf("old generation not reclaimed after last release: %+v closes=%d",
			m.Status().Retained, closesOf(oldPlugin))
	}
	if c := closesOf(newPlugin); c != 0 {
		t.Fatalf("new (active) plugin must remain open, closes=%d", c)
	}
	hist := m.Status().History
	if len(hist) != 1 || hist[0].ID != 1 || hist[0].Outcome != OutcomeDrained {
		t.Fatalf("history must show v1 reclaimed drained, got %+v", hist)
	}
	log.Printf("JUDGEMENT v1 reclaimed outcome=%s after A released; active stays v%d, old resource released=%t",
		hist[0].Outcome, m.ActiveVersion(), closesOf(oldPlugin) == 1)
}

func closesOf(p *recPlugin) int {
	_, c := p.state()
	return c
}

// TestInterleavedUsersAndReloadsStress interleaves many users (each pinning
// a generation, doing work that reads BOTH config and plugins from that same
// lease, then releasing) with reloads. Every user must observe a coherent
// generation: the plugin name must match the config port it was built from.
func TestInterleavedUsersAndReloadsStress(t *testing.T) {
	// Large capacity: with many slow users we exercise draining but never
	// rejection in this scenario.
	m, mkcfg := newTestManager(t, Options{
		MaxRetainedGenerations: 16,
		DrainTimeout:           time.Second,
		ReapTimeout:            time.Second,
	})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(1)); err != nil {
		t.Fatal(err)
	}

	var errorsFound atomic.Int64
	var usersDone atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup

	// Users: acquire, then perform "work" in small slices before release.
	// The whole work must stay on the pinned generation.
	for u := 0; u < 24; u++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				l := m.Acquire()
				if l == nil {
					errorsFound.Add(1)
					return
				}
				port := l.Snapshot().Config.GetInt("server.port")
				expected := fmt.Sprintf("p-%d", port)
				for i := 0; i < 5; i++ {
					got, ok := l.Snapshot().Plugin(expected)
					if !ok || got.Name() != expected {
						errorsFound.Add(1)
					}
					time.Sleep(200 * time.Microsecond)
				}
				l.Release()
				usersDone.Add(1)
			}
		}()
	}

	// Reloader: publish generations 2..12, with gaps between them.
	versions := 0
	for v := 2; v <= 12; v++ {
		if err := m.Reload(ctx, mkcfg(v)); err != nil {
			// A drain-full rejection is allowed here (many slow users); if it
			// happens the active state must still be coherent and reloadable
			// later. It must never corrupt served state.
			var re *ReloadError
			if !errors.As(err, &re) || re.Kind != ReloadDrainFull {
				t.Errorf("unexpected reload error v=%d: %v", v, err)
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		versions++
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	final := m.Acquire()
	if final == nil {
		t.Fatal("final acquire failed")
	}
	finalPort := final.Snapshot().Config.GetInt("server.port")
	finalPlugin, _ := final.Snapshot().Plugin(fmt.Sprintf("p-%d", finalPort))
	final.Release()

	if errorsFound.Load() != 0 {
		t.Fatalf("observed %d incoherent (port,plugin) generations across users", errorsFound.Load())
	}
	log.Printf("INPUT 11 reload attempts interleaved with 24 lease users (%d completed)\n"+
		"JUDGEMENT 0 torn/mismatched generations; final coherent state port=%d plugin=%s; retained=%d history=%d",
		usersDone.Load(), finalPort, finalPlugin.Name(), len(m.Status().Retained), len(m.Status().History))
}

// TestReloadRefusedAtCapacity verifies that once the retained-generation
// capacity is full, new reloads are explicitly refused with ReloadDrainFull,
// no in-use generation is torn down, and after leases drain, reloads work
// again.
func TestReloadRefusedAtCapacity(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           10 * time.Second,
		ReapTimeout:            time.Second,
	})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(100)); err != nil {
		t.Fatal(err)
	}
	// Pin v1.
	l1 := m.Acquire()
	// Reload v2: v1 retained (1 lease).
	if err := m.Reload(ctx, mkcfg(200)); err != nil {
		t.Fatal(err)
	}
	// Pin v2, reload v3: v1 and v2 both retained -> capacity 2 reached.
	l2 := m.Acquire()
	if err := m.Reload(ctx, mkcfg(300)); err != nil {
		t.Fatal(err)
	}
	st := m.Status()
	log.Printf("INPUT pinned v1,v2 then reloaded to v3 (capacity=2)\n"+
		"JUDGEMENT active=v%d retained ids=%v leases=%v",
		st.ActiveVersion, idsOf(st.Retained), leasesOf(st.Retained))
	if st.ActiveVersion != 3 || len(st.Retained) != 2 {
		t.Fatalf("want active v3 with 2 retained, got %+v", st)
	}

	// The next reload MUST be refused before building anything.
	err := m.Reload(ctx, mkcfg(400))
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadDrainFull || !re.OldKept {
		t.Fatalf("expected ReloadDrainFull with old kept, got %v", err)
	}
	log.Printf("JUDGEMENT 4th reload refused distinctly: kind=%s reason=%q", re.Kind, re.Reason)
	if m.ActiveVersion() != 3 {
		t.Fatal("active generation must not change when reload is refused")
	}

	// Pinned old generations must still be fully usable and open.
	p1 := l1.Snapshot().Plugins[0].(*recPlugin)
	p2 := l2.Snapshot().Plugins[0].(*recPlugin)
	if c := closesOf(p1); c != 0 || closesOf(p2) != 0 {
		t.Fatalf("in-use old plugins must not be closed to make room: %d/%d",
			closesOf(p1), closesOf(p2))
	}
	if port := l1.Snapshot().Config.GetInt("server.port"); port != 100 {
		t.Fatalf("lease on v1 saw port=%d, want 100", port)
	}

	// Release one retained generation: capacity frees up, reload is accepted.
	l1.Release()
	if !waitFor(2*time.Second, func() bool { return len(m.Status().Retained) == 1 }) {
		t.Fatalf("v1 should be reclaimed after release: %+v", m.Status().Retained)
	}
	if err := m.Reload(ctx, mkcfg(400)); err != nil {
		t.Fatalf("reload should be accepted after capacity freed: %v", err)
	}
	log.Printf("JUDGEMENT after v1 drained, reload to v4 accepted; active=v%d retained ids=%v",
		m.ActiveVersion(), idsOf(m.Status().Retained))
	if m.ActiveVersion() != 4 {
		t.Fatal("active must be v4 after accepted reload")
	}
	l2.Release()
}

// TestDrainTimeoutMarksStuck verifies a generation whose leases outlive the
// drain timeout becomes observable as "stuck" (distinguishable from normal
// draining) WITHOUT being closed, still counts against capacity, and is
// eventually reclaimed normally when the lease ends.
func TestDrainTimeoutMarksStuck(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           60 * time.Millisecond,
		ReapTimeout:            50 * time.Millisecond,
	})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(1)); err != nil {
		t.Fatal(err)
	}
	l := m.Acquire()
	p := l.Snapshot().Plugins[0].(*recPlugin)
	if err := m.Reload(ctx, mkcfg(2)); err != nil {
		t.Fatal(err)
	}
	// Pin the active v2 as well, then reload to v3 so both v1 and v2 are
	// retained: v1 is the generation whose stuck timeout we observe.
	l2 := m.Acquire()
	_ = l2
	if err := m.Reload(ctx, mkcfg(3)); err != nil {
		t.Fatal(err)
	}
	// Wait past drain timeout while the lease on v1 is still held. v1 is the
	// oldest retained generation (retained is ordered oldest-first).
	if !waitFor(2*time.Second, func() bool {
		st := m.Status()
		return len(st.Retained) >= 1 && st.Retained[0].ID == 1 &&
			st.Retained[0].State == GenStuck
	}) {
		t.Fatalf("retained v1 should become stuck: %+v", m.Status().Retained)
	}
	st := m.Status()
	v1 := st.Retained[0]
	log.Printf("INPUT lease on v1 held past drain timeout\nJUDGEMENT v%d state=%s timedOut=%t leases=%d stillOpen=%t",
		v1.ID, v1.State, v1.DrainTimedOut, v1.Leases, closesOf(p) == 0)
	if closesOf(p) != 0 {
		t.Fatal("stuck generation must NOT be closed under a live user")
	}
	// While stuck it still occupies capacity: another reload is refused.
	err := m.Reload(ctx, mkcfg(4))
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadDrainFull {
		t.Fatalf("stuck generation must still count against capacity, got %v", err)
	}
	// Release: eventually reclaimed with a timeout-labeled outcome.
	l.Release()
	if !waitFor(2*time.Second, func() bool {
		h := m.Status().History
		for _, gi := range h {
			if gi.ID == 1 {
				return gi.Outcome == OutcomeDrainTimeout
			}
		}
		return false
	}) {
		t.Fatal("stuck v1 should reclaim with drain_timeout outcome")
	}
	if closesOf(p) != 1 {
		t.Fatalf("stuck generation resources must be released after release, closes=%d", closesOf(p))
	}
	var h1 GenerationInfo
	for _, gi := range m.Status().History {
		if gi.ID == 1 {
			h1 = gi
		}
	}
	if h1.Outcome != OutcomeDrainTimeout || !h1.DrainTimedOut {
		t.Fatalf("history outcome must distinguish timeout drain, got %+v", h1)
	}
	log.Printf("JUDGEMENT after release v%d reclaimed outcome=%s resource released=true",
		h1.ID, h1.Outcome)
	l2.Release()
}

// TestCloseFailureOutcome verifies that when plugin Close fails, the
// generation is still reclaimed (leases drop, capacity frees) but the outcome
// is distinguishable as close_failed and the error text is observable.
func TestCloseFailureOutcome(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{
		MaxRetainedGenerations: 1,
		DrainTimeout:           time.Second,
		ReapTimeout:            time.Second,
	})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(1)); err != nil {
		t.Fatal(err)
	}
	l := m.Acquire()
	bad := l.Snapshot().Plugins[0].(*recPlugin)
	bad.closeErr = errors.New("simulated close failure")
	if err := m.Reload(ctx, mkcfg(2)); err != nil {
		t.Fatal(err)
	}
	l.Release()
	if !waitFor(2*time.Second, func() bool {
		h := m.Status().History
		return len(h) > 0 && h[0].Outcome == OutcomeCloseFailed
	}) {
		t.Fatalf("expected close_failed outcome in history, got %+v", m.Status().History)
	}
	h0 := m.Status().History[0]
	log.Printf("INPUT plugin close returns error on last release\n"+
		"JUDGEMENT reclaimed outcome=%s closeErr=%q capacityFreed=%t",
		h0.Outcome, h0.CloseErr, len(m.Status().Retained) == 0)
	if h0.CloseErr == "" {
		t.Fatal("close error text must be observable")
	}
	// Capacity must free even though close failed, so a new reload works.
	if err := m.Reload(ctx, mkcfg(3)); err != nil {
		t.Fatalf("reload must be accepted after close-failed generation reclaimed: %v", err)
	}
}

func idsOf(gs []GenerationInfo) []uint64 {
	out := make([]uint64, 0, len(gs))
	for _, g := range gs {
		out = append(out, g.ID)
	}
	return out
}

func leasesOf(gs []GenerationInfo) []int {
	out := make([]int, 0, len(gs))
	for _, g := range gs {
		out = append(out, g.Leases)
	}
	return out
}

// TestShutdownWaitsAndKeepsLeasedOpen verifies Shutdown closes idle
// generations but never closes a generation under a live lease: the lease
// holder keeps working, and the generation is closed exactly once when the
// last Release happens after Shutdown returns.
func TestShutdownWaitsAndKeepsLeasedOpen(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           time.Second,
		ReapTimeout:            80 * time.Millisecond,
	})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(1)); err != nil {
		t.Fatal(err)
	}
	l := m.Acquire()
	p := l.Snapshot().Plugins[0].(*recPlugin)
	// Retire v1 to v2 while v1 is leased.
	if err := m.Reload(ctx, mkcfg(2)); err != nil {
		t.Fatal(err)
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- m.Shutdown(ctx) }()

	// Shutdown waits up to ReapTimeout; while it waits (and after it gives up
	// waiting and returns), v1 must remain open for the lease holder.
	time.Sleep(40 * time.Millisecond)
	if closesOf(p) != 0 {
		t.Fatalf("leased old generation must remain open during shutdown wait, closes=%d", closesOf(p))
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if closesOf(p) != 0 {
		t.Fatal("leased generation must still be open after Shutdown gave up waiting")
	}
	if m.Acquire() != nil {
		t.Fatal("Acquire must return nil after shutdown")
	}
	// Lease holder finishes work after shutdown: plugin still works and then
	// gets closed exactly once on Release.
	if port := l.Snapshot().Config.GetInt("server.port"); port != 1 {
		t.Fatalf("post-shutdown lease must keep old config, port=%d", port)
	}
	l.Release()
	if !waitFor(2*time.Second, func() bool { return closesOf(p) == 1 }) {
		t.Fatalf("leased generation must close once after release, closes=%d", closesOf(p))
	}
	outcome := OutcomeNone
	for _, h := range m.Status().History {
		if h.ID == 1 {
			outcome = h.Outcome
		}
	}
	log.Printf("INPUT shutdown with leased v1 (ReapTimeout=80ms)\n"+
		"JUDGEMENT v1 stayed open through shutdown, closed once after release, outcome=%s", outcome)
	if outcome != OutcomeShutdown && outcome != OutcomeDrained {
		t.Fatalf("post-shutdown reclaim outcome should be shutdown/drained, got %s", outcome)
	}
	// Repeated shutdown is safe.
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

// TestMultipleLeasesRefcount verifies a generation is reclaimed only after
// EVERY lease on it is released (not after the first), and each Release is
// idempotent.
func TestMultipleLeasesRefcount(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           time.Second,
		ReapTimeout:            time.Second,
	})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(1)); err != nil {
		t.Fatal(err)
	}
	l1 := m.Acquire()
	l2 := m.Acquire()
	l3 := m.Acquire()
	if err := m.Reload(ctx, mkcfg(2)); err != nil {
		t.Fatal(err)
	}
	p := l1.Snapshot().Plugins[0].(*recPlugin)
	l1.Release()
	l1.Release() // idempotent
	l2.Release()
	if closesOf(p) != 0 {
		t.Fatalf("generation must stay open while l3 is held, closes=%d", closesOf(p))
	}
	st := m.Status()
	if len(st.Retained) != 1 || st.Retained[0].Leases != 1 {
		t.Fatalf("one lease must remain, got %+v", st.Retained)
	}
	l3.Release()
	if !waitFor(2*time.Second, func() bool { return closesOf(p) == 1 }) {
		t.Fatal("generation must close after last lease release")
	}
	log.Printf("INPUT 3 leases on v1, released interleaved after reload\nJUDGEMENT reclaimed only after 3rd release, closes=1")
}

// TestBuildFailureConsumesNoCapacity verifies a failed build does not publish
// or retire anything, so it does not consume capacity and reloads keep
// working afterward. A lease taken before the failure stays on the old
// generation.
func TestBuildFailureConsumesNoCapacity(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           time.Second,
		ReapTimeout:            50 * time.Millisecond,
	})
	ctx := context.Background()
	if err := m.Reload(ctx, mkcfg(1)); err != nil {
		t.Fatal(err)
	}
	l := m.Acquire()
	if err := m.Reload(ctx, mkcfg(2)); err != nil {
		t.Fatal(err)
	} // v1 retained; one capacity slot still free

	// A failed build must not publish/retire anything and must not consume
	// the remaining capacity slot.
	m.build = func(context.Context, *config.Config) (*Snapshot, error) {
		return nil, errors.New("boom")
	}
	err := m.Reload(ctx, mkcfg(3))
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadBuildFailed {
		t.Fatalf("expected build failure, got %v", err)
	}
	st := m.Status()
	log.Printf("INPUT build failed with one free capacity slot and leased v1\n"+
		"JUDGEMENT active=v%d retained=%v (unchanged), kind=%s",
		st.ActiveVersion, idsOf(st.Retained), re.Kind)
	if st.ActiveVersion != 2 || len(st.Retained) != 1 || st.Retained[0].ID != 1 {
		t.Fatalf("state must be unchanged after build failure, got active=%d retained=%v",
			st.ActiveVersion, idsOf(st.Retained))
	}

	// Restore a healthy builder and fill the remaining slot by pinning v2 and
	// reloading to v3; then the next reload must be refused for capacity —
	// proving the failed attempt leaked no slot (otherwise we could only have
	// 0 or 2 retained here).
	m2build := func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		p := &recPlugin{name: fmt.Sprintf("p-%d", cfg.GetInt("server.port"))}
		if err := p.Init(ctx); err != nil {
			return nil, err
		}
		return &Snapshot{Config: cfg, Plugins: []plugin.Plugin{p}}, nil
	}
	m.build = m2build
	l2 := m.Acquire()
	if err := m.Reload(ctx, mkcfg(3)); err != nil {
		t.Fatalf("reload to v3 should succeed: %v", err)
	}
	if err := m.Reload(ctx, mkcfg(4)); err == nil {
		t.Fatal("reload must be refused once both retained slots are occupied")
	} else {
		var re2 *ReloadError
		if !errors.As(err, &re2) || re2.Kind != ReloadDrainFull {
			t.Fatalf("expected drain-full, got %v", err)
		}
		log.Printf("JUDGEMENT failed build consumed no slot; v4 later refused with %s", re2.Kind)
	}
	l2.Release()
	l.Release()
}

// TestAcquireBeforeBootAndAfterShutdown returns nil in both cases.
func TestAcquireBeforeBootAndAfterShutdown(t *testing.T) {
	m, mkcfg := newTestManager(t, Options{ReapTimeout: 20 * time.Millisecond})
	if m.Acquire() != nil {
		t.Fatal("Acquire before any load must return nil")
	}
	if m.Status().ActiveVersion != 0 || m.Status().Reloading {
		t.Fatal("fresh manager must have no active version and no reload in progress")
	}
	if err := m.Reload(context.Background(), mkcfg(1)); err != nil {
		t.Fatal(err)
	}
	if m.Acquire() == nil {
		t.Fatal("Acquire after load must succeed")
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Acquire() != nil {
		t.Fatal("Acquire after shutdown must return nil")
	}
	if m.ActiveVersion() != 0 {
		t.Fatal("active version must be 0 after shutdown")
	}
	// Reload after shutdown is rejected and leaves no active gen.
	err := m.Reload(context.Background(), mkcfg(2))
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadBuildFailed {
		t.Fatalf("expected rejection after shutdown, got %v", err)
	}
	log.Printf("INPUT acquire before boot / reload after shutdown\nJUDGEMENT both safely refused; %v", err)
}
