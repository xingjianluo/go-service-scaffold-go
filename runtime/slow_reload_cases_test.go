package runtime

// Slow-reload window test cases. Each test logs "INPUT ..." describing the
// covered business scenario and "JUDGEMENT ..." stating the assertion basis,
// matching the rest of the package.

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

// TestStatusImmediatelyReflectsPreparingReload covers: "重载还在准备、还没真正
// 生效的这段时间，随时能查到当前正在重载"。A slow build must NOT make Status
// wait until the reload ends; Status returns at once with Reloading=true while
// the old ActiveVersion keeps being served, then flips to Reloading=false with
// the new ActiveVersion once publication lands.
func TestStatusImmediatelyReflectsPreparingReload(t *testing.T) {
	g := newGateBuild()
	m := newGateManager(t, g, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           time.Second,
		ReapTimeout:            time.Second,
	})

	// Boot v1.
	g.finish()
	if err := m.Reload(context.Background(), cfgWithPort(100)); err != nil {
		t.Fatalf("boot reload: %v", err)
	}
	if st := m.Status(); st.Reloading || st.ActiveVersion != 1 {
		t.Fatalf("post-boot status inconsistent: %+v", st)
	}

	// Begin a SLOW reload to v2 and hold it in the prepare phase.
	g.reset()
	done := startSlowReload(m, cfgWithPort(200))
	waitBuildStarted(t, g)

	// Status MUST return immediately (well before the build finishes) and
	// honestly say a reload is in progress while v1 stays the served version.
	statusCh := make(chan ManagerStatus, 1)
	go func() { statusCh <- m.Status() }()
	var st ManagerStatus
	select {
	case st = <-statusCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Status blocked during the slow prepare phase instead of returning immediately")
	}
	if !st.Reloading {
		t.Fatal("Status must report Reloading=true while the reload is still preparing")
	}
	if st.ActiveVersion != 1 {
		t.Fatalf("old version must stay active during prepare, got active=v%d", st.ActiveVersion)
	}
	if st.ReloadingSince.IsZero() {
		t.Fatal("ReloadingSince must be set while a reload is preparing")
	}
	// Other externally observable reads stay consistent: the served snapshot
	// is still the old one and reads are non-nil/immediate.
	if v := m.ActiveVersion(); v != 1 {
		t.Fatalf("ActiveVersion must read v1 during prepare, got v%d", v)
	}
	if snap := m.CurrentSnapshot(); snap == nil || snap.Config.GetInt("server.port") != 100 {
		t.Fatalf("CurrentSnapshot must keep serving old state during prepare, got %+v", snap)
	}
	log.Printf("INPUT slow reload preparing (build held), queried Status mid-flight\n"+
		"JUDGEMENT Status returned immediately: reloading=%t active=v%d since=%v servedPort=%d",
		st.Reloading, st.ActiveVersion, st.ReloadingSince.Format(time.RFC3339Nano),
		m.CurrentSnapshot().Config.GetInt("server.port"))

	// Let the build finish: publication lands and the observable state flips.
	g.finish()
	if err := waitReloadResult(t, done); err != nil {
		t.Fatalf("reload after release: %v", err)
	}
	after := m.Status()
	if after.Reloading {
		t.Fatal("Reloading must clear once the reload completes")
	}
	if !after.ReloadingSince.IsZero() {
		t.Fatal("ReloadingSince must reset once the reload completes")
	}
	if after.ActiveVersion != 2 {
		t.Fatalf("active must become v2 after publication, got v%d", after.ActiveVersion)
	}
	log.Printf("INPUT build released, reload published\n"+
		"JUDGEMENT Status converged: reloading=%t active=v%d", after.Reloading, after.ActiveVersion)
}

// TestSlowPrepareNeverBlocksBusiness covers: "重载进行中不能把业务请求堵住"。
//   - An in-flight request already on the old generation runs to completion and
//     releases on time while the reload is still preparing.
//   - Requests that ARRIVE during the slow prepare immediately pin the old
//     (still-serving) generation and proceed; they never wait for the build.
//   - After the reload actually publishes, new arrivals pin the new generation.
//
// Acquire/Release latencies are bounded even though the prepare phase is held
// far longer than those bounds.
func TestSlowPrepareNeverBlocksBusiness(t *testing.T) {
	g := newGateBuild()
	m := newGateManager(t, g, Options{
		MaxRetainedGenerations: 4,
		DrainTimeout:           5 * time.Second,
		ReapTimeout:            time.Second,
	})

	g.finish()
	if err := m.Reload(context.Background(), cfgWithPort(100)); err != nil {
		t.Fatalf("boot: %v", err)
	}

	// In-flight request A started BEFORE the reload, pinned to v1.
	leaseA := m.Acquire()
	if leaseA == nil {
		t.Fatal("Acquire after boot must succeed")
	}
	oldSnap := leaseA.Snapshot()
	oldPlugin := oldSnap.Plugins[0].(*recPlugin)
	if port := oldSnap.Config.GetInt("server.port"); port != 100 {
		t.Fatalf("request A expected port 100, got %d", port)
	}

	// Start a deliberately SLOW reload (prepare held open until we release it).
	g.reset()
	done := startSlowReload(m, cfgWithPort(200))
	waitBuildStarted(t, g)
	prepareStart := time.Now()

	// New request B arriving DURING prepare must be served immediately from the
	// OLD generation; Acquire must return in well under the held-build window.
	acquireWithin := func(limit time.Duration) *Lease {
		got := make(chan *Lease, 1)
		go func() { got <- m.Acquire() }()
		select {
		case l := <-got:
			return l
		case <-time.After(limit):
			t.Fatalf("Acquire blocked for >%v during slow prepare", limit)
			return nil
		}
	}
	beforeAcquire := time.Now()
	leaseB := acquireWithin(50 * time.Millisecond)
	acquireLatency := time.Since(beforeAcquire)
	if leaseB == nil {
		t.Fatal("new request during prepare must still get a lease")
	}
	if port := leaseB.Snapshot().Config.GetInt("server.port"); port != 100 {
		t.Fatalf("request B during prepare must be served old port 100, got %d", port)
	}
	if leaseB.Snapshot() != oldSnap {
		t.Fatal("request B during prepare must pin the same (old) generation as A")
	}

	// In-flight A finishes its whole unit of work on the old plugin, then
	// releases. Release must return promptly (old gen stays open because B
	// still holds it).
	oldPluginWorked := make(chan bool, 1)
	go func() {
		_ = oldPlugin.Name() // exercise the pinned plugin
		oldPluginWorked <- true
	}()
	select {
	case <-oldPluginWorked:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("in-flight request A could not finish its work during slow prepare")
	}
	releaseReturned := make(chan struct{}, 1)
	go func() { leaseA.Release(); close(releaseReturned) }()
	select {
	case <-releaseReturned:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Release blocked during slow prepare")
	}
	if c := closesOf(oldPlugin); c != 0 {
		t.Fatalf("old generation must stay open while B holds it, closes=%d", c)
	}

	// Repeated new arrivals throughout the slow window are all served at once
	// from v1, each promptly.
	const arrivals = 20
	var extra []*Lease
	for i := 0; i < arrivals; i++ {
		l := acquireWithin(50 * time.Millisecond)
		if port := l.Snapshot().Config.GetInt("server.port"); port != 100 {
			t.Fatalf("arrival %d during prepare must see old port 100, got %d", i, port)
		}
		extra = append(extra, l)
	}

	// Hold the prepare phase open for a deliberately long window. Business
	// operations above and below all proceed during this slow period.
	time.Sleep(250 * time.Millisecond)
	prepareHeldFor := time.Since(prepareStart)

	log.Printf("INPUT reload prepare held %v; A in-flight, B+%d arrivals during prepare\n"+
		"JUDGEMENT all served immediately on old v1 (first Acquire latency=%v); A finished+released; old plugin open=%t",
		prepareHeldFor, arrivals, acquireLatency, closesOf(oldPlugin) == 0)

	// Release B and the extra arrivals so v1 can drain right after publication.
	leaseB.Release()
	for _, l := range extra {
		l.Release()
	}

	// Let the slow build finish; reload publishes v2.
	g.finish()
	if err := waitReloadResult(t, done); err != nil {
		t.Fatalf("reload: %v", err)
	}

	// A request arriving AFTER publication immediately pins the NEW generation.
	leaseD := acquireWithin(50 * time.Millisecond)
	if port := leaseD.Snapshot().Config.GetInt("server.port"); port != 200 {
		t.Fatalf("request D after publish must see new port 200, got %d", port)
	}
	newPlugin := leaseD.Snapshot().Plugins[0].(*recPlugin)

	// The old generation, now fully released, is reclaimed and closed once;
	// the new active plugin stays open.
	if !waitFor(2*time.Second, func() bool { return closesOf(oldPlugin) == 1 }) {
		t.Fatalf("old generation not reclaimed after draining, closes=%d", closesOf(oldPlugin))
	}
	if c := closesOf(newPlugin); c != 0 {
		t.Fatalf("new plugin must remain open while D holds it, closes=%d", c)
	}
	log.Printf("INPUT prepare released, reload published to v2\n"+
		"JUDGEMENT D served new port=200; old v1 reclaimed (closes=1); new plugin open=%t",
		closesOf(newPlugin) == 0)
	leaseD.Release()
}

// checkConvergedOldKept is the shared assertion for failed/timed-out slow
// reloads: the Reloading flag has reset, the old generation is still the
// served one, its plugin is untouched and still usable, and no new retained
// generation appeared.
func checkConvergedOldKept(t *testing.T, m *Manager, oldSnap *Snapshot, oldPlugin *recPlugin) {
	t.Helper()
	st := m.Status()
	if st.Reloading {
		t.Fatal("Reloading flag must reset after a failed/timed-out reload")
	}
	if !st.ReloadingSince.IsZero() {
		t.Fatal("ReloadingSince must reset after a failed/timed-out reload")
	}
	// The boot generation in these failure cases is v1 and must stay active.
	if st.ActiveVersion != 1 {
		t.Fatalf("old generation v1 must stay active, got v%d", st.ActiveVersion)
	}
	if got := m.CurrentSnapshot(); got != oldSnap {
		t.Fatal("served snapshot must be unchanged after failed/timed-out reload")
	}
	if c := closesOf(oldPlugin); c != 0 {
		t.Fatalf("old plugin must be untouched (no close), closes=%d", c)
	}
	if len(st.Retained) != 0 {
		t.Fatalf("a failed reload must not retire anything, retained=%+v", st.Retained)
	}
}

// TestSlowPrepareBuildFailureResetsFlag covers: slow prepare that ends in a
// BUILD FAILURE must leave the old state intact and clear Reloading.
func TestSlowPrepareBuildFailureResetsFlag(t *testing.T) {
	g := newGateBuild()
	m := newGateManager(t, g, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           time.Second,
		ReapTimeout:            time.Second,
	})

	g.finish()
	if err := m.Reload(context.Background(), cfgWithPort(100)); err != nil {
		t.Fatalf("boot: %v", err)
	}
	lease := m.Acquire()
	oldSnap := lease.Snapshot()
	oldPlugin := oldSnap.Plugins[0].(*recPlugin)

	// Slow prepare that will FAIL when released.
	g.setFail(errors.New("plugin failed to bind new generation"))
	g.reset()
	done := startSlowReload(m, cfgWithPort(200))
	waitBuildStarted(t, g)
	if st := m.Status(); !st.Reloading {
		t.Fatal("must observe Reloading=true during the failing prepare")
	}
	time.Sleep(50 * time.Millisecond) // genuinely slow prepare window
	g.finish()

	err := waitReloadResult(t, done)
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadBuildFailed || !re.OldKept {
		t.Fatalf("expected ReloadBuildFailed with OldKept, got %v", err)
	}

	checkConvergedOldKept(t, m, oldSnap, oldPlugin)
	// The leased old generation is still fully usable by its holder.
	if port := lease.Snapshot().Config.GetInt("server.port"); port != 100 {
		t.Fatalf("lease must keep working on old port 100, got %d", port)
	}
	lease.Release()
	log.Printf("INPUT slow prepare ended in build failure while a request held v1\n"+
		"JUDGEMENT kind=%s oldKept=%t reloading=%t active=v%d oldCloses=%d retained=%d",
		re.Kind, re.OldKept, m.Status().Reloading, m.Status().ActiveVersion,
		closesOf(oldPlugin), len(m.Status().Retained))

	// A later healthy reload still works: the failed attempt left no residue.
	g.setFail(nil)
	g.reset()
	g.finish()
	if err := m.Reload(context.Background(), cfgWithPort(300)); err != nil {
		t.Fatalf("reload after a failed prepare must still work: %v", err)
	}
	if v := m.ActiveVersion(); v != 2 {
		t.Fatalf("active must advance to v2 after the healthy reload, got v%d", v)
	}
}

// TestSlowPrepareTimeoutResetsFlag covers: a slow prepare whose context times
// out must be classified reload_timeout, reset the flag, and leave the old
// generation completely intact and still serving.
func TestSlowPrepareTimeoutResetsFlag(t *testing.T) {
	g := newGateBuild()
	m := newGateManager(t, g, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           time.Second,
		ReapTimeout:            time.Second,
	})

	g.finish()
	if err := m.Reload(context.Background(), cfgWithPort(100)); err != nil {
		t.Fatalf("boot: %v", err)
	}
	lease := m.Acquire()
	oldSnap := lease.Snapshot()
	oldPlugin := oldSnap.Plugins[0].(*recPlugin)

	// Context with a short deadline while the build stays in slow prepare.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	g.reset()
	done := startSlowReloadCtx(ctx, m, cfgWithPort(200))
	waitBuildStarted(t, g)
	if st := m.Status(); !st.Reloading {
		t.Fatal("must observe Reloading=true during the timed-out prepare")
	}

	err := waitReloadResult(t, done)
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadTimeout || !re.OldKept {
		t.Fatalf("expected ReloadTimeout with OldKept, got %v", err)
	}

	checkConvergedOldKept(t, m, oldSnap, oldPlugin)
	if port := lease.Snapshot().Config.GetInt("server.port"); port != 100 {
		t.Fatalf("lease must keep working on old port 100, got %d", port)
	}
	lease.Release()
	log.Printf("INPUT slow prepare exceeded its 80ms context deadline while a request held v1\n"+
		"JUDGEMENT kind=%s oldKept=%t reloading=%t active=v%d oldCloses=%d",
		re.Kind, re.OldKept, m.Status().Reloading, m.Status().ActiveVersion, closesOf(oldPlugin))

	// The gate was never released; a subsequent healthy reload still succeeds.
	g.reset()
	g.finish()
	if err := m.Reload(context.Background(), cfgWithPort(300)); err != nil {
		t.Fatalf("reload after a timed-out prepare must still work: %v", err)
	}
	if v := m.ActiveVersion(); v != 2 {
		t.Fatalf("active must advance to v2 after the healthy reload, got v%d", v)
	}
}

// delayBuild is a build func that spends a short but real amount of time in the
// prepare phase every call and fails for a configured "poison" port, used to
// drive many genuinely-slow reloads concurrently with readers and workers.
type delayBuild struct {
	failPorts map[int]bool
	calls     atomic.Int64
}

func (b *delayBuild) build(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
	b.calls.Add(1)
	port := cfg.GetInt("server.port")
	// Stay in the slow prepare window long enough that concurrent Status
	// polls and workers overlap it, but short enough to keep the test fast.
	select {
	case <-time.After(5 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if b.failPorts[port] {
		return nil, fmt.Errorf("simulated slow-build failure for port %d", port)
	}
	p := &recPlugin{name: fmt.Sprintf("p-%d", port)}
	if err := p.Init(ctx); err != nil {
		return nil, err
	}
	return &Snapshot{Config: cfg, Plugins: []plugin.Plugin{p}}, nil
}

// TestSlowReloadInterleavedWithStatusAndWork covers: "多个状态查询和业务请求与
// 重载并发交错时结果自洽，不互相卡死"。Workers continuously pin and use a
// coherent generation; a status poller continuously validates observable
// invariants; a reloader drives many slow reloads (mixed success/failure and
// the occasional canceled context). Nothing may stall beyond a bounded
// latency, and every observed (config, plugin) pair must belong to one
// complete generation.
func TestSlowReloadInterleavedWithStatusAndWork(t *testing.T) {
	db := &delayBuild{failPorts: map[int]bool{}}
	m := NewManager(db.build, Options{
		MaxRetainedGenerations: 64,
		DrainTimeout:           200 * time.Millisecond,
		ReapTimeout:            time.Second,
	})

	// Boot v1.
	if err := m.Reload(context.Background(), cfgWithPort(1)); err != nil {
		t.Fatalf("boot: %v", err)
	}

	var (
		stop              atomic.Bool
		errorsFound       atomic.Int64
		stalled           atomic.Int64
		workersDone       atomic.Int64
		pollsDone         atomic.Int64
		sawReloading      atomic.Bool
		lastActiveVersion atomic.Uint64
	)

	// Workers: each unit of work Acquires, uses BOTH config and its matching
	// plugin from the pinned lease, then Releases. Acquire/Release must return
	// promptly even while a slow reload is preparing.
	const workerCount = 16
	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				acqCh := make(chan *Lease, 1)
				go func() { acqCh <- m.Acquire() }()
				var l *Lease
				select {
				case l = <-acqCh:
				case <-time.After(500 * time.Millisecond):
					stalled.Add(1)
					return
				}
				if l == nil {
					errorsFound.Add(1)
					return
				}
				port := l.Snapshot().Config.GetInt("server.port")
				want := fmt.Sprintf("p-%d", port)
				if p, ok := l.Snapshot().Plugin(want); !ok || p.Name() != want {
					errorsFound.Add(1)
				}
				relCh := make(chan struct{}, 1)
				go func() { l.Release(); relCh <- struct{}{} }()
				select {
				case <-relCh:
				case <-time.After(500 * time.Millisecond):
					stalled.Add(1)
					return
				}
				workersDone.Add(1)
			}
		}()
	}

	// Status poller: validates observable invariants at every instant.
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for !stop.Load() {
			st := m.Status()
			if st.Reloading {
				sawReloading.Store(true)
				if st.ReloadingSince.IsZero() {
					errorsFound.Add(1) // reloading with no timestamp is inconsistent
				}
			}
			// The active version must never go backwards across observations.
			// (We deliberately do NOT compare a second, separately-sampled
			// lock-free read here: two independent reads may legitimately
			// straddle one publication; each individual read is already
			// guaranteed to observe one complete generation.)
			prev := lastActiveVersion.Load()
			if st.ActiveVersion < prev {
				errorsFound.Add(1)
			}
			lastActiveVersion.CompareAndSwap(prev, st.ActiveVersion)
			// Retained generations must be internally consistent: draining or
			// stuck with non-negative leases and distinct IDs.
			seenIDs := map[uint64]bool{}
			for _, gi := range st.Retained {
				if gi.State != GenDraining && gi.State != GenStuck {
					errorsFound.Add(1)
				}
				if seenIDs[gi.ID] {
					errorsFound.Add(1)
				}
				seenIDs[gi.ID] = true
			}
			pollsDone.Add(1)
		}
	}()

	// Reloader: many slow reloads, a deterministic mix of outcomes.
	const reloadAttempts = 40
	reloadOK, reloadFailed, reloadTimeout, reloadRefused := 0, 0, 0, 0
	for v := 2; v <= reloadAttempts+1; v++ {
		port := v
		ctx := context.Background()
		switch {
		case v%5 == 0:
			db.failPorts[port] = true // build failure
		case v%7 == 0:
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Millisecond) // times out mid prepare
			defer cancel()
		}
		err := m.Reload(ctx, cfgWithPort(port))
		switch {
		case err == nil:
			reloadOK++
		default:
			var re *ReloadError
			if errors.As(err, &re) {
				switch re.Kind {
				case ReloadBuildFailed:
					reloadFailed++
				case ReloadTimeout:
					reloadTimeout++
				case ReloadDrainFull:
					reloadRefused++
				}
			}
		}
	}

	// Give workers/poller a little more overlap time, then stop everything.
	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	joinCh := make(chan struct{})
	go func() { wg.Wait(); close(joinCh) }()
	select {
	case <-joinCh:
	case <-time.After(3 * time.Second):
		t.Fatal("workers deadlocked/stalled during interleaved slow reloads")
	}
	<-pollDone

	if errorsFound.Load() != 0 {
		t.Fatalf("interleaving produced %d inconsistent observations", errorsFound.Load())
	}
	if stalled.Load() != 0 {
		t.Fatalf("%d Acquire/Release operations stalled during slow reloads", stalled.Load())
	}
	if !sawReloading.Load() {
		t.Fatal("status poller never observed Reloading=true during slow reloads")
	}

	final := m.Acquire()
	if final == nil {
		t.Fatal("final Acquire failed after interleaving")
	}
	finalPort := final.Snapshot().Config.GetInt("server.port")
	finalPlugin, _ := final.Snapshot().Plugin(fmt.Sprintf("p-%d", finalPort))
	final.Release()
	st := m.Status()
	log.Printf("INPUT %d slow reloads (ok=%d buildFailed=%d timeout=%d drainFull=%d) interleaved with %d workers/%d status polls\n"+
		"JUDGEMENT inconsistent=%d stalled=%d sawReloading=%t; final coherent port=%d plugin=%s active=v%d retained=%d",
		reloadAttempts, reloadOK, reloadFailed, reloadTimeout, reloadRefused,
		workerCount, pollsDone.Load(), errorsFound.Load(), stalled.Load(), sawReloading.Load(),
		finalPort, finalPlugin.Name(), st.ActiveVersion, len(st.Retained))
	if st.Reloading {
		t.Fatal("Reloading must be settled false after all reloads completed")
	}
}

// TestShutdownDuringSlowPrepareDiscardsNewGeneration covers the coordination
// path added for lock-free prepare: Shutdown lands while a reload is still
// building. Shutdown is authoritative; when the build finishes the reload must
// discard the freshly built generation (closing its plugins) and report a
// kept-old failure, never resurrect a served generation. In-flight leases on
// the old generation stay usable and close on their last Release.
func TestShutdownDuringSlowPrepareDiscardsNewGeneration(t *testing.T) {
	g := newGateBuild()
	m := newGateManager(t, g, Options{
		MaxRetainedGenerations: 2,
		DrainTimeout:           time.Second,
		ReapTimeout:            100 * time.Millisecond,
	})

	g.finish()
	if err := m.Reload(context.Background(), cfgWithPort(100)); err != nil {
		t.Fatalf("boot: %v", err)
	}
	lease := m.Acquire()
	oldPlugin := lease.Snapshot().Plugins[0].(*recPlugin)

	// Slow reload preparing v2 ...
	g.reset()
	done := startSlowReload(m, cfgWithPort(200))
	waitBuildStarted(t, g)
	if st := m.Status(); !st.Reloading {
		t.Fatal("must observe Reloading=true before shutdown lands")
	}

	// ... Shutdown happens while the build is still in flight. The leased old
	// generation is kept open for its holder.
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown during prepare: %v", err)
	}
	if m.ActiveVersion() != 0 {
		t.Fatal("served generation must be removed by shutdown")
	}
	if m.Acquire() != nil {
		t.Fatal("Acquire must return nil after shutdown")
	}
	if c := closesOf(oldPlugin); c != 0 {
		t.Fatalf("leased old generation must stay open for its holder, closes=%d", c)
	}

	// Let the in-flight build finish: it must be discarded, not published.
	g.finish()
	err := waitReloadResult(t, done)
	var re *ReloadError
	if !errors.As(err, &re) || !re.OldKept {
		t.Fatalf("in-flight reload after shutdown must fail keeping old state, got %v", err)
	}
	if m.ActiveVersion() != 0 {
		t.Fatal("discarded build must not resurrect a served generation after shutdown")
	}

	// The leased old generation remains usable and closes exactly once on its
	// last Release (labeled shutdown/drain_timeout in history).
	if port := lease.Snapshot().Config.GetInt("server.port"); port != 100 {
		t.Fatalf("holder must keep old port 100, got %d", port)
	}
	lease.Release()
	if !waitFor(2*time.Second, func() bool { return closesOf(oldPlugin) == 1 }) {
		t.Fatalf("old generation must close once after final release, closes=%d", closesOf(oldPlugin))
	}
	log.Printf("INPUT shutdown landed while v2 was still preparing; v1 leased\n"+
		"JUDGEMENT in-flight reload discarded kind=%s oldKept=%t noResurrect=%t; v1 usable then closed once",
		re.Kind, re.OldKept, m.ActiveVersion() == 0)
}
