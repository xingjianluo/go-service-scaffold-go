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

// respondFast is the upper bound any "must not block" probe is allowed to
// take on a healthy CI runner; the build it races against is held far longer
// (slowBuildHold), so a blocking implementation blows the bound decisively.
const (
	respondFast = 200 * time.Millisecond
)

// gateBuild builds one snapshot; ports registered via gate block until that
// gate is released or ctx ends. A negative port fails preparation.
type gateBuild struct {
	gates   map[int]chan struct{}
	started map[int]chan struct{}
}

func newGateBuild() *gateBuild {
	return &gateBuild{gates: map[int]chan struct{}{}, started: map[int]chan struct{}{}}
}

// gate registers a blocking build for port and returns (holdGate, startedCh).
func (g *gateBuild) gate(port int) (chan struct{}, chan struct{}) {
	hold := make(chan struct{})
	started := make(chan struct{})
	g.gates[port] = hold
	g.started[port] = started
	return hold, started
}

func (g *gateBuild) build(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
	port := cfg.GetInt("server.port")
	if hold, ok := g.gates[port]; ok {
		close(g.started[port])
		select {
		case <-hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if port < 0 {
		return nil, errors.New("simulated preparation failure")
	}
	p := &recPlugin{name: fmt.Sprintf("p-%d", port)}
	if err := p.Init(ctx); err != nil {
		return nil, err
	}
	return &Snapshot{Config: cfg, Plugins: []plugin.Plugin{p}}, nil
}

// bootManager reloads the initial v1 generation.
func bootManager(t *testing.T, m *Manager, mkcfg func(int) *config.Config) {
	t.Helper()
	if err := m.Reload(context.Background(), mkcfg(1)); err != nil {
		t.Fatalf("initial load: %v", err)
	}
}

// TestStatusReflectsReloadingImmediately is business "重载状态查询": while the
// reload is still preparing, Status must return AT once with Reloading=true
// and the still-serving old ActiveVersion; it must not wait for the reload to
// finish (after which it would already read false, hiding the state).
func TestStatusReflectsReloadingImmediately(t *testing.T) {
	gb := newGateBuild()
	m := NewManager(gb.build, Options{})
	mkcfg := func(p int) *config.Config { return cfgWithPort(p) }
	bootManager(t, m, mkcfg)

	hold, started := gb.gate(2)
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- m.Reload(context.Background(), mkcfg(2)) }()
	<-started

	// JUDGEMENT 1: a status query during preparation returns immediately and
	// honestly reports an in-progress reload on the still-active old version.
	done := make(chan ManagerStatus, 1)
	go func() { done <- m.Status() }()
	select {
	case st := <-done:
		if !st.Reloading {
			t.Fatal("Status must report Reloading=true while preparation runs")
		}
		if st.ActiveVersion != 1 {
			t.Fatalf("old v1 must stay the served version during preparation, got v%d", st.ActiveVersion)
		}
		log.Printf("INPUT reload to v2 gate-held in preparation\n"+
			"JUDGEMENT Status returned immediately: reloading=%t active=v%d retained=%d history=%d",
			st.Reloading, st.ActiveVersion, len(st.Retained), len(st.History))
	case <-time.After(respondFast):
		t.Fatal("Status blocked until the slow reload finished")
	}

	// Consistency of the other external reads during the same window:
	// ActiveVersion and CurrentSnapshot must instantly show the old state too.
	vch := make(chan uint64, 1)
	go func() { vch <- m.ActiveVersion() }()
	select {
	case v := <-vch:
		if v != 1 {
			t.Fatalf("ActiveVersion must read v1 during preparation, got v%d", v)
		}
	case <-time.After(respondFast):
		t.Fatal("ActiveVersion blocked during slow reload")
	}
	sch := make(chan *Snapshot, 1)
	go func() { sch <- m.CurrentSnapshot() }()
	select {
	case s := <-sch:
		if s == nil || s.Config.GetInt("server.port") != 1 {
			t.Fatalf("CurrentSnapshot must instantly serve old v1, got %+v", s)
		}
		log.Printf("JUDGEMENT consistent external reads during preparation: ActiveVersion=v%d snapshotPort=%d",
			1, s.Config.GetInt("server.port"))
	case <-time.After(respondFast):
		t.Fatal("CurrentSnapshot blocked during slow reload")
	}

	// Let preparation finish: the flag must converge to false and v2 go live.
	close(hold)
	select {
	case err := <-reloadDone:
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reload never completed")
	}
	st := m.Status()
	if st.Reloading || st.ActiveVersion != 2 {
		t.Fatalf("after publication want reloading=false active=v2, got %+v", st)
	}
	log.Printf("JUDGEMENT after preparation completes: reloading=%t active=v%d (flag converged)",
		st.Reloading, st.ActiveVersion)
}

// TestSlowReloadNeverBlocksRequests is business "慢重载期间请求服务": an
// in-flight request started before the reload finishes normally on the old
// generation, and every new request arriving during preparation is served
// immediately on the old generation; after publication new requests switch to
// the new one. Acquire/Release/Snapshot must never hang on the slow build.
func TestSlowReloadNeverBlocksRequests(t *testing.T) {
	gb := newGateBuild()
	m := NewManager(gb.build, Options{
		MaxRetainedGenerations: 4,
		DrainTimeout:           2 * time.Second,
	})
	mkcfg := func(p int) *config.Config { return cfgWithPort(p) }
	bootManager(t, m, mkcfg)

	// In-flight request A on v1, started before the reload.
	leaseA := m.Acquire()
	oldPlug := leaseA.Snapshot().Plugins[0].(*recPlugin)

	hold, started := gb.gate(2)
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- m.Reload(context.Background(), mkcfg(2)) }()
	<-started

	// New requests arriving DURING preparation: each is served immediately on
	// the old v1 and released; none may wait on the build.
	const arrivals = 25
	for i := 0; i < arrivals; i++ {
		lch := make(chan *Lease, 1)
		go func() { lch <- m.Acquire() }()
		var l *Lease
		select {
		case l = <-lch:
		case <-time.After(respondFast):
			t.Fatalf("new request #%d blocked behind the slow build", i)
		}
		if port := l.Snapshot().Config.GetInt("server.port"); port != 1 {
			t.Fatalf("request during preparation must be served old v1, got port %d", port)
		}
		if p, ok := l.Snapshot().Plugin("p-1"); !ok || p.Name() != "p-1" {
			t.Fatal("request during preparation must see the old plugin set")
		}
		rch := make(chan struct{})
		go func() { l.Release(); close(rch) }()
		select {
		case <-rch:
		case <-time.After(respondFast):
			t.Fatalf("release of request #%d blocked behind the slow build", i)
		}
	}

	// Request A is still fully functional on the old plugin (not closed).
	if closesOf(oldPlug) != 0 {
		t.Fatal("in-flight old plugin must remain open while preparing")
	}
	log.Printf("INPUT slow preparation held; %d new requests served on v1 while A in flight\n"+
		"JUDGEMENT all %d requests served within %s on old generation; A's plugin open=%t",
		arrivals, arrivals, respondFast, closesOf(oldPlug) == 0)

	// Publish: subsequent new requests immediately switch to v2.
	close(hold)
	if err := <-reloadDone; err != nil {
		t.Fatalf("reload: %v", err)
	}
	l := m.Acquire()
	if port := l.Snapshot().Config.GetInt("server.port"); port != 2 {
		t.Fatalf("new request after publication must see v2, got port %d", port)
	}
	newPlug := l.Snapshot().Plugins[0].(*recPlugin)
	l.Release()

	// A only now finishes, entirely on the old generation; it then drains.
	if port := leaseA.Snapshot().Config.GetInt("server.port"); port != 1 {
		t.Fatalf("in-flight A must finish on old v1, got port %d", port)
	}
	leaseA.Release()
	if !waitFor(2*time.Second, func() bool { return closesOf(oldPlug) == 1 }) {
		t.Fatalf("old generation must be reclaimed after A finished, closes=%d", closesOf(oldPlug))
	}
	if closesOf(newPlug) != 0 {
		t.Fatal("new plugin must remain open while serving")
	}
	log.Printf("JUDGEMENT after publication new requests on v2; A finished on v1 and it was reclaimed; new plugin open=%t",
		closesOf(newPlug) == 0)
}

// TestSlowReloadFailureAndTimeoutReset is business "失败/超时收敛": whether
// preparation fails outright or the reload is canceled (timeout), the
// Reloading flag must clear, the active version and its plugins must be
// untouched, and subsequent reloads must work normally.
func TestSlowReloadFailureAndTimeoutReset(t *testing.T) {
	mkcfg := func(p int) *config.Config { return cfgWithPort(p) }

	t.Run("preparation_failure", func(t *testing.T) {
		gb := newGateBuild()
		m := NewManager(gb.build, Options{})
		bootManager(t, m, mkcfg)
		l0 := m.Acquire()
		oldPlug := l0.Snapshot().Plugins[0].(*recPlugin)
		l0.Release()

		hold, started := gb.gate(-1)
		done := make(chan error, 1)
		go func() { done <- m.Reload(context.Background(), mkcfg(-1)) }()
		<-started
		if !m.Status().Reloading {
			t.Fatal("must report reloading while a (failing) preparation runs")
		}
		close(hold)
		err := <-done
		var re *ReloadError
		if !errors.As(err, &re) || re.Kind != ReloadBuildFailed || !re.OldKept {
			t.Fatalf("expected ReloadBuildFailed with old kept, got %v", err)
		}
		st := m.Status()
		if st.Reloading {
			t.Fatal("Reloading flag must clear after preparation failure")
		}
		if st.ActiveVersion != 1 {
			t.Fatalf("old v1 must remain active after failure, got v%d", st.ActiveVersion)
		}
		if closesOf(oldPlug) != 0 {
			t.Fatal("old plugins must not be touched by a failed preparation")
		}
		log.Printf("INPUT slow preparation ultimately failed\n"+
			"JUDGEMENT kind=%s reloading=%t active=v%d oldPluginOpen=%t",
			re.Kind, st.Reloading, st.ActiveVersion, closesOf(oldPlug) == 0)

		// A later healthy reload must work: the failure did not wedge the lock.
		if err := m.Reload(context.Background(), mkcfg(2)); err != nil {
			t.Fatalf("reload after failure must succeed: %v", err)
		}
		if m.Status().ActiveVersion != 2 {
			t.Fatal("manager must recover and accept the next reload")
		}
		log.Printf("JUDGEMENT subsequent reload accepted, active=v2 (no wedge)")
	})

	t.Run("preparation_timeout", func(t *testing.T) {
		gb := newGateBuild()
		m := NewManager(gb.build, Options{})
		bootManager(t, m, mkcfg)
		l0 := m.Acquire()
		oldPlug := l0.Snapshot().Plugins[0].(*recPlugin)
		l0.Release()

		hold, started := gb.gate(2)
		defer close(hold)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- m.Reload(ctx, mkcfg(2)) }()
		<-started
		if !m.Status().Reloading {
			t.Fatal("must report reloading before cancellation")
		}
		cancel() // emulate a reload deadline/timeout while preparing
		err := <-done
		var re *ReloadError
		if !errors.As(err, &re) || re.Kind != ReloadTimeout || !re.OldKept {
			t.Fatalf("expected ReloadTimeout with old kept, got %v", err)
		}
		st := m.Status()
		if st.Reloading {
			t.Fatal("Reloading flag must clear after timeout/cancel")
		}
		if st.ActiveVersion != 1 || closesOf(oldPlug) != 0 {
			t.Fatalf("timeout must leave old v1 intact, active=v%d closes=%d",
				st.ActiveVersion, closesOf(oldPlug))
		}
		log.Printf("INPUT reload canceled while preparation blocked\n"+
			"JUDGEMENT kind=%s reloading=%t active=v%d oldPluginOpen=%t",
			re.Kind, st.Reloading, st.ActiveVersion, closesOf(oldPlug) == 0)

		if err := m.Reload(context.Background(), mkcfg(3)); err != nil {
			t.Fatalf("reload after timeout must succeed: %v", err)
		}
		if m.Status().ActiveVersion != 2 {
			t.Fatal("manager must recover after a timed-out reload")
		}
		log.Printf("JUDGEMENT subsequent reload accepted, active=v2 (canceled attempt consumed no version, no wedge)")
	})
}

// TestConcurrentReloadStatusRequestsInterleaved is business "并发交错自洽":
// slow reloads, status pollers and lease-holding request workers all run at
// once. Every observed generation must be internally coherent (port paired
// with its own plugin), status must never show a stuck Reloading flag after a
// reload settles, nothing may deadlock, and the final state must be one valid
// generation.
func TestConcurrentReloadStatusRequestsInterleaved(t *testing.T) {
	gb := newGateBuild()
	m := NewManager(gb.build, Options{
		MaxRetainedGenerations: 8,
		DrainTimeout:           time.Second,
		ReapTimeout:            time.Second,
	})
	mkcfg := func(p int) *config.Config { return cfgWithPort(p) }
	bootManager(t, m, mkcfg)

	var incoherent atomic.Int64
	var blockedProbe atomic.Int64
	var requestsDone atomic.Int64
	var statusDone atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup

	// Request workers: pin, read config+plugin coherence, do sliced work,
	// release. A request must always get a lease quickly and a matching pair.
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				lch := make(chan *Lease, 1)
				go func() { lch <- m.Acquire() }()
				var l *Lease
				select {
				case l = <-lch:
				case <-time.After(respondFast):
					blockedProbe.Add(1)
					return
				}
				if l == nil {
					incoherent.Add(1)
					return
				}
				port := l.Snapshot().Config.GetInt("server.port")
				want := fmt.Sprintf("p-%d", port)
				for i := 0; i < 4; i++ {
					if p, ok := l.Snapshot().Plugin(want); !ok || p.Name() != want {
						incoherent.Add(1)
					}
					time.Sleep(100 * time.Microsecond)
				}
				l.Release()
				requestsDone.Add(1)
			}
		}()
	}

	// Status pollers: every read must itself return promptly.
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				ch := make(chan ManagerStatus, 1)
				go func() { ch <- m.Status() }()
				select {
				case <-ch:
					statusDone.Add(1)
				case <-time.After(respondFast):
					blockedProbe.Add(1)
					return
				}
			}
		}()
	}

	// Slow reloads: each version's preparation is held briefly, then released
	// out of band to interleave preparation windows with live traffic.
	const reloads = 6
	for v := 2; v <= reloads+1; v++ {
		hold, started := gb.gate(v)
		rdone := make(chan error, 1)
		go func(port int) { rdone <- m.Reload(context.Background(), mkcfg(port)) }(v)
		<-started
		// Let traffic and status polls run while this one is still preparing.
		time.Sleep(20 * time.Millisecond)
		if !m.Status().Reloading {
			t.Errorf("reload v%d preparing but status did not show Reloading", v)
		}
		close(hold)
		err := <-rdone
		// A drain-full refusal is acceptable under heavy retention and must
		// leave a coherent, still-serving manager; anything else is a bug.
		if err != nil {
			var re *ReloadError
			if !errors.As(err, &re) || re.Kind != ReloadDrainFull {
				t.Errorf("reload v%d: unexpected error %v", v, err)
			}
		}
		// Flag must converge back shortly after the reload settles.
		if !waitFor(time.Second, func() bool { return !m.Status().Reloading }) {
			t.Errorf("Reloading flag stuck after reload v%d", v)
		}
	}

	time.Sleep(30 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	final := m.Acquire()
	if final == nil {
		t.Fatal("final acquire failed; manager not serving")
	}
	port := final.Snapshot().Config.GetInt("server.port")
	p, ok := final.Snapshot().Plugin(fmt.Sprintf("p-%d", port))
	final.Release()
	if !ok || p.Name() != fmt.Sprintf("p-%d", port) {
		t.Fatalf("final state incoherent: port=%d plugin=%v", port, ok)
	}
	if incoherent.Load() != 0 || blockedProbe.Load() != 0 {
		t.Fatalf("incoherent=%d blockedProbes=%d requests=%d status=%d",
			incoherent.Load(), blockedProbe.Load(), requestsDone.Load(), statusDone.Load())
	}
	if m.Status().Reloading {
		t.Fatal("Reloading flag must be false once all reloads settled")
	}
	log.Printf("INPUT %d slow reloads interleaved with 16 request workers and 6 status pollers\n"+
		"JUDGEMENT requests=%d statusReads=%d incoherent=0 blockedProbes=0 final coherent v%d (plugin=%s)",
		reloads, requestsDone.Load(), statusDone.Load(), port, p.Name())
}
