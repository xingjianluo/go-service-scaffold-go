// This file adds the generation/lease layer on top of the atomic snapshot
// primitives in runtime.go. See package documentation in runtime.go for the
// full read-lifecycle and reclamation guarantees.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
)

// GenerationState describes where a generation currently is in its lifecycle.
type GenerationState int

const (
	// GenActive is the generation currently served to new readers.
	GenActive GenerationState = iota
	// GenDraining is a retired generation that still holds one or more leases.
	GenDraining
	// GenReclaimed means the generation reached zero leases and its plugins
	// were closed (successfully or not); it no longer counts against capacity.
	GenReclaimed
	// GenStuck means the drain timeout elapsed while leases were still held.
	// The generation is still usable by its lease holders and still counts
	// against capacity; it is never forcibly closed.
	GenStuck
)

func (s GenerationState) String() string {
	switch s {
	case GenActive:
		return "active"
	case GenDraining:
		return "draining"
	case GenReclaimed:
		return "reclaimed"
	case GenStuck:
		return "stuck"
	default:
		return "unknown"
	}
}

// DrainOutcome records why a generation left the draining/retained set.
type DrainOutcome int

const (
	// OutcomeNone: not reclaimed yet.
	OutcomeNone DrainOutcome = iota
	// OutcomeDrained: reclaimed normally because all leases were released.
	OutcomeDrained
	// OutcomeDrainTimeout: drain timeout elapsed before all leases were gone.
	OutcomeDrainTimeout
	// OutcomeCloseFailed: reached zero leases but one or more Close calls failed.
	OutcomeCloseFailed
	// OutcomeShutdown: reclaimed as part of Manager.Shutdown.
	OutcomeShutdown
)

func (o DrainOutcome) String() string {
	switch o {
	case OutcomeDrained:
		return "drained"
	case OutcomeDrainTimeout:
		return "drain_timeout"
	case OutcomeCloseFailed:
		return "close_failed"
	case OutcomeShutdown:
		return "shutdown"
	default:
		return "none"
	}
}

// GenerationInfo is an immutable snapshot of one generation's bookkeeping.
type GenerationInfo struct {
	ID      uint64
	State   GenerationState
	Leases  int
	Outcome DrainOutcome
	// DrainTimedOut is true once the drain timeout ever elapsed for this
	// generation, even if it was eventually reclaimed after all leases ended.
	DrainTimedOut bool
	CloseErr      string
	BornAt        time.Time
	RetiredAt     time.Time
	ClosedAt      time.Time
}

// ManagerStatus is the observable state of a Manager.
type ManagerStatus struct {
	ActiveVersion uint64
	Reloading     bool
	// Retained lists retired-but-not-reclaimed generations (draining or stuck),
	// oldest first.
	Retained []GenerationInfo
	// History lists the most recently reclaimed generations, newest first,
	// bounded so observability cannot grow without limit.
	History []GenerationInfo
}

// Options configures a Manager.
type Options struct {
	// MaxRetainedGenerations bounds how many retired generations may remain
	// unreclaimed (draining or stuck) at once. 0 means a sane package default.
	MaxRetainedGenerations int
	// DrainTimeout bounds how long a retired generation may wait for leases
	// before it is reported as stuck. A negative value means wait forever;
	// zero selects the package default.
	DrainTimeout time.Duration
	// ReapTimeout is how long Shutdown waits for leased generations before
	// giving up waiting. A negative value means wait forever; zero selects the
	// package default.
	ReapTimeout time.Duration
}

// Lease pins one generation for the duration of one unit of work. The pinned
// Snapshot (and its plugins) are guaranteed to stay open until Release.
type Lease struct {
	mgr *Manager
	gen *generation

	releaseOnce sync.Once
}

// Snapshot returns the pinned generation. It stays complete and open for the
// whole lifetime of the lease.
func (l *Lease) Snapshot() *Snapshot { return l.gen.snap }

// Release unpins the generation. It is safe to call more than once; calls
// after the first are no-ops. Plugin closing, when this was the last lease,
// happens asynchronously so a slow Close can never block the releasing
// caller or the reload main path.
func (l *Lease) Release() {
	l.releaseOnce.Do(func() {
		l.mgr.releaseLease(l.gen)
	})
}

// Default capacity / timing used when Options leaves a field at zero.
const (
	DefaultMaxRetainedGenerations = 2
	DefaultDrainTimeout           = 30 * time.Second
	DefaultReapTimeout            = 10 * time.Second
	// historyLimit bounds how many reclaimed generations Status keeps for
	// observability, so bookkeeping itself cannot grow without limit.
	historyLimit = 16
)

// generation is the internal bookkeeping record for one served version.
type generation struct {
	id            uint64
	snap          *Snapshot
	leases        int
	state         GenerationState
	outcome       DrainOutcome
	drainTimedOut bool
	closeErr      string
	bornAt        time.Time
	retiredAt     time.Time
	closedAt      time.Time
	timer         *time.Timer
}

func (g *generation) info() GenerationInfo {
	return GenerationInfo{
		ID:            g.id,
		State:         g.state,
		Leases:        g.leases,
		Outcome:       g.outcome,
		DrainTimedOut: g.drainTimedOut,
		CloseErr:      g.closeErr,
		BornAt:        g.bornAt,
		RetiredAt:     g.retiredAt,
		ClosedAt:      g.closedAt,
	}
}

// Manager owns generations: it serves the active one via leases, retires old
// ones on reload, reclaims them after their leases drain, and enforces the
// retained-generation capacity.
type Manager struct {
	build BuildFunc
	opts  Options

	mu        sync.Mutex
	active    *generation
	retained  []*generation // retired but not reclaimed (draining/stuck), oldest first
	history   []*generation // reclaimed, newest first, capped at historyLimit
	reloading bool
	closed    bool
	nextID    uint64
	cond      *sync.Cond

	// current is the single atomic publication point readers load without
	// taking mu. It always points to the active generation, or nil before the
	// first successful load / after shutdown.
	current atomic.Pointer[generation]
}

// NewManager creates a Manager with no active generation. Zero-valued Options
// fields are replaced by package defaults. A negative DrainTimeout or
// ReapTimeout disables that timeout (wait forever); a negative capacity
// selects the default capacity.
func NewManager(build BuildFunc, opts Options) *Manager {
	if opts.MaxRetainedGenerations <= 0 {
		opts.MaxRetainedGenerations = DefaultMaxRetainedGenerations
	}
	if opts.DrainTimeout == 0 {
		opts.DrainTimeout = DefaultDrainTimeout
	}
	if opts.ReapTimeout == 0 {
		opts.ReapTimeout = DefaultReapTimeout
	}
	m := &Manager{build: build, opts: opts}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// Acquire pins the active generation for one unit of work. It returns nil
// before the first successful load or after Shutdown. The caller MUST call
// Release when the unit of work is finished; until then the pinned snapshot's
// plugins are guaranteed not to be closed.
func (m *Manager) Acquire() *Lease {
	// Optimistic lock-free read of the publication point...
	g := m.current.Load()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	// ...then validate under the bookkeeping lock. A generation loaded just
	// before a swap may already be retired; in that race pin the new active
	// generation instead of returning nil.
	if g == nil || g.state != GenActive {
		g = m.active
	}
	if g == nil {
		return nil
	}
	g.leases++
	return &Lease{mgr: m, gen: g}
}

// CurrentSnapshot returns the served snapshot without pinning it. It never
// returns nil while the manager is up, but the result is NOT protected
// against concurrent reclamation: use Acquire for any work that calls into
// the snapshot's plugins.
func (m *Manager) CurrentSnapshot() *Snapshot {
	g := m.current.Load()
	if g == nil {
		return nil
	}
	return g.snap
}

// releaseLease drops one reference and, on the last reference, kicks off
// asynchronous reclamation so Close can never block the releaser.
func (m *Manager) releaseLease(g *generation) {
	var reap *generation
	m.mu.Lock()
	if g.leases > 0 {
		g.leases--
	}
	if g.leases == 0 && g.state != GenActive && g.state != GenReclaimed {
		// Detach under the lock: the generation stops counting against
		// capacity and is hidden from Retained immediately.
		reap = m.detachForReapLocked(g)
	}
	m.mu.Unlock()
	if reap != nil {
		go m.reaperClose(reap)
	}
}

// drainTimeout marks a retired generation as stuck when its leases outlive
// the configured drain timeout. It is never closed; it simply becomes
// distinguishable in Status and still counts against capacity.
func (m *Manager) drainTimeout(g *generation) {
	m.mu.Lock()
	if g.state == GenDraining && g.leases > 0 {
		g.state = GenStuck
		g.drainTimedOut = true
	}
	m.mu.Unlock()
}

// detachForReapLocked removes g from the retained slice and flips it to the
// reclaimed bookkeeping state (closing happens later, outside the lock).
func (m *Manager) detachForReapLocked(g *generation) *generation {
	for i, r := range m.retained {
		if r == g {
			m.retained = append(m.retained[:i], m.retained[i+1:]...)
			break
		}
	}
	return m.markReapingLocked(g)
}

// markReapingLocked flips a generation into the reclaimed bookkeeping state
// and cancels its drain timer. Closing itself happens outside the lock.
func (m *Manager) markReapingLocked(g *generation) *generation {
	if g.timer != nil {
		g.timer.Stop()
		g.timer = nil
	}
	g.state = GenReclaimed
	m.cond.Broadcast()
	return g
}

// pushHistoryLocked records a fully closed generation, newest first, bounded.
func (m *Manager) pushHistoryLocked(g *generation) {
	m.history = append([]*generation{g}, m.history...)
	if len(m.history) > historyLimit {
		m.history = m.history[:historyLimit]
	}
	m.cond.Broadcast()
}

// reaperClose performs the actual plugin Close outside the manager lock and
// then finalizes the record. Used for generations drained asynchronously on
// their last Release.
func (m *Manager) reaperClose(g *generation) {
	closeErr := ClosePlugins(g.snap.Plugins)

	m.mu.Lock()
	defer m.mu.Unlock()
	g.closedAt = time.Now()
	switch {
	case closeErr != nil:
		g.outcome = OutcomeCloseFailed
		g.closeErr = closeErr.Error()
	case g.drainTimedOut:
		g.outcome = OutcomeDrainTimeout
	case m.closed:
		g.outcome = OutcomeShutdown
	default:
		g.outcome = OutcomeDrained
	}
	m.pushHistoryLocked(g)
}

// Reload builds a new generation and publishes it atomically.
//
// Capacity policy: before building, if the number of retired-but-unreclaimed
// generations already equals MaxRetainedGenerations, the reload is refused
// with a *ReloadError of Kind ReloadDrainFull. No in-use generation is ever
// closed to make room and nothing is built.
//
// Build policy mirrors Reloader: a build failure (or a canceled context)
// leaves the active generation completely untouched.
//
// After publication the previous generation is retired. Retirement never
// blocks waiting for leases: with zero leases it is reclaimed right away;
// otherwise it drains in the background (up to DrainTimeout, then it is
// reported stuck).
func (m *Manager) Reload(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "cannot reload from nil configuration",
			OldKept: true,
		}
	}

	// Reloads are serialized for the whole build+publish sequence: the
	// capacity decision and the publication must be atomic, otherwise two
	// concurrent reloads could both pass the capacity check and overshoot it.
	// Reading the served generation (Acquire's atomic load) stays lock-free;
	// only the short lease-pin waits behind an in-progress build.
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "manager is shut down; reload refused",
			OldKept: true,
		}
	}
	if len(m.retained) >= m.opts.MaxRetainedGenerations {
		stuck, draining := m.retainedCountsLocked()
		m.mu.Unlock()
		return &ReloadError{
			Kind: ReloadDrainFull,
			Reason: fmt.Sprintf("reload refused: %d old generation(s) still in use at capacity %d "+
				"(draining=%d stuck=%d); old states are kept intact",
				stuck+draining, m.opts.MaxRetainedGenerations, draining, stuck),
			OldKept: true,
		}
	}
	m.reloading = true

	// Build the new world while the old one keeps serving. Manager state
	// cannot change here because Reload holds mu (and Shutdown takes it too).
	next, err := m.build(ctx, cfg)
	if err != nil {
		m.reloading = false
		m.cond.Broadcast()
		m.mu.Unlock()
		kind := ReloadBuildFailed
		reason := "snapshot build failed; previous snapshot remains in service"
		if ctxErr := ctx.Err(); ctxErr != nil {
			kind = ReloadTimeout
			reason = "reload canceled or timed out while building snapshot"
			err = fmt.Errorf("%w: %v", ctxErr, err)
		}
		return &ReloadError{Kind: kind, Reason: reason, WrapErr: err, OldKept: true}
	}
	if next == nil || next.Config == nil {
		m.reloading = false
		m.cond.Broadcast()
		m.mu.Unlock()
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "build returned an empty snapshot; previous snapshot remains in service",
			OldKept: true,
		}
	}

	m.nextID++
	ng := &generation{id: m.nextID, snap: next, state: GenActive, bornAt: time.Now()}
	prev := m.active
	m.active = ng
	m.reloading = false
	// Single atomic publication point.
	m.current.Store(ng)
	var syncReap *generation
	if prev != nil {
		syncReap = m.retireLocked(prev)
	}
	m.cond.Broadcast()
	m.mu.Unlock()

	// A displaced generation with no outstanding leases is reclaimed before
	// Reload returns (backward-compatible teardown semantics) but the plugin
	// Close happens outside the lock. An in-use generation drains
	// asynchronously, so a slow lease holder can never block the reload path.
	if syncReap != nil {
		if err := m.reaperCloseSync(syncReap, false); err != nil {
			return &ReloadError{
				Kind:    ReloadFailureUnknown,
				Reason:  "new generation is active, but closing the previous generation reported errors",
				WrapErr: err,
				OldKept: false,
			}
		}
	}
	return nil
}

// retainedCountsLocked splits retained generations into stuck vs draining.
func (m *Manager) retainedCountsLocked() (stuck, draining int) {
	for _, g := range m.retained {
		if g.state == GenStuck {
			stuck++
		} else {
			draining++
		}
	}
	return stuck, draining
}

// retireLocked moves a displaced active generation into the retained set and
// starts its drain. It returns a non-nil generation when that generation has
// no leases and should be reclaimed synchronously by the caller (Close itself
// must run outside the lock). Generations still in use stay open and complete
// later asynchronously on their last Release.
func (m *Manager) retireLocked(g *generation) *generation {
	g.retiredAt = time.Now()
	if g.leases == 0 {
		return m.markReapingLocked(g)
	}
	g.state = GenDraining
	m.retained = append(m.retained, g)
	if d := m.opts.DrainTimeout; d > 0 {
		g.timer = time.AfterFunc(d, func() { m.drainTimeout(g) })
	}
	return nil
}

// Status returns the observable generation bookkeeping: the active version,
// whether a reload is in progress, every retired-but-unreclaimed generation
// (draining or stuck, with lease counts and the stuck flag), and a bounded
// history of reclaimed generations with their terminal outcomes.
func (m *Manager) Status() ManagerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := ManagerStatus{Reloading: m.reloading}
	if m.active != nil {
		st.ActiveVersion = m.active.id
	}
	st.Retained = make([]GenerationInfo, 0, len(m.retained))
	for _, g := range m.retained {
		st.Retained = append(st.Retained, g.info())
	}
	st.History = make([]GenerationInfo, 0, len(m.history))
	for _, g := range m.history {
		st.History = append(st.History, g.info())
	}
	return st
}

// ActiveVersion is a convenience accessor returning the served generation ID.
// It returns 0 before the first successful load / after shutdown.
func (m *Manager) ActiveVersion() uint64 {
	g := m.current.Load()
	if g == nil {
		return 0
	}
	return g.id
}

// Shutdown atomically removes the active generation and reclaims every
// generation that is not currently leased. Leased generations keep serving
// their holders and are reclaimed on their last Release (labeled with
// OutcomeShutdown unless they had already hit the drain timeout). It waits up
// to opts.ReapTimeout (or the deadline/cancellation of ctx, whichever comes
// first) for in-use generations to drain; generations still leased after that
// are left open rather than closed under live users. It is safe to call
// repeatedly. Close errors from synchronously reclaimed generations are
// aggregated and returned; they never block shutdown of the others.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.reloading = false

	var toClose []*generation
	if m.active != nil {
		active := m.active
		m.active = nil
		m.current.Store(nil)
		if active.leases == 0 {
			// Active with no leases is not in retained; detach is a direct flip.
			toClose = append(toClose, m.markReapingLocked(active))
		} else {
			m.retireLocked(active)
		}
	}
	// Collect zero-lease retained generations, then detach them after the
	// scan (detaching mutates the retained slice in place).
	ready := make([]*generation, 0)
	alive := m.retained[:0]
	for _, g := range m.retained {
		if g.leases == 0 {
			ready = append(ready, g)
		} else {
			alive = append(alive, g)
		}
	}
	m.retained = alive
	for _, g := range ready {
		toClose = append(toClose, m.detachForReapLocked(g))
	}

	// Wait (bounded) for the rest to drain naturally.
	deadline := time.Now().Add(m.opts.ReapTimeout)
	for len(m.retained) > 0 {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if m.opts.ReapTimeout > 0 && !time.Now().Before(deadline) {
			break
		}
		m.waitSignalLocked(ctx, deadline)
		ready = ready[:0]
		alive = m.retained[:0]
		for _, g := range m.retained {
			if g.leases == 0 {
				ready = append(ready, g)
			} else {
				alive = append(alive, g)
			}
		}
		m.retained = alive
		for _, g := range ready {
			toClose = append(toClose, m.detachForReapLocked(g))
		}
	}
	m.mu.Unlock()

	var errs []error
	for _, g := range toClose {
		// These were detached with leases==0 and Shutdown owns them.
		if err := m.reaperCloseSync(g, true); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// waitSignalLocked waits on the cond until the remaining reap budget elapses
// or ctx is canceled, whichever happens first. Wakeups are spurious-safe
// because the caller re-checks its predicate afterward. Must be called with
// m.mu held (which cond.Wait temporarily releases).
func (m *Manager) waitSignalLocked(ctx context.Context, deadline time.Time) {
	wake := func() {
		m.mu.Lock()
		m.cond.Broadcast()
		m.mu.Unlock()
	}
	if wait := time.Until(deadline); wait > 0 {
		t := time.AfterFunc(wait, wake)
		defer t.Stop()
	}
	if ctx != nil {
		stop := context.AfterFunc(ctx, wake)
		defer stop()
	}
	m.cond.Wait()
}

// reaperCloseSync closes a detached generation synchronously and returns its
// aggregated close error. onShutdown selects OutcomeShutdown vs OutcomeDrained
// for the terminal label; close failures and prior drain-timeouts take
// precedence either way.
func (m *Manager) reaperCloseSync(g *generation, onShutdown bool) error {
	closeErr := ClosePlugins(g.snap.Plugins)
	m.mu.Lock()
	defer m.mu.Unlock()
	g.closedAt = time.Now()
	switch {
	case closeErr != nil:
		g.outcome = OutcomeCloseFailed
		g.closeErr = closeErr.Error()
	case g.drainTimedOut:
		g.outcome = OutcomeDrainTimeout
	case onShutdown:
		g.outcome = OutcomeShutdown
	default:
		g.outcome = OutcomeDrained
	}
	m.pushHistoryLocked(g)
	return closeErr
}
