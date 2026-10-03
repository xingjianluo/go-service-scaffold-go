// Generation lifecycle: lease-based in-flight protection, bounded retirement
// and observable reclamation of displaced snapshot generations.
//
// Read lifecycle:
//
//   - Users call Guard.Acquire before touching a generation and Release the
//     returned Lease when the unit of work is done. The Lease pins one exact
//     generation for its whole lifetime: every access goes to the same Config
//     and the same Plugins, regardless of how many reloads happen meanwhile.
//   - A successful Reload publishes a brand-new generation with a single
//     atomic decision (under Guard.mu): every Acquire after that point gets
//     the new generation; every Lease acquired earlier keeps the old one.
//   - A displaced generation enters GenRetiring. Its plugins are closed only
//     after ALL of its leases have been released, so an in-flight request can
//     never observe torn state or a prematurely closed plugin.
//   - Retirement is bounded by GuardConfig.MaxRetiredGenerations. When the
//     bound is reached, new reloads are REFUSED with
//     ReloadCapacityExceeded (OldKept=true) instead of force-closing a
//     generation that is still in use or growing the set without limit.
//   - Reload never waits on slow users: a generation with leases outstanding
//     is simply left in GenRetiring; the last Release performs reclamation
//     (event-driven, no blocking goroutine per generation).
//   - Shutdown optionally waits ShutdownDrainTimeout for leases to drain;
//     past the deadline every remaining generation is reclaimed forcibly
//     (ReclaimForcedShutdown) so the process is never stuck behind one user.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
)

// Additional reload failure kinds. Values start well above the existing
// FailureKind constants in runtime.go so existing numbers never change.
const (
	// ReloadCapacityExceeded means a reload was refused because the configured
	// number of retired-but-still-in-use generations is already at capacity.
	ReloadCapacityExceeded FailureKind = iota + 10
	// ReloadShuttingDown means the Guard has been shut down.
	ReloadShuttingDown
)

// ErrNoActiveGeneration is returned by Acquire when no generation is in
// service (before the first publish or after Shutdown).
var ErrNoActiveGeneration = errors.New("runtime: no active generation")

// recentReclaimHistory bounds how many finished generations Status reports.
const recentReclaimHistory = 16

// GenerationState is the lifecycle state of one snapshot generation.
type GenerationState int

const (
	// GenActive is the generation served to new Acquire calls.
	GenActive GenerationState = iota
	// GenRetiring is a displaced generation kept alive only for in-flight
	// leases; new users can no longer acquire it.
	GenRetiring
	// GenReclaimed is a generation whose plugins have been closed.
	GenReclaimed
)

func (s GenerationState) String() string {
	switch s {
	case GenActive:
		return "active"
	case GenRetiring:
		return "retiring"
	case GenReclaimed:
		return "reclaimed"
	default:
		return "unknown"
	}
}

// ReclaimOutcome explains WHY a generation's resources were finally released.
type ReclaimOutcome int

const (
	// ReclaimDrained: every lease was released normally and the generation
	// became empty on its own.
	ReclaimDrained ReclaimOutcome = iota
	// ReclaimForcedShutdown: the service shut down (optionally after a drain
	// deadline) and remaining resources were released while leases existed.
	ReclaimForcedShutdown
)

func (o ReclaimOutcome) String() string {
	switch o {
	case ReclaimDrained:
		return "drained"
	case ReclaimForcedShutdown:
		return "forced_shutdown"
	default:
		return "unknown"
	}
}

// GenerationInfo is an observable, point-in-time copy of generation metadata.
type GenerationInfo struct {
	ID          int64
	State       GenerationState
	LeasesInUse int
	InstalledAt time.Time
	RetiredAt   time.Time
	ReclaimedAt time.Time
	// Outcome is meaningful only when State == GenReclaimed.
	Outcome ReclaimOutcome
	// CloseErr carries plugin close errors observed at reclamation, if any.
	CloseErr error
}

func generationInfo(g *generation) GenerationInfo {
	return GenerationInfo{
		ID:          g.id,
		State:       g.state,
		LeasesInUse: g.leases,
		InstalledAt: g.installedAt,
		RetiredAt:   g.retiredAt,
		ReclaimedAt: g.reclaimedAt,
		Outcome:     g.outcome,
		CloseErr:    g.closeErr,
	}
}

// GuardConfig configures bounded retirement.
type GuardConfig struct {
	// MaxRetiredGenerations bounds how many displaced generations may be kept
	// around while still in use. Zero/negative values default to 1.
	// A reload that would exceed this bound is rejected with
	// ReloadCapacityExceeded; generations in use are never force-closed.
	MaxRetiredGenerations int
	// ShutdownDrainTimeout bounds how long Shutdown waits for in-flight leases
	// before closing plugins anyway. Zero means "do not wait" — plugins are
	// closed immediately, matching the legacy shutdown behavior.
	ShutdownDrainTimeout time.Duration
}

// GuardStatus is the observable state of a Guard.
type GuardStatus struct {
	ActiveID       int64 // -1 when no generation is in service
	Reloading      bool
	ShuttingDown   bool
	Capacity       int
	CapacityUsed   int // number of generations currently waiting for reclamation
	Retiring       []GenerationInfo
	RecentReclaims []GenerationInfo
}

// generation is one internal snapshot generation with its lease bookkeeping.
type generation struct {
	id          int64
	snap        *Snapshot
	state       GenerationState
	leases      int
	installedAt time.Time
	retiredAt   time.Time
	reclaimedAt time.Time
	outcome     ReclaimOutcome
	closeErr    error
}

// Guard owns the active generation and the bounded set of retiring
// generations, handing out Leases to in-flight users.
type Guard struct {
	build    BuildFunc
	capacity int
	drainTO  time.Duration

	// reloadSem serializes reloads; only one reload runs at a time. Other
	// reload callers wait (bounded by their ctx) instead of racing.
	reloadSem chan struct{}

	mu     sync.Mutex
	nextID int64
	active *generation
	// retiring holds displaced generations that have not been reclaimed yet,
	// in retirement order (oldest first). len(retiring) is the capacity use.
	retiring []*generation
	// recent is a bounded history of reclaimed generations (newest last).
	recent    []GenerationInfo
	reloading bool
	down      bool

	// wake is closed (and replaced) whenever a lease is released or the
	// active generation changes. Shutdown selects on it to wait for an
	// orderly drain without holding g.mu or polling.
	wake chan struct{}
}

// notifyLocked wakes every drain waiter. Caller must hold g.mu.
func (g *Guard) notifyLocked() {
	close(g.wake)
	g.wake = make(chan struct{})
}

// NewGuard creates a Guard using build to construct new generations.
func NewGuard(build BuildFunc, cfg GuardConfig) *Guard {
	capacity := cfg.MaxRetiredGenerations
	if capacity < 1 {
		capacity = 1
	}
	g := &Guard{
		build:     build,
		capacity:  capacity,
		drainTO:   cfg.ShutdownDrainTimeout,
		reloadSem: make(chan struct{}, 1),
		wake:      make(chan struct{}),
	}
	return g
}

// Install publishes an externally-built snapshot as the first generation.
// It fails if a generation is already active or the Guard has shut down.
func (g *Guard) Install(snap *Snapshot) error {
	if snap == nil || snap.Config == nil {
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "cannot install an empty snapshot",
			OldKept: true,
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.down {
		return &ReloadError{Kind: ReloadShuttingDown, Reason: "guard is shut down", OldKept: true}
	}
	if g.active != nil {
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "a generation is already active; use Reload to replace it",
			OldKept: true,
		}
	}
	g.active = g.newGenerationLocked(snap)
	return nil
}

// newGenerationLocked assigns the next id. Caller must hold g.mu.
func (g *Guard) newGenerationLocked(snap *Snapshot) *generation {
	g.nextID++
	return &generation{
		id:          g.nextID,
		snap:        snap,
		state:       GenActive,
		installedAt: time.Now(),
	}
}

// Lease pins one generation for the duration of one unit of work.
type Lease struct {
	guard    *Guard
	gen      *generation
	released bool
}

// Snapshot returns the pinned, immutable generation snapshot. It stays valid
// for the lease's whole lifetime even if reloads replace the active generation.
func (l *Lease) Snapshot() *Snapshot { return l.gen.snap }

// GenerationID reports which generation this lease belongs to.
func (l *Lease) GenerationID() int64 { return l.gen.id }

// Release unpins the generation. It is safe to call more than once; the
// second and later calls are no-ops. When the last lease of a retiring
// generation is released, its plugins are closed exactly once.
func (l *Lease) Release() {
	g := l.guard
	g.mu.Lock()
	if l.released {
		g.mu.Unlock()
		return
	}
	l.released = true
	gen := l.gen
	gen.leases--
	if gen.leases < 0 {
		// Defensive: indicates a bug in this package, not user code.
		g.mu.Unlock()
		panic("runtime: generation lease count went negative")
	}
	var toClose *generation
	if gen.leases == 0 && gen.state == GenRetiring {
		// Claim under the lock so racing Shutdown can never close the same
		// plugins a second time.
		toClose = g.claimLocked(gen)
	}
	g.notifyLocked()
	g.mu.Unlock()

	if toClose != nil {
		g.finishReclaim(toClose, ReclaimDrained)
	}
}

// claimLocked removes gen from the retiring list and marks it reclaimed
// pending the actual ClosePlugins call. Returns gen when the caller owns the
// reclamation, nil when another path (Shutdown) already claimed it.
// Caller must hold g.mu.
func (g *Guard) claimLocked(gen *generation) *generation {
	if gen.state == GenReclaimed {
		return nil
	}
	for i, rg := range g.retiring {
		if rg == gen {
			g.retiring = append(g.retiring[:i], g.retiring[i+1:]...)
			break
		}
	}
	gen.state = GenReclaimed
	return gen
}

// finishReclaim closes the generation's plugins and records the outcome in
// the reclamation history. The caller MUST have claimed the generation under
// g.mu (claimLocked), guaranteeing exactly one closer. It is called without
// holding g.mu so a slow plugin Close never blocks Acquire/Reload/Status.
func (g *Guard) finishReclaim(gen *generation, outcome ReclaimOutcome) {
	cerr := ClosePlugins(gen.snap.Plugins)

	g.mu.Lock()
	defer g.mu.Unlock()
	gen.outcome = outcome
	gen.reclaimedAt = time.Now()
	gen.closeErr = cerr
	info := generationInfo(gen)
	g.recent = append(g.recent, info)
	if len(g.recent) > recentReclaimHistory {
		g.recent = g.recent[len(g.recent)-recentReclaimHistory:]
	}
	g.notifyLocked()
}

// Acquire pins the currently active generation. A caller that gets a non-nil
// lease is guaranteed to keep using that exact generation until Release.
func (g *Guard) Acquire() (*Lease, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.down || g.active == nil {
		return nil, ErrNoActiveGeneration
	}
	gen := g.active
	gen.leases++
	return &Lease{guard: g, gen: gen}, nil
}

// acquireReloadLock serializes reloads, honoring ctx cancellation while
// waiting for another reload to finish.
func (g *Guard) acquireReloadLock(ctx context.Context) error {
	select {
	case g.reloadSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return &ReloadError{
			Kind:    ReloadTimeout,
			Reason:  "canceled or timed out while waiting for an in-progress reload",
			WrapErr: ctx.Err(),
			OldKept: true,
		}
	}
}

func (g *Guard) releaseReloadLock() { <-g.reloadSem }

// Reload builds a new generation, publishes it atomically and retires the
// previously active one:
//
//   - Build failures (incl. ctx cancellation) leave every generation untouched.
//   - When the retired-generation capacity is already full, the reload is
//     refused with ReloadCapacityExceeded; the active generation keeps
//     serving and no in-use generation is closed.
//   - A displaced generation with no outstanding leases is reclaimed
//     synchronously before Reload returns (matching the historical behavior);
//     one still in use is reclaimed by the last Release.
func (g *Guard) Reload(ctx context.Context, cfg *config.Config) error {
	if err := g.acquireReloadLock(ctx); err != nil {
		return err
	}
	defer g.releaseReloadLock()

	g.mu.Lock()
	g.reloading = true
	defer func() {
		g.mu.Lock()
		g.reloading = false
		g.notifyLocked()
		g.mu.Unlock()
	}()

	if g.down {
		g.mu.Unlock()
		return &ReloadError{Kind: ReloadShuttingDown, Reason: "guard is shut down", OldKept: true}
	}
	if cfg == nil {
		g.mu.Unlock()
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "cannot reload from nil configuration",
			OldKept: true,
		}
	}
	if len(g.retiring) >= g.capacity {
		ids := make([]string, 0, len(g.retiring))
		for _, rg := range g.retiring {
			ids = append(ids, fmt.Sprintf("v%d(leases=%d)", rg.id, rg.leases))
		}
		g.mu.Unlock()
		return &ReloadError{
			Kind: ReloadCapacityExceeded,
			Reason: fmt.Sprintf(
				"reload refused: %d retired generation(s) still in use and capacity is %d [%s]; "+
					"release the held leases and retry",
				len(ids), g.capacity, strings.Join(ids, ", ")),
			OldKept: true,
		}
	}
	g.mu.Unlock()

	// Build the new world while every existing generation keeps serving.
	next, err := g.build(ctx, cfg)
	if err != nil {
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
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "build returned an empty snapshot; previous snapshot remains in service",
			OldKept: true,
		}
	}

	g.mu.Lock()
	if g.down {
		// Shutdown raced the build. Close the freshly built snapshot so its
		// resources do not leak, and report the guard as shut down.
		g.mu.Unlock()
		_ = ClosePlugins(next.Plugins)
		return &ReloadError{Kind: ReloadShuttingDown, Reason: "guard shut down while building", OldKept: true}
	}
	gen := g.newGenerationLocked(next)
	prev := g.active
	g.active = gen
	var emptyRetired *generation
	if prev != nil {
		prev.state = GenRetiring
		prev.retiredAt = time.Now()
		g.retiring = append(g.retiring, prev)
		if prev.leases == 0 {
			emptyRetired = g.claimLocked(prev)
		}
	}
	g.notifyLocked()
	g.mu.Unlock()

	if emptyRetired != nil {
		// Mirrors the historical "close the displaced generation before
		// returning" behavior; no slow user can be involved (0 leases).
		g.finishReclaim(emptyRetired, ReclaimDrained)
	}
	return nil
}

// totalOutstandingLeases counts leases across the active and all retiring
// generations. Caller must hold g.mu.
func (g *Guard) totalOutstandingLeasesLocked() int {
	n := 0
	if g.active != nil {
		n += g.active.leases
	}
	for _, rg := range g.retiring {
		n += rg.leases
	}
	return n
}

// Shutdown removes the active generation and reclaims every generation.
// It waits up to GuardConfig.ShutdownDrainTimeout for in-flight leases to
// drain; generations still occupied past the deadline are closed forcibly.
// Safe to call repeatedly.
func (g *Guard) Shutdown(ctx context.Context) error {
	if err := g.acquireReloadLock(ctx); err != nil {
		return err
	}
	defer g.releaseReloadLock()

	g.mu.Lock()
	if g.down {
		g.mu.Unlock()
		return nil
	}
	g.down = true
	g.reloading = true
	defer func() {
		g.mu.Lock()
		g.reloading = false
		g.notifyLocked()
		g.mu.Unlock()
	}()

	if g.active != nil {
		g.active.state = GenRetiring
		g.active.retiredAt = time.Now()
		g.retiring = append(g.retiring, g.active)
		g.active = nil
	}
	g.notifyLocked()

	// Drain wait (event-driven + short safety tick): bounded by the
	// configured timeout and ctx. The main reload flow never does this;
	// only explicit Shutdown waits, and never beyond the deadline.
	wait := g.drainTO > 0
	if wait {
		deadline := time.Now().Add(g.drainTO)
		for g.totalOutstandingLeasesLocked() > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 || (ctx != nil && ctx.Err() != nil) {
				break
			}
			ch := g.wake
			tick := minDuration(10*time.Millisecond, remaining)
			g.mu.Unlock()
			select {
			case <-ch: // a lease was released; re-check
			case <-time.After(tick): // safety re-check near the deadline
			case <-ctxDone(ctx):
			}
			g.mu.Lock()
		}
	}
	// Claim every still-pending generation exactly once. Anything that
	// reached zero leases during the wait is recorded as a normal drain;
	// anything still occupied is force-reclaimed.
	toReclaim := make([]*generation, 0, len(g.retiring))
	outcomes := make([]ReclaimOutcome, 0, len(g.retiring))
	for _, gen := range g.retiring {
		outcome := ReclaimForcedShutdown
		if gen.leases == 0 {
			outcome = ReclaimDrained
		}
		if claimed := g.claimLocked(gen); claimed != nil {
			toReclaim = append(toReclaim, claimed)
			outcomes = append(outcomes, outcome)
		}
	}
	g.retiring = nil
	g.mu.Unlock()

	var errs []string
	for i, gen := range toReclaim {
		g.finishReclaim(gen, outcomes[i])
		if gen.closeErr != nil {
			errs = append(errs, fmt.Sprintf("generation v%d: %v", gen.id, gen.closeErr))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown completed with plugin close errors: %s",
			strings.Join(errs, "; "))
	}
	return nil
}

func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// Status returns an observable point-in-time copy: which generation is
// active, whether a reload is running, capacity usage and the detailed state
// of generations waiting for reclamation plus recently reclaimed ones
// (including their reclaim outcome).
func (g *Guard) Status() GuardStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	st := GuardStatus{
		ActiveID:     -1,
		Reloading:    g.reloading,
		ShuttingDown: g.down,
		Capacity:     g.capacity,
		CapacityUsed: len(g.retiring),
	}
	if g.active != nil {
		st.ActiveID = g.active.id
	}
	st.Retiring = make([]GenerationInfo, 0, len(g.retiring))
	for _, rg := range g.retiring {
		st.Retiring = append(st.Retiring, generationInfo(rg))
	}
	st.RecentReclaims = make([]GenerationInfo, len(g.recent))
	copy(st.RecentReclaims, g.recent)
	return st
}
