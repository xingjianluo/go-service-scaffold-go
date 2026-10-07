// Package runtime provides atomic configuration/plugin snapshots,
// coordinated hot reload, and generation-scoped leases.
//
// Two layers are exposed:
//
//   - Primitives: Holder (an atomic pointer to a Snapshot), Reloader and
//     ClosePlugins. Reloader swaps snapshots and immediately closes the
//     displaced plugins; use it only when no long-lived work calls into a
//     snapshot's plugins.
//   - Generation manager: Manager + Lease. This is the layer services should
//     use. Every reload publishes a new numbered generation; a unit of work
//     Acquires a Lease that pins one generation and Releases it when done.
//
// Read lifecycle:
//
//  1. The new generation is built (config validated, plugins initialized)
//     BEFORE the old one is touched. A build failure leaves the serving
//     generation completely untouched and returns a *ReloadError with a
//     distinguishable FailureKind.
//  2. The build runs WITHOUT the bookkeeping lock (it is serialized by a
//     separate reload lock). Therefore, while a reload is still preparing:
//     Status returns immediately with Reloading=true and the still-serving
//     ActiveVersion; Acquire pins and serves the active generation at once;
//     Release keeps reclaiming retired generations. No read or request ever
//     waits for a slow build, no matter how long it takes.
//  3. Publication is a single atomic pointer swap: Acquire (and
//     CurrentSnapshot) always observe either the complete old generation or
//     the complete new one, never a partially-merged state. Work that started
//     on the old generation keeps using the SAME config AND plugins until it
//     calls Release; those plugins cannot be closed underneath it.
//  4. New work arriving after the swap immediately pins the new generation.
//  5. A retired generation is reclaimed (plugins closed in reverse
//     initialization order) only after ALL its leases are released. The
//     release path never blocks on Close (closing is asynchronous); the
//     reload main path never waits for a slow lease holder.
//
// Reload convergence: whether a reload succeeds, its preparation fails, or it
// is canceled/times out while building, the Reloading flag always converges
// back to false and a failed/timed-out attempt never mutates the served
// generation (and consumes no generation ID or retained capacity).
//
// Reclamation observability (Manager.Status):
//
//   - ActiveVersion: the generation currently served to new work.
//   - Reloading: whether a reload build+publish is in progress.
//   - Retained: retired-but-unreclaimed generations, each with its lease
//     count and state: "draining" (still waiting inside the drain budget) or
//     "stuck" (the DrainTimeout elapsed while leases were held). Stuck
//     generations remain fully usable by their holders and are never forcibly
//     closed; they are reclaimed normally on their last Release.
//   - History: a bounded list of the most recently reclaimed generations with
//     the terminal outcome: drained (normal), drain_timeout (users overran
//     the budget but eventually released), close_failed (one or more Close
//     calls failed; the error text is attached), or shutdown.
//
// Capacity and refusal policy:
//
// MaxRetainedGenerations bounds how many drained/stuck generations may
// accumulate. While below the limit, reloads proceed and old generations wait
// for their users. At the limit, a new reload is refused BEFORE building with
// ReloadError kind ReloadDrainFull (OldKept=true): no in-use generation is
// torn down and memory/resource usage cannot grow without bound. Retained
// generations that later reach zero leases free capacity again, and even a
// close_failed generation frees its slot (its failure remains visible in
// History).
package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/plugin"
)

// Snapshot is an immutable view of config together with its plugins.
// Callers MUST NOT mutate Plugins; treat every snapshot as read-only.
type Snapshot struct {
	Config  *config.Config
	Plugins []plugin.Plugin
}

// Plugin returns the plugin with the given name from the snapshot.
func (s *Snapshot) Plugin(name string) (plugin.Plugin, bool) {
	if s == nil {
		return nil, false
	}
	for _, p := range s.Plugins {
		if p.Name() == name {
			return p, true
		}
	}
	return nil, false
}

// Holder provides lock-free atomic access to the current snapshot.
type Holder struct {
	current atomic.Pointer[Snapshot]
}

// NewHolder creates a Holder. initial may be nil and installed later via Swap.
func NewHolder(initial *Snapshot) *Holder {
	h := &Holder{}
	if initial != nil {
		h.current.Store(initial)
	}
	return h
}

// Current returns the active snapshot, or nil before the first successful load.
func (h *Holder) Current() *Snapshot {
	return h.current.Load()
}

// Swap atomically replaces the snapshot and returns the previous one
// (nil when there was none).
func (h *Holder) Swap(next *Snapshot) *Snapshot {
	return h.current.Swap(next)
}

// BuildFunc builds a ready-to-serve snapshot from raw configuration.
// Implementations MUST return a fully initialized, internally consistent
// snapshot or an error; partial results must be cleaned up before returning.
type BuildFunc func(ctx context.Context, cfg *config.Config) (*Snapshot, error)

// FailureKind classifies reload failures.
type FailureKind int

const (
	ReloadFailureUnknown FailureKind = iota
	ReloadBuildFailed
	ReloadTimeout
	// ReloadDrainFull means the reload was refused BEFORE building the new
	// generation because the number of retired-but-unreclaimed generations
	// already equals the configured capacity. No in-use generation is ever
	// torn down to make room.
	ReloadDrainFull
)

func (k FailureKind) String() string {
	switch k {
	case ReloadBuildFailed:
		return "reload_build_failed"
	case ReloadTimeout:
		return "reload_timeout"
	case ReloadDrainFull:
		return "reload_drain_full"
	default:
		return "reload_unknown"
	}
}

// ReloadError describes a failed reload. The previous snapshot stays in
// service; OldKept is always true for the kinds produced here.
type ReloadError struct {
	Kind    FailureKind
	Reason  string
	WrapErr error
	OldKept bool
}

func (e *ReloadError) Error() string {
	return fmt.Sprintf("hot reload failed (kind=%s, old-state-kept=%t): %s: %v",
		e.Kind, e.OldKept, e.Reason, e.WrapErr)
}

func (e *ReloadError) Unwrap() error { return e.WrapErr }

// Reloader serializes snapshot replacements. Readers are never blocked
// (they use the Holder atomically); only concurrent reloads are serialized.
type Reloader struct {
	holder *Holder
	build  BuildFunc
}

// NewReloader wires a Holder with the snapshot builder.
func NewReloader(h *Holder, build BuildFunc) *Reloader {
	return &Reloader{holder: h, build: build}
}

// Reload builds a new snapshot from cfg and swaps it in atomically.
// On any failure the previous snapshot remains fully available and the
// returned error is a *ReloadError. After a successful swap, plugins owned by
// the previous snapshot are closed in reverse order.
func (r *Reloader) Reload(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return &ReloadError{
			Kind:    ReloadBuildFailed,
			Reason:  "cannot reload from nil configuration",
			OldKept: true,
		}
	}

	// Build the new world while the old one keeps serving.
	next, err := r.build(ctx, cfg)
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

	// Single atomic publication point.
	prev := r.holder.Swap(next)

	// Tear down the displaced generation after publication.
	if prev != nil {
		if cerr := ClosePlugins(prev.Plugins); cerr != nil {
			// The new snapshot is already serving; report the cleanup trouble
			// as a distinguishable build-independent error wrapper.
			return &ReloadError{
				Kind:    ReloadFailureUnknown,
				Reason:  "new snapshot is active, but closing the previous generation reported errors",
				WrapErr: cerr,
				OldKept: false,
			}
		}
	}
	return nil
}

// ClosePlugins closes plugins in reverse slice order and aggregates all
// failures into one error instead of stopping at the first.
func ClosePlugins(plugins []plugin.Plugin) error {
	failures := make(map[string]error)
	names := make([]string, 0)
	for i := len(plugins) - 1; i >= 0; i-- {
		p := plugins[i]
		if p == nil {
			continue
		}
		names = append(names, p.Name())
		if err := p.Close(); err != nil {
			failures[p.Name()] = err
		}
	}
	if len(failures) == 0 {
		return nil
	}
	sorted := make([]string, 0, len(failures))
	for n := range failures {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	parts := make([]string, 0, len(sorted))
	for _, n := range sorted {
		parts = append(parts, fmt.Sprintf("%q: %v", n, failures[n]))
	}
	return fmt.Errorf("close failed for plugin(s) %s (close order: %s)",
		strings.Join(parts, "; "), strings.Join(names, ", "))
}
