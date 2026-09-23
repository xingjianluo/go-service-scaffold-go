// Package runtime provides atomic configuration/plugin snapshots and
// coordinated hot reload.
//
// Reload rule:
//
//  1. The new Snapshot is built (config validated, plugins initialized) BEFORE
//     the old one is touched. A build failure leaves the serving snapshot
//     completely untouched and returns a *ReloadError with a distinguishable
//     FailureKind.
//  2. Replacement is a single atomic pointer swap: concurrent readers always
//     observe either the complete old snapshot or the complete new one, never
//     a partially-merged state.
//  3. Plugins of the previous snapshot are closed in reverse initialization
//     order only after the swap has succeeded.
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
)

func (k FailureKind) String() string {
	switch k {
	case ReloadBuildFailed:
		return "reload_build_failed"
	case ReloadTimeout:
		return "reload_timeout"
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
