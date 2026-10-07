// Package app wires configuration, plugins and runtime reload together into
// a consistent boot / serve / reload / shutdown flow.
//
// Lifecycle guarantees:
//
//   - Boot performs layered config load (rejecting invalid config before any
//     plugin starts), builds the first generation and publishes it atomically.
//   - A unit of work that needs consistent config+plugins MUST use
//     Acquire/Release: the pinned *runtime.Lease keeps that whole generation
//     (and its plugins) open until Release, even across reloads. Snapshot() is
//     retained as a lock-free best-effort read for code that does not call
//     plugins beyond the instant of the read.
//   - Reload re-reads every config layer; on failure the served generation is
//     untouched and the error wraps a *config.LoadError or *runtime.ReloadError.
//     When the retained-generation capacity is full it fails fast with
//     ReloadDrainFull instead of closing any in-use generation.
//   - Retired generations are reclaimed only after every lease is released.
//   - Shutdown atomically removes the served generation and closes generations
//     that are not leased; leased ones close on their last Release. It is safe
//     to call more than once.
//   - Boot/Reload/Shutdown are mutually serialized; pinning through Acquire is
//     lock-free on the hot path and never observes a half-built state.
//   - While a reload is still PREPARING (a slow build that has not published),
//     RuntimeStatus returns immediately with Reloading=true and
//     ReloadingSince, Acquire keeps serving the old generation without
//     blocking, and in-flight leases run to completion; only after the single
//     atomic publication do new Acquires see the new generation. A failed or
//     timed-out prepare, or a Shutdown landing mid-build, always converges back
//     to a consistent state with the old generation intact.
package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/runtime"
)

// BuildSnapshot constructs the initial snapshot from loaded configuration.
type BuildSnapshot func(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error)

// Options tunes generation retention for an App.
type Options struct {
	// MaxRetainedGenerations bounds how many retired, still-in-use generations
	// may accumulate before further reloads are refused. Zero selects the
	// runtime default (2).
	MaxRetainedGenerations int
	// DrainTimeout bounds how long a retired generation waits for outstanding
	// leases before it is reported "stuck" in status (it is never forcibly
	// closed). Zero selects the runtime default.
	DrainTimeout time.Duration
	// ShutdownWait bounds how long Shutdown waits for leased generations to
	// drain before returning (live leases are never closed underneath their
	// holders). Zero selects the runtime default.
	ShutdownWait time.Duration
}

// App is the reusable service scaffold facade.
type App struct {
	loader *config.Loader
	mgr    *runtime.Manager

	mu     sync.Mutex
	booted bool
}

// New creates an App over the given loader and snapshot builder with default
// generation-retention options.
func New(loader *config.Loader, build BuildSnapshot) *App {
	return NewWithOptions(loader, build, Options{})
}

// NewWithOptions creates an App with explicit retention/timing options.
func NewWithOptions(loader *config.Loader, build BuildSnapshot, opts Options) *App {
	mgr := runtime.NewManager(
		func(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error) {
			return build(ctx, cfg)
		},
		runtime.Options{
			MaxRetainedGenerations: opts.MaxRetainedGenerations,
			DrainTimeout:           opts.DrainTimeout,
			ReapTimeout:            opts.ShutdownWait,
		},
	)
	return &App{loader: loader, mgr: mgr}
}

// Boot loads configuration, builds the first generation and serves it.
// Boot fails (without starting anything visible) when configuration is
// rejected or the initial build fails.
func (a *App) Boot(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.booted {
		return errors.New("app already booted")
	}
	cfg, err := a.loader.Load()
	if err != nil {
		return fmt.Errorf("boot aborted during configuration load: %w", err)
	}
	if err := a.mgr.Reload(ctx, cfg); err != nil {
		return fmt.Errorf("boot aborted while building initial snapshot: %w", err)
	}
	a.booted = true
	return nil
}

// Reload re-reads configuration and atomically publishes a new generation.
// Invalid configuration, a failed build, or a full retention set leaves the
// running state intact; use errors.As with *config.LoadError or
// *runtime.ReloadError (Kind ReloadBuildFailed / ReloadTimeout /
// ReloadDrainFull) to distinguish.
func (a *App) Reload(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.booted {
		return errors.New("app is not booted")
	}
	cfg, err := a.loader.Load()
	if err != nil {
		// Old state stays in service; nothing was swapped.
		return fmt.Errorf("reload aborted during configuration load (old state kept): %w", err)
	}
	if err := a.mgr.Reload(ctx, cfg); err != nil {
		return err
	}
	return nil
}

// Acquire pins the currently served generation for one unit of work. It
// returns nil before Boot or after Shutdown. The caller MUST call Release on
// the returned lease once the work is done (defer is typical); until then the
// pinned config and all its plugins are guaranteed to remain open and
// consistent even if reloads happen in between.
func (a *App) Acquire() *runtime.Lease {
	return a.mgr.Acquire()
}

// Snapshot returns the currently served snapshot, or nil before Boot. It is a
// lock-free best-effort read: prefer Acquire/Release for any work that calls
// into the snapshot's plugins, since an unpinned generation can be retired
// and closed by a concurrent reload.
func (a *App) Snapshot() *runtime.Snapshot {
	return a.mgr.CurrentSnapshot()
}

// RuntimeStatus exposes generation bookkeeping: active version, in-progress
// reload flag, retained (draining/stuck) generations and reclaimed history
// with terminal outcomes.
func (a *App) RuntimeStatus() runtime.ManagerStatus {
	return a.mgr.Status()
}

// Shutdown atomically removes the served generation and closes every
// generation not currently leased; leased generations close on their last
// Release. It waits up to the configured ShutdownWait for in-flight work to
// finish and is safe to call repeatedly.
func (a *App) Shutdown() error {
	return a.ShutdownContext(context.Background())
}

// ShutdownContext is Shutdown with an explicit deadline/cancellation for the
// drain wait.
func (a *App) ShutdownContext(ctx context.Context) error {
	a.mu.Lock()
	if !a.booted {
		a.mu.Unlock()
		return nil
	}
	a.booted = false
	a.mu.Unlock()
	if err := a.mgr.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown completed with plugin close errors: %w", err)
	}
	return nil
}
