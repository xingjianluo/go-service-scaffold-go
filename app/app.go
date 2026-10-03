// Package app wires configuration, plugins and runtime reload together into
// a consistent boot / serve / reload / shutdown flow.
//
// Lifecycle guarantees:
//
//   - Boot performs layered config load (rejecting invalid config before any
//     plugin starts), builds the first snapshot and publishes it atomically.
//   - Reload re-reads every config layer; on failure the served snapshot is
//     untouched and the error wraps a *config.LoadError or *runtime.ReloadError.
//   - Shutdown atomically removes the served snapshot and closes its plugins
//     in reverse order; it is safe to call more than once.
//   - Boot/Reload/Shutdown are mutually serialized; reads through Snapshot()
//     are lock-free and never observe a half-built state.
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

// Option configures an App at construction time.
type Option func(*App)

// WithGenerationRetirement enables lease-based in-flight protection and
// bounded retirement:
//
//   - app.Acquire()/Lease.Release() pin one consistent generation per unit of
//     work; displaced generations keep their plugins alive until their last
//     lease is released.
//   - maxRetired bounds how many displaced-but-in-use generations may coexist;
//     reloads beyond the bound fail with runtime.ReloadCapacityExceeded.
//   - shutdownDrain bounds how long Shutdown waits for in-flight leases
//     before force-closing (0 = do not wait, legacy behavior).
//
// Without this option the App behaves exactly as before
// (immediate old-generation teardown after swap).
func WithGenerationRetirement(maxRetired int, shutdownDrain time.Duration) Option {
	return func(a *App) {
		a.guardCfg = &runtime.GuardConfig{
			MaxRetiredGenerations: maxRetired,
			ShutdownDrainTimeout:  shutdownDrain,
		}
	}
}

// App is the reusable service scaffold facade.
type App struct {
	loader   *config.Loader
	holder   *runtime.Holder
	reloader *runtime.Reloader
	guard    *runtime.Guard
	guardCfg *runtime.GuardConfig
	build    BuildSnapshot

	mu     sync.Mutex
	booted bool
}

// New creates an App over the given loader and snapshot builder.
// Options (e.g. WithGenerationRetirement) add capabilities without changing
// the default legacy behavior.
func New(loader *config.Loader, build BuildSnapshot, opts ...Option) *App {
	h := runtime.NewHolder(nil)
	a := &App{
		loader: loader,
		holder: h,
		reloader: runtime.NewReloader(h, func(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error) {
			return build(ctx, cfg)
		}),
		build: build,
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.guardCfg != nil {
		a.guard = runtime.NewGuard(func(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error) {
			return build(ctx, cfg)
		}, *a.guardCfg)
	}
	return a
}

// Acquire pins the currently active generation and must be paired with
// Lease.Release when the unit of work finishes. It is available only when the
// App was created with WithGenerationRetirement; without that option it
// returns a non-nil error. The pinned snapshot keeps serving its own, fully
// consistent config and plugins across any number of concurrent reloads.
func (a *App) Acquire() (*runtime.Lease, error) {
	if a.guard == nil {
		return nil, errors.New("app: generation retirement is not enabled; use WithGenerationRetirement")
	}
	return a.guard.Acquire()
}

// RuntimeStatus reports observable generation/reload state. It returns a
// zero-value status (ActiveID == -1, Capacity == 0) when
// WithGenerationRetirement was not enabled.
func (a *App) RuntimeStatus() runtime.GuardStatus {
	if a.guard == nil {
		return runtime.GuardStatus{ActiveID: -1}
	}
	return a.guard.Status()
}

// Boot loads configuration, builds the first snapshot and serves it.
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
	if a.guard != nil {
		if err := a.guard.Reload(ctx, cfg); err != nil {
			return fmt.Errorf("boot aborted while building initial snapshot: %w", err)
		}
		a.booted = true
		return nil
	}
	if err := a.reloader.Reload(ctx, cfg); err != nil {
		return fmt.Errorf("boot aborted while building initial snapshot: %w", err)
	}
	a.booted = true
	return nil
}

// Reload re-reads configuration and atomically swaps in a new snapshot.
// Invalid configuration or a failed build leaves the running state intact;
// use errors.As with *config.LoadError or *runtime.ReloadError to distinguish.
// When WithGenerationRetirement is enabled, *runtime.ReloadError with
// Kind=ReloadCapacityExceeded additionally means the reload was refused
// because too many old generations are still in use (the active state is
// still fully available in that case).
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
	if a.guard != nil {
		return a.guard.Reload(ctx, cfg)
	}
	return a.reloader.Reload(ctx, cfg)
}

// Snapshot returns the currently served snapshot, or nil before Boot.
// The returned snapshot is immutable from the reader's perspective.
//
// Compatibility note: a snapshot obtained this way is the lock-free
// "observe now" view. For a guarantee that the same config AND plugins remain
// usable for the ENTIRE unit of work (even across a concurrent reload),
// enable WithGenerationRetirement and use Acquire()/Lease.Release() instead.
func (a *App) Snapshot() *runtime.Snapshot {
	if a.guard != nil {
		lease, err := a.guard.Acquire()
		if err != nil {
			return nil
		}
		snap := lease.Snapshot()
		// "Observe now" semantics: do not pin — release immediately. Readers
		// that need pinning must use Acquire explicitly.
		lease.Release()
		return snap
	}
	return a.holder.Current()
}

// Shutdown atomically removes the served snapshot and closes its plugins in
// reverse initialization order. When generation retirement is enabled it
// additionally waits up to the configured drain timeout for in-flight leases
// and then reclaims every generation (forcibly past the deadline).
// It is safe to call repeatedly.
func (a *App) Shutdown() error {
	return a.ShutdownCtx(context.Background())
}

// ShutdownCtx is Shutdown with an explicit context bounding the drain wait.
func (a *App) ShutdownCtx(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.booted {
		return nil
	}
	a.booted = false
	if a.guard != nil {
		if err := a.guard.Shutdown(ctx); err != nil {
			return err
		}
		return nil
	}
	prev := a.holder.Swap(nil)
	if prev == nil {
		return nil
	}
	if err := runtime.ClosePlugins(prev.Plugins); err != nil {
		return fmt.Errorf("shutdown completed with plugin close errors: %w", err)
	}
	return nil
}
