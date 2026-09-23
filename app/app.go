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

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/runtime"
)

// BuildSnapshot constructs the initial snapshot from loaded configuration.
type BuildSnapshot func(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error)

// App is the reusable service scaffold facade.
type App struct {
	loader   *config.Loader
	holder   *runtime.Holder
	reloader *runtime.Reloader
	build    BuildSnapshot

	mu     sync.Mutex
	booted bool
}

// New creates an App over the given loader and snapshot builder.
func New(loader *config.Loader, build BuildSnapshot) *App {
	h := runtime.NewHolder(nil)
	return &App{
		loader: loader,
		holder: h,
		reloader: runtime.NewReloader(h, func(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error) {
			return build(ctx, cfg)
		}),
		build: build,
	}
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
	if err := a.reloader.Reload(ctx, cfg); err != nil {
		return fmt.Errorf("boot aborted while building initial snapshot: %w", err)
	}
	a.booted = true
	return nil
}

// Reload re-reads configuration and atomically swaps in a new snapshot.
// Invalid configuration or a failed build leaves the running state intact;
// use errors.As with *config.LoadError or *runtime.ReloadError to distinguish.
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
	if err := a.reloader.Reload(ctx, cfg); err != nil {
		return err
	}
	return nil
}

// Snapshot returns the currently served snapshot, or nil before Boot.
// The returned snapshot is immutable from the reader's perspective.
func (a *App) Snapshot() *runtime.Snapshot {
	return a.holder.Current()
}

// Shutdown atomically removes the served snapshot and closes its plugins in
// reverse initialization order. It is safe to call repeatedly.
func (a *App) Shutdown() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.booted {
		return nil
	}
	prev := a.holder.Swap(nil)
	a.booted = false
	if prev == nil {
		return nil
	}
	if err := runtime.ClosePlugins(prev.Plugins); err != nil {
		return fmt.Errorf("shutdown completed with plugin close errors: %w", err)
	}
	return nil
}
