package runtime

// This file covers the slow-reload window: the time when a reload is still
// PREPARING (building) and has not published yet. During that window Status
// must immediately report Reloading=true, business work via Acquire must keep
// being served from the old generation without blocking, and every outcome
// (success / build failure / timeout / shutdown race) must converge back to a
// consistent state.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/plugin"
)

// gateBuild is a controllable BuildFunc: every build blocks in the prepare
// phase until release() is called or ctx is canceled. Tests can therefore hold
// a reload deterministically inside its slow window and inspect/serve during
// it, then choose success, failure, or timeout as the outcome.
type gateBuild struct {
	mu       sync.Mutex
	started  chan struct{} // closed when the current build enters prepare
	release  chan struct{} // closed to let the current build finish successfully
	failWith error         // when set, the build returns this error once released/canceled
	builds   int
	lastPort int
}

func newGateBuild() *gateBuild {
	g := &gateBuild{}
	g.reset()
	return g
}

// reset prepares fresh signal channels for the next build.
func (g *gateBuild) reset() {
	g.started = make(chan struct{})
	g.release = make(chan struct{})
}

// allowFinish sets the build up to fail once it leaves the prepare phase.
func (g *gateBuild) setFail(err error) {
	g.mu.Lock()
	g.failWith = err
	g.mu.Unlock()
}

// finish releases the prepare gate (success or, with setFail, failure).
func (g *gateBuild) finish() { close(g.release) }

func (g *gateBuild) build(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
	g.mu.Lock()
	g.builds++
	g.lastPort = cfg.GetInt("server.port")
	fail := g.failWith
	close(g.started)
	g.mu.Unlock()

	// Slow prepare phase: block until the test releases us or ctx ends.
	select {
	case <-g.release:
	case <-ctx.Done():
		// Cooperative cancellation/timeout: return promptly. If a failure was
		// configured the failure kind classification follows ctx.Err().
		if fail != nil {
			return nil, fail
		}
		return nil, ctx.Err()
	}
	if fail != nil {
		return nil, fail
	}
	p := &recPlugin{name: fmt.Sprintf("p-%d", cfg.GetInt("server.port"))}
	if err := p.Init(ctx); err != nil {
		return nil, err
	}
	return &Snapshot{Config: cfg, Plugins: []plugin.Plugin{p}}, nil
}

// newGateManager wires a Manager (default options) around a gateBuild.
func newGateManager(t *testing.T, g *gateBuild, opts Options) *Manager {
	t.Helper()
	return NewManager(g.build, opts)
}

// startSlowReload runs Reload in a goroutine and returns its result channel.
func startSlowReload(m *Manager, cfg *config.Config) <-chan error {
	done := make(chan error, 1)
	go func() { done <- m.Reload(context.Background(), cfg) }()
	return done
}

// startSlowReloadCtx runs Reload with an explicit context (timeout/cancel
// tests) in a goroutine and returns its result channel.
func startSlowReloadCtx(ctx context.Context, m *Manager, cfg *config.Config) <-chan error {
	done := make(chan error, 1)
	go func() { done <- m.Reload(ctx, cfg) }()
	return done
}

// waitBuildStarted blocks until the gate build enters the prepare phase.
func waitBuildStarted(t *testing.T, g *gateBuild) {
	t.Helper()
	select {
	case <-g.started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow build never entered the prepare phase")
	}
}

// waitReloadResult waits for the background reload to settle.
func waitReloadResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("reload did not settle in time")
		return nil
	}
}
