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

func cfgWithPort(port int) *config.Config {
	schema := config.Schema{
		{Key: "service.name", Type: config.String, Required: true},
		{Key: "server.port", Type: config.Int, Required: true},
	}
	content := fmt.Sprintf(`{"service.name":"svc","server.port":%d}`, port)
	dir := tTempDir(content)
	cfg, err := config.NewLoader(schema, dir, "APP").Load()
	if err != nil {
		panic(err)
	}
	return cfg
}

// recPlugin records init/close calls and can be forced to fail.
type recPlugin struct {
	name      string
	closeHook func()
	mu        sync.Mutex
	inits     int
	closes    int
	closed    bool
	closeErr  error
}

func (p *recPlugin) Name() string   { return p.name }
func (p *recPlugin) Deps() []string { return nil }
func (p *recPlugin) Init(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inits++
	return nil
}
func (p *recPlugin) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closes++
	p.closed = true
	if p.closeHook != nil {
		p.closeHook()
	}
	return p.closeErr
}
func (p *recPlugin) state() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inits, p.closes
}

var _ plugin.Plugin = (*recPlugin)(nil)

func TestHolderNilAndSwap(t *testing.T) {
	h := NewHolder(nil)
	if h.Current() != nil {
		t.Fatal("fresh holder should serve nil")
	}
	s1 := &Snapshot{Config: cfgWithPort(1)}
	prev := h.Swap(s1)
	if prev != nil {
		t.Fatal("first swap should return nil previous")
	}
	if h.Current() != s1 {
		t.Fatal("holder should serve s1")
	}
	s2 := &Snapshot{Config: cfgWithPort(2)}
	if h.Swap(s2) != s1 {
		t.Fatal("second swap should return s1")
	}
	if h.Current() != s2 {
		t.Fatal("holder should serve s2")
	}
	log.Printf("INPUT swap s1->s2\nJUDGEMENT holder serves port=%d",
		h.Current().Config.GetInt("server.port"))
}

// TestConcurrentReadersConsistency runs many readers while snapshots swap.
// Each reader must see exactly one complete generation, never a torn state:
// port value must match the plugin set of that same build.
func TestConcurrentReadersConsistency(t *testing.T) {
	s1 := &Snapshot{
		Config:  cfgWithPort(100),
		Plugins: []plugin.Plugin{&recPlugin{name: "g1"}},
	}
	h := NewHolder(s1)
	var stop atomic.Bool
	var wg sync.WaitGroup

	// Readers: every observed (port, plugin-generation) pair must be coherent.
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for !stop.Load() {
				snap := h.Current()
				port := snap.Config.GetInt("server.port")
				var gen string
				if p, ok := snap.Plugin("g1"); ok {
					gen = p.Name()
				} else if p, ok := snap.Plugin("g2"); ok {
					gen = p.Name()
				}
				// Coherence rule: port 100 must pair with g1, port 200 with g2.
				if (port == 100 && gen != "g1") || (port == 200 && gen != "g2") {
					t.Errorf("reader %d saw torn state port=%d plugin=%s", id, port, gen)
					return
				}
			}
		}(i)
	}

	// Swapper alternates generations. Old plugins are intentionally not closed
	// here (Reloader owns that policy in other tests).
	s2 := &Snapshot{
		Config:  cfgWithPort(200),
		Plugins: []plugin.Plugin{&recPlugin{name: "g2"}},
	}
	for i := 0; i < 200; i++ {
		h.Swap(s2)
		h.Swap(s1)
	}
	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	log.Printf("INPUT 200 swaps g1<->g2 with 16 concurrent readers\nJUDGEMENT no torn state observed")
}

// TestReloadFailureKeepsOld verifies build failure never touches the serving state.
func TestReloadFailureKeepsOld(t *testing.T) {
	oldP := &recPlugin{name: "old"}
	if err := oldP.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := &Snapshot{Config: cfgWithPort(100), Plugins: []plugin.Plugin{oldP}}
	h := NewHolder(old)

	buildErr := errors.New("cannot bind new generation")
	rl := NewReloader(h, func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		log.Printf("INPUT build called with port=%d; returning failure", cfg.GetInt("server.port"))
		return nil, buildErr
	})

	err := rl.Reload(context.Background(), cfgWithPort(200))
	var re *ReloadError
	if !errors.As(err, &re) {
		t.Fatalf("expected *ReloadError, got %T: %v", err, err)
	}
	log.Printf("JUDGEMENT %v", err)
	if re.Kind != ReloadBuildFailed || !re.OldKept {
		t.Fatalf("expected build-failed with old kept, got kind=%s oldKept=%t", re.Kind, re.OldKept)
	}
	if h.Current() != old {
		t.Fatal("old snapshot must remain the served one after failed reload")
	}
	i, c := oldP.state()
	if i != 1 || c != 0 {
		t.Fatalf("old plugin must stay untouched: inits=%d closes=%d", i, c)
	}
	if h.Current().Config.GetInt("server.port") != 100 {
		t.Fatal("served config must stay on old generation")
	}
}

// TestReloadSuccessSwapsAndClosesOld verifies atomic swap plus old-gen teardown.
func TestReloadSuccessSwapsAndClosesOld(t *testing.T) {
	oldP := &recPlugin{name: "old"}
	if err := oldP.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := &Snapshot{Config: cfgWithPort(100), Plugins: []plugin.Plugin{oldP}}
	h := NewHolder(old)

	rl := NewReloader(h, func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		p := &recPlugin{name: "new"}
		if err := p.Init(ctx); err != nil {
			return nil, err
		}
		return &Snapshot{Config: cfg, Plugins: []plugin.Plugin{p}}, nil
	})

	if err := rl.Reload(context.Background(), cfgWithPort(200)); err != nil {
		t.Fatalf("reload: %v", err)
	}
	served := h.Current()
	log.Printf("INPUT successful reload port 100->200\nJUDGEMENT served port=%d",
		served.Config.GetInt("server.port"))
	if served.Config.GetInt("server.port") != 200 {
		t.Fatal("new generation must be served")
	}
	if _, ok := served.Plugin("new"); !ok {
		t.Fatal("new plugin must be in served snapshot")
	}
	if _, ok := served.Plugin("old"); ok {
		t.Fatal("old plugin must not be in served snapshot")
	}
	if _, c := oldP.state(); c != 1 {
		t.Fatalf("old plugin must be closed exactly once, closes=%d", c)
	}
	if p, ok := served.Plugin("new"); !ok {
		t.Fatal("new plugin missing")
	} else {
		if i, _ := p.(*recPlugin).state(); i != 1 {
			t.Fatalf("built new plugin must be initialized once, inits=%d", i)
		}
	}
}

// TestReloadCanceled verifies context cancellation is classified as timeout.
func TestReloadCanceled(t *testing.T) {
	h := NewHolder(&Snapshot{Config: cfgWithPort(100)})
	rl := NewReloader(h, func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		return nil, errors.New("build aborted")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rl.Reload(ctx, cfgWithPort(200))
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadTimeout {
		t.Fatalf("expected ReloadTimeout, got %v", err)
	}
	log.Printf("INPUT canceled context during build\nJUDGEMENT %v", err)
	if h.Current().Config.GetInt("server.port") != 100 {
		t.Fatal("old state must survive canceled reload")
	}
}

// TestReloadNilConfig verifies nil config is a distinguishable, safe failure.
func TestReloadNilConfig(t *testing.T) {
	h := NewHolder(&Snapshot{Config: cfgWithPort(100)})
	rl := NewReloader(h, func(context.Context, *config.Config) (*Snapshot, error) {
		t.Fatal("build must not be called for nil config")
		return nil, nil
	})
	err := rl.Reload(context.Background(), nil)
	var re *ReloadError
	if !errors.As(err, &re) || re.Kind != ReloadBuildFailed || !re.OldKept {
		t.Fatalf("expected safe build failure, got %v", err)
	}
	log.Printf("INPUT nil config\nJUDGEMENT %v", err)
}

// TestConcurrentReloads verifies serialized reloads all converge and that
// readers never see nil during the sequence.
func TestConcurrentReloads(t *testing.T) {
	first := &recPlugin{name: "p-0"}
	h := NewHolder(&Snapshot{Config: cfgWithPort(0), Plugins: []plugin.Plugin{first}})
	var counter atomic.Int64
	rl := NewReloader(h, func(ctx context.Context, cfg *config.Config) (*Snapshot, error) {
		n := counter.Add(1)
		p := &recPlugin{name: fmt.Sprintf("p-%d", n)}
		if err := p.Init(ctx); err != nil {
			return nil, err
		}
		return &Snapshot{
			Config:  cfg,
			Plugins: []plugin.Plugin{p},
		}, nil
	})

	var wg sync.WaitGroup
	var stopRead atomic.Bool
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopRead.Load() {
				if h.Current() == nil {
					t.Error("reader observed nil during reloads")
					return
				}
			}
		}()
	}
	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			if err := rl.Reload(context.Background(), cfgWithPort(port)); err != nil {
				t.Errorf("reload port=%d: %v", port, err)
			}
		}(i)
	}
	// Readers need to stop before waiting the reader goroutines.
	time.Sleep(30 * time.Millisecond)
	stopRead.Store(true)
	wg.Wait()

	served := h.Current()
	log.Printf("INPUT 50 concurrent reloads + 8 readers\nJUDGEMENT final served port=%d plugin generation=%s",
		served.Config.GetInt("server.port"), served.Plugins[0].Name())
	if p := served.Config.GetInt("server.port"); p < 1 || p > 50 {
		t.Fatalf("served port out of final range: %d", p)
	}
}
