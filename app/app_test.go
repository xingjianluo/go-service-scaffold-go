package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/plugin"
	"github.com/xingjianluo/go-service-scaffold-go/runtime"
)

func appSchema() config.Schema {
	return config.Schema{
		{Key: "service.name", Type: config.String, Required: true},
		{Key: "db.password", Type: config.String, Required: true, Secret: true},
		{Key: "server.port", Type: config.Int, Required: true, Validator: func(v interface{}) error {
			n := v.(int)
			if n < 1 || n > 65535 {
				return errors.New("port out of range")
			}
			return nil
		}},
	}
}

func writeAppConfig(t *testing.T, name string, port int, password string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	content := fmt.Sprintf(`{"service.name":%q,"db.password":%q,"server.port":%d}`,
		name, password, port)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// lifecyclePlugin is a Plugin whose Init can fail when the port hits a
// configured bad value, letting tests exercise snapshot-build rollback paths.
type lifecyclePlugin struct {
	name     string
	failPort int
	mu       sync.Mutex
	initd    bool
	closed   bool
}

func (p *lifecyclePlugin) Name() string   { return p.name }
func (p *lifecyclePlugin) Deps() []string { return nil }
func (p *lifecyclePlugin) Init(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initd = true
	return nil
}
func (p *lifecyclePlugin) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}
func (p *lifecyclePlugin) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

var _ plugin.Plugin = (*lifecyclePlugin)(nil)

// newBuild returns a BuildSnapshot that registers two plugins in dependency
// order through plugin.Manager and fails for a specific "poison" port.
func newBuild(t *testing.T, poisonPort int, tracker *orderTracker) BuildSnapshot {
	return func(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error) {
		port := cfg.GetInt("server.port")
		t.Logf("BUILD input: service=%q port=%d secret=***",
			cfg.GetString("service.name"), port)
		if port == poisonPort {
			return nil, fmt.Errorf("simulated build failure for poison port %d", port)
		}
		reg := plugin.NewRegistry()
		pa := &lifecyclePlugin{name: "a"}
		pb := &lifecyclePlugin{name: "b", failPort: poisonPort}
		if err := reg.Register(pa); err != nil {
			return nil, err
		}
		if err := reg.Register(&depsPlugin{Plugin: pb, deps: []string{"a"}}); err != nil {
			return nil, err
		}
		mgr := plugin.NewManager(reg)
		if err := mgr.Start(ctx); err != nil {
			return nil, err
		}
		tracker.record(port, pa, pb)
		t.Logf("BUILD judgement: plugins active in order %v", mgr.Active())
		return &runtime.Snapshot{Config: cfg, Plugins: []plugin.Plugin{pa, pb}}, nil
	}
}

type depsPlugin struct {
	plugin.Plugin
	deps []string
}

func (d *depsPlugin) Deps() []string { return d.deps }

type orderTracker struct {
	mu    sync.Mutex
	ports []int
}

func (o *orderTracker) record(port int, ps ...*lifecyclePlugin) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ports = append(o.ports, port)
}

func TestBootAndShutdown(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "secret-xyz")
	a := New(config.NewLoader(appSchema(), path, "APP"), newBuild(t, -1, &orderTracker{}))
	if err := a.Boot(context.Background()); err != nil {
		t.Fatalf("Boot: %v", err)
	}
	snap := a.Snapshot()
	if snap == nil || snap.Config.GetInt("server.port") != 8080 {
		t.Fatalf("snapshot wrong after boot: %+v", snap)
	}
	log.Printf("INPUT boot with port=8080 secret=%q\nJUDGEMENT Describe=%s",
		"secret-xyz", snap.Config.Describe(appSchema()))
	if strings.Contains(snap.Config.Describe(appSchema()), "secret-xyz") {
		t.Fatal("secret must not appear in snapshot description")
	}
	if err := a.Boot(context.Background()); err == nil {
		t.Fatal("double Boot must be rejected")
	}
	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if a.Snapshot() != nil {
		t.Fatal("snapshot must be nil after shutdown")
	}
	// Repeated shutdown must be safe.
	if err := a.Shutdown(); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestBootRejectsInvalidConfig(t *testing.T) {
	path := writeAppConfig(t, "svc", 70000, "secret-xyz") // invalid port
	a := New(config.NewLoader(appSchema(), path, "APP"),
		func(context.Context, *config.Config) (*runtime.Snapshot, error) {
			t.Fatal("build must not run when config is invalid")
			return nil, nil
		})
	err := a.Boot(context.Background())
	log.Printf("INPUT invalid port 70000 at boot\nJUDGEMENT %v", err)
	var le *config.LoadError
	if !errors.As(err, &le) || !le.HasKind(config.FailureInvalidValue) {
		t.Fatalf("expected invalid-value LoadError, got %v", err)
	}
	if a.Snapshot() != nil {
		t.Fatal("no snapshot may be published on invalid config")
	}
}

func TestBootRejectsMissingRequired(t *testing.T) {
	// File with missing db.password and no env fallback.
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"service.name":"svc","server.port":8080}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a := New(config.NewLoader(appSchema(), path, "APP"),
		func(context.Context, *config.Config) (*runtime.Snapshot, error) {
			t.Fatal("build must not run")
			return nil, nil
		})
	err := a.Boot(context.Background())
	var le *config.LoadError
	if !errors.As(err, &le) || !le.HasKind(config.FailureMissingRequired) {
		t.Fatalf("expected missing-required, got %v", err)
	}
	log.Printf("INPUT missing db.password at boot\nJUDGEMENT rejected: %v", err)
}

func TestReloadOldStateKeptOnFailure(t *testing.T) {
	tracker := &orderTracker{}
	path := writeAppConfig(t, "svc", 8080, "pw1")
	a := New(config.NewLoader(appSchema(), path, "APP"), newBuild(t, 9999, tracker))
	if err := a.Boot(context.Background()); err != nil {
		t.Fatalf("Boot: %v", err)
	}
	old := a.Snapshot()
	oldB := old.Plugins[1].(*lifecyclePlugin)

	// Poison port: build fails, old snapshot must stay.
	poisonPath := writeAppConfig(t, "svc", 9999, "pw2")
	a2 := a // reload uses the same loader; rewrite path via a new loader is not
	_ = poisonPath
	// Instead, swap the file content at the original path is not possible
	// across temp dirs; emulate by rewriting the booted file directly.
	if err := os.WriteFile(path, []byte(
		`{"service.name":"svc","db.password":"pw2","server.port":9999}`), 0o600); err != nil {
		t.Fatal(err)
	}
	err := a.Reload(context.Background())
	log.Printf("INPUT reload with poison port 9999\nJUDGEMENT %v", err)
	var re *runtime.ReloadError
	if !errors.As(err, &re) || !re.OldKept {
		t.Fatalf("expected ReloadError with old state kept, got %v", err)
	}
	if a.Snapshot() != old {
		t.Fatal("old snapshot must remain served after failed reload")
	}
	if a.Snapshot().Config.GetInt("server.port") != 8080 {
		t.Fatal("old config (port 8080) must remain available")
	}
	if oldB.isClosed() {
		t.Fatal("old plugins must not be closed when reload build fails")
	}
	_ = a2
	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !oldB.isClosed() {
		t.Fatal("old plugin b must be closed by shutdown")
	}
}

func TestReloadSuccessReplacesGeneration(t *testing.T) {
	tracker := &orderTracker{}
	path := writeAppConfig(t, "svc", 8080, "pw1")
	build := newBuild(t, -1, tracker)
	a := New(config.NewLoader(appSchema(), path, "APP"), build)
	if err := a.Boot(context.Background()); err != nil {
		t.Fatalf("Boot: %v", err)
	}
	old := a.Snapshot()
	oldB := old.Plugins[1].(*lifecyclePlugin)

	// Rewrite the same file to a valid new generation and reload.
	if err := os.WriteFile(path, []byte(
		`{"service.name":"svc","db.password":"pw2","server.port":9090}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.Reload(context.Background()); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	cur := a.Snapshot()
	log.Printf("INPUT reload 8080->9090\nJUDGEMENT served port=%d", cur.Config.GetInt("server.port"))
	if cur == old {
		t.Fatal("new snapshot object must be served")
	}
	if cur.Config.GetInt("server.port") != 9090 {
		t.Fatalf("expected new port 9090, got %d", cur.Config.GetInt("server.port"))
	}
	if !oldB.isClosed() {
		t.Fatal("previous generation plugins must be closed after successful swap")
	}
	newB := cur.Plugins[1].(*lifecyclePlugin)
	if newB.isClosed() {
		t.Fatal("new generation plugins must be running")
	}
	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestReloadRejectsInvalidConfigKeepsState(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "pw1")
	a := New(config.NewLoader(appSchema(), path, "APP"), newBuild(t, -1, &orderTracker{}))
	if err := a.Boot(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := a.Snapshot()
	// Invalid type: port as string.
	if err := os.WriteFile(path, []byte(
		`{"service.name":"svc","db.password":"pw2","server.port":"oops"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	err := a.Reload(context.Background())
	var le *config.LoadError
	if !errors.As(err, &le) || !le.HasKind(config.FailureTypeMismatch) {
		t.Fatalf("expected type-mismatch LoadError, got %v", err)
	}
	log.Printf("INPUT invalid-typed reload\nJUDGEMENT %v; served port still=%d",
		err, a.Snapshot().Config.GetInt("server.port"))
	if a.Snapshot() != old {
		t.Fatal("invalid reload must keep serving the old snapshot")
	}
	if err := a.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentReadsDuringReload(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "pw1")
	build := newBuild(t, -1, &orderTracker{})
	a := New(config.NewLoader(appSchema(), path, "APP"), build)
	if err := a.Boot(context.Background()); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				snap := a.Snapshot()
				if snap == nil {
					t.Error("reader saw nil snapshot while app is up")
					return
				}
				port := snap.Config.GetInt("server.port")
				if _, ok := snap.Plugin("b"); !ok {
					t.Error("snapshot missing plugin b: torn read")
					return
				}
				if port != 8080 && port != 9090 {
					t.Errorf("reader saw unexpected port %d", port)
					return
				}
			}
		}()
	}
	for i := 0; i < 30; i++ {
		port := 8080
		if i%2 == 0 {
			port = 9090
		}
		content := fmt.Sprintf(`{"service.name":"svc","db.password":"pw%d","server.port":%d}`, i, port)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := a.Reload(context.Background()); err != nil {
			t.Fatalf("reload %d: %v", i, err)
		}
	}
	stop.Store(true)
	wg.Wait()
	log.Printf("INPUT 30 alternating reloads with 12 readers\nJUDGEMENT final port=%d, no torn reads",
		a.Snapshot().Config.GetInt("server.port"))
	if err := a.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

// TestAppLeaseSurvivesReload is the end-to-end version of the runtime lease
// guarantee: work started on the old generation finishes against the old
// plugins even though reload swapped in a new generation; new work sees the
// new state immediately.
func TestAppLeaseSurvivesReload(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "pw1")
	a := NewWithOptions(config.NewLoader(appSchema(), path, "APP"),
		newBuild(t, -1, &orderTracker{}), Options{
			MaxRetainedGenerations: 2,
			DrainTimeout:           time.Second,
			ShutdownWait:           time.Second,
		})
	if err := a.Boot(context.Background()); err != nil {
		t.Fatalf("Boot: %v", err)
	}

	// Request A starts before reload on v1.
	leaseA := a.Acquire()
	if leaseA == nil {
		t.Fatal("Acquire after boot must succeed")
	}
	oldB := leaseA.Snapshot().Plugins[1].(*lifecyclePlugin)

	if err := os.WriteFile(path, []byte(
		`{"service.name":"svc","db.password":"pw2","server.port":9090}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.Reload(context.Background()); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// New request B immediately observes the new generation.
	leaseB := a.Acquire()
	if leaseB.Snapshot().Config.GetInt("server.port") != 9090 {
		t.Fatalf("new work must see new port, got %d",
			leaseB.Snapshot().Config.GetInt("server.port"))
	}
	// A still owns a complete old generation; its plugin is not closed.
	if oldB.isClosed() {
		t.Fatal("old plugin must remain open for in-flight request A")
	}
	if port := leaseA.Snapshot().Config.GetInt("server.port"); port != 8080 {
		t.Fatalf("request A must keep seeing old port 8080, got %d", port)
	}
	st := a.RuntimeStatus()
	log.Printf("INPUT request A leased v1, reload to v2, request B arrives\n"+
		"JUDGEMENT active=v%d retained=%d A_port=%d B_port=%d oldPluginOpen=%t",
		st.ActiveVersion, len(st.Retained),
		leaseA.Snapshot().Config.GetInt("server.port"),
		leaseB.Snapshot().Config.GetInt("server.port"), !oldB.isClosed())

	leaseB.Release()
	leaseA.Release()
	// After A finishes, the old generation is reclaimed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if oldB.isClosed() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !oldB.isClosed() {
		t.Fatal("old generation must be closed after request A released its lease")
	}
	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestAppReloadRejectedAtCapacity verifies the facade surfaces ReloadDrainFull
// and keeps every in-use generation intact.
func TestAppReloadRejectedAtCapacity(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "pw")
	a := NewWithOptions(config.NewLoader(appSchema(), path, "APP"),
		newBuild(t, -1, &orderTracker{}), Options{
			MaxRetainedGenerations: 1,
			DrainTimeout:           10 * time.Second,
			ShutdownWait:           100 * time.Millisecond,
		})
	if err := a.Boot(context.Background()); err != nil {
		t.Fatal(err)
	}
	l1 := a.Acquire()
	oldB := l1.Snapshot().Plugins[1].(*lifecyclePlugin)
	writePort := func(p int) {
		if err := os.WriteFile(path, []byte(fmt.Sprintf(
			`{"service.name":"svc","db.password":"pw","server.port":%d}`, p)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePort(9090)
	if err := a.Reload(context.Background()); err != nil {
		t.Fatalf("first reload: %v", err)
	}
	writePort(9091)
	err := a.Reload(context.Background())
	var re *runtime.ReloadError
	if !errors.As(err, &re) || re.Kind != runtime.ReloadDrainFull {
		t.Fatalf("expected ReloadDrainFull, got %v", err)
	}
	log.Printf("INPUT app capacity=1, v1 still leased, attempted third generation\n"+
		"JUDGEMENT reload rejected kind=%s; active port=%d; old plugin open=%t",
		re.Kind, a.Snapshot().Config.GetInt("server.port"), !oldB.isClosed())
	if a.Snapshot().Config.GetInt("server.port") != 9090 {
		t.Fatal("active generation must remain v2 (9090)")
	}
	if oldB.isClosed() {
		t.Fatal("leased v1 plugins must not be removed at capacity")
	}
	l1.Release()
	if err := a.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
