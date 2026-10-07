package app

// End-to-end slow-reload window tests through the App facade: while a reload
// is still preparing, RuntimeStatus must return immediately reporting the
// in-progress reload, and business work via Acquire must keep being served
// from the old state without blocking. INPUT/JUDGEMENT logs match the suite.

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/plugin"
	"github.com/xingjianluo/go-service-scaffold-go/runtime"
)

// gateFacadeBuild is a BuildSnapshot that blocks in the prepare phase on every
// build until release is closed or ctx ends, so a test can hold a reload
// deterministically inside its slow window and still drive the normal
// two-plugin (a -> b) build used elsewhere in the package.
type gateFacadeBuild struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
}

func newGateFacadeBuild() *gateFacadeBuild {
	b := &gateFacadeBuild{}
	b.reset()
	return b
}

func (b *gateFacadeBuild) reset() {
	b.started = make(chan struct{})
	b.release = make(chan struct{})
}

func (b *gateFacadeBuild) buildFn(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error) {
	b.mu.Lock()
	close(b.started)
	b.mu.Unlock()

	// Slow prepare phase.
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	reg := plugin.NewRegistry()
	pa := &lifecyclePlugin{name: "a"}
	pb := &lifecyclePlugin{name: "b"}
	if err := reg.Register(pa); err != nil {
		return nil, err
	}
	if err := reg.Register(&depsPlugin{Plugin: pb, deps: []string{"a"}}); err != nil {
		return nil, err
	}
	pm := plugin.NewManager(reg)
	if err := pm.Start(ctx); err != nil {
		return nil, err
	}
	return &runtime.Snapshot{Config: cfg, Plugins: []plugin.Plugin{pa, pb}}, nil
}

// rewritePort rewrites the app config file to serve a new port/password.
func rewritePort(t *testing.T, path string, port int) {
	t.Helper()
	content := fmt.Sprintf(`{"service.name":"svc","db.password":"pw","server.port":%d}`, port)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAppFacadeStatusAndWorkDuringSlowReload is the facade-level proof that the
// runtime fix is actually exposed to callers: while a reload is slow to
// prepare, RuntimeStatus returns immediately with Reloading=true and the old
// active version, and new Acquire calls return immediately on the old state;
// after publish, new Acquires see the new state.
func TestAppFacadeStatusAndWorkDuringSlowReload(t *testing.T) {
	path := writeAppConfig(t, "svc", 8080, "pw")
	gb := newGateFacadeBuild()
	a := NewWithOptions(config.NewLoader(appSchema(), path, "APP"), gb.buildFn, Options{
		MaxRetainedGenerations: 4,
		DrainTimeout:           5 * time.Second,
		ShutdownWait:           time.Second,
	})

	// Boot v1 (port 8080): wait for the first build to enter prepare, then
	// release it. Re-arm the gate only AFTER boot is building is released.
	bootDone := make(chan error, 1)
	go func() { bootDone <- a.Boot(context.Background()) }()
	select {
	case <-gb.started:
	case <-time.After(2 * time.Second):
		t.Fatal("boot build never started")
	}
	close(gb.release)
	if err := <-bootDone; err != nil {
		t.Fatalf("boot: %v", err)
	}
	gb.reset() // arm fresh gates for the reload

	// In-flight request A pinned to v1 before the reload.
	leaseA := a.Acquire()
	if leaseA == nil {
		t.Fatal("Acquire after boot must succeed")
	}
	oldB := leaseA.Snapshot().Plugins[1].(*lifecyclePlugin)

	// Trigger a slow reload (port 9090) and hold it in prepare.
	rewritePort(t, path, 9090)
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- a.Reload(context.Background()) }()
	select {
	case <-gb.started:
	case <-time.After(2 * time.Second):
		t.Fatal("reload build never entered prepare")
	}

	// RuntimeStatus must return immediately and honestly report preparing.
	statusCh := make(chan runtime.ManagerStatus, 1)
	go func() { statusCh <- a.RuntimeStatus() }()
	var st runtime.ManagerStatus
	select {
	case st = <-statusCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("RuntimeStatus blocked during slow prepare")
	}
	if !st.Reloading || st.ActiveVersion != 1 || st.ReloadingSince.IsZero() {
		t.Fatalf("during prepare want reloading=true active=v1 with timestamp, got %+v", st)
	}

	// New business during prepare is served immediately on the OLD state.
	acqCh := make(chan *runtime.Lease, 1)
	go func() { acqCh <- a.Acquire() }()
	var leaseB *runtime.Lease
	select {
	case leaseB = <-acqCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Acquire blocked during slow prepare")
	}
	if port := leaseB.Snapshot().Config.GetInt("server.port"); port != 8080 {
		t.Fatalf("new request during prepare must be served old port 8080, got %d", port)
	}
	if oldB.isClosed() {
		t.Fatal("old plugin must stay open for in-flight A during prepare")
	}
	log.Printf("INPUT facade slow reload preparing (held), RuntimeStatus + Acquire issued mid-flight\n"+
		"JUDGEMENT status returned immediately reloading=%t active=v%d since=%v; new request served old port=8080; oldPluginOpen=%t",
		st.Reloading, st.ActiveVersion, st.ReloadingSince.Format(time.RFC3339Nano), !oldB.isClosed())

	// Release leases before publish so the old generation can drain, then let
	// the build finish.
	leaseA.Release()
	leaseB.Release()
	close(gb.release)
	if err := <-reloadDone; err != nil {
		t.Fatalf("reload: %v", err)
	}

	after := a.RuntimeStatus()
	if after.Reloading || after.ActiveVersion != 2 {
		t.Fatalf("after publish want reloading=false active=v2, got %+v", after)
	}
	leaseC := a.Acquire()
	if port := leaseC.Snapshot().Config.GetInt("server.port"); port != 9090 {
		t.Fatalf("request after publish must see new port 9090, got %d", port)
	}
	log.Printf("INPUT facade slow reload published to v2\n"+
		"JUDGEMENT reloading=%t active=v%d newRequestPort=9090", after.Reloading, after.ActiveVersion)
	leaseC.Release()

	if err := a.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
