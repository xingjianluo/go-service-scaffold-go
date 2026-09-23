package plugin

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
)

// rec is a shared call recorder so tests can assert lifecycle ordering.
type rec struct {
	mu    sync.Mutex
	calls []string
}

func (r *rec) append(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, s)
}

func (r *rec) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

type mockPlugin struct {
	name     string
	deps     []string
	initErr  error
	closeErr error
	r        *rec
}

func (m *mockPlugin) Name() string   { return m.name }
func (m *mockPlugin) Deps() []string { return m.deps }
func (m *mockPlugin) Init(context.Context) error {
	m.r.append("init:" + m.name)
	return m.initErr
}
func (m *mockPlugin) Close() error {
	m.r.append("close:" + m.name)
	return m.closeErr
}

func mustRegister(t *testing.T, reg *Registry, p Plugin) {
	t.Helper()
	if err := reg.Register(p); err != nil {
		t.Fatalf("register %q: %v", p.Name(), err)
	}
}

func TestDuplicateRegistration(t *testing.T) {
	r := &rec{}
	reg := NewRegistry()
	mustRegister(t, reg, &mockPlugin{name: "a", r: r})
	err := reg.Register(&mockPlugin{name: "a", r: r})
	log.Printf("INPUT register two plugins named 'a'\nJUDGEMENT error=%v", err)
	var pe *Error
	if !errors.As(err, &pe) || pe.Kind != PluginDuplicate {
		t.Fatalf("expected PluginDuplicate, got %v", err)
	}
	if err := reg.Register(nil); err == nil {
		t.Fatal("nil plugin must be rejected")
	}
	if err := reg.Register(&mockPlugin{name: "  ", r: r}); err == nil {
		t.Fatal("empty plugin name must be rejected")
	}
	if names := reg.Names(); len(names) != 1 || names[0] != "a" {
		t.Fatalf("duplicate must not overwrite registry, got %v", names)
	}
	log.Printf("JUDGEMENT registry unchanged: %v", reg.Names())
}

func TestMissingDependency(t *testing.T) {
	r := &rec{}
	reg := NewRegistry()
	mustRegister(t, reg, &mockPlugin{name: "a", deps: []string{"ghost"}, r: r})
	mgr := NewManager(reg)
	_, err := mgr.InitOrder()
	log.Printf("INPUT a depends on missing 'ghost'\nJUDGEMENT error=%v", err)
	var pe *Error
	if !errors.As(err, &pe) || pe.Kind != PluginMissingDep {
		t.Fatalf("expected PluginMissingDep, got %v", err)
	}
	if pe.Plugin != "a" {
		t.Fatalf("error should name plugin a, got %q", pe.Plugin)
	}
}

func TestDependencyCycle(t *testing.T) {
	r := &rec{}
	reg := NewRegistry()
	mustRegister(t, reg, &mockPlugin{name: "a", deps: []string{"b"}, r: r})
	mustRegister(t, reg, &mockPlugin{name: "b", deps: []string{"a"}, r: r})
	mustRegister(t, reg, &mockPlugin{name: "c", r: r})
	mgr := NewManager(reg)
	_, err := mgr.InitOrder()
	log.Printf("INPUT cycle a<->b plus independent c\nJUDGEMENT error=%v", err)
	var pe *Error
	if !errors.As(err, &pe) || pe.Kind != PluginCycle {
		t.Fatalf("expected PluginCycle, got %v", err)
	}
	// No plugin may have been touched by order resolution.
	if calls := r.snapshot(); len(calls) != 0 {
		t.Fatalf("cycle detection must not initialize anything, calls=%v", calls)
	}
}

func TestStartOrderAndReverseClose(t *testing.T) {
	r := &rec{}
	reg := NewRegistry()
	mustRegister(t, reg, &mockPlugin{name: "web", deps: []string{"db", "cache"}, r: r})
	mustRegister(t, reg, &mockPlugin{name: "db", r: r})
	mustRegister(t, reg, &mockPlugin{name: "cache", deps: []string{"db"}, r: r})
	mgr := NewManager(reg)

	order, err := mgr.InitOrder()
	if err != nil {
		t.Fatalf("InitOrder: %v", err)
	}
	log.Printf("INPUT web->[db,cache], cache->db\nJUDGEMENT init order=%v", order)
	wantOrder := []string{"db", "cache", "web"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("init order = %v, want %v", order, wantOrder)
	}

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !reflect.DeepEqual(mgr.Active(), wantOrder) {
		t.Fatalf("active = %v, want %v", mgr.Active(), wantOrder)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	calls := r.snapshot()
	log.Printf("JUDGEMENT full call trace=%v", calls)
	wantCalls := []string{"init:db", "init:cache", "init:web", "close:web", "close:cache", "close:db"}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls=%v, want %v", calls, wantCalls)
	}
}

func TestRollbackOnInitFailure(t *testing.T) {
	r := &rec{}
	reg := NewRegistry()
	mustRegister(t, reg, &mockPlugin{name: "db", r: r})
	mustRegister(t, reg, &mockPlugin{name: "cache", deps: []string{"db"}, r: r})
	mustRegister(t, reg, &mockPlugin{
		name: "web", deps: []string{"db", "cache"},
		initErr: errors.New("bind: address in use"), r: r,
	})
	mgr := NewManager(reg)

	err := mgr.Start(context.Background())
	var pe *Error
	if !errors.As(err, &pe) || pe.Kind != PluginInitFailed {
		t.Fatalf("expected PluginInitFailed, got %v", err)
	}
	log.Printf("INPUT web.Init fails after db,cache started\nJUDGEMENT %v | rolled-back=%v",
		err, pe.Rollback)
	wantRollback := []string{"cache", "db"} // reverse of start order
	if !reflect.DeepEqual(pe.Rollback, wantRollback) {
		t.Fatalf("rollback list=%v, want %v", pe.Rollback, wantRollback)
	}
	calls := r.snapshot()
	log.Printf("JUDGEMENT call trace=%v", calls)
	wantCalls := []string{"init:db", "init:cache", "init:web", "close:cache", "close:db"}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls=%v, want %v", calls, wantCalls)
	}
	// Manager must not report any active plugins after rollback.
	if len(mgr.Active()) != 0 {
		t.Fatalf("no plugin should remain active after rollback, got %v", mgr.Active())
	}
	// Closing again must be a harmless no-op.
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close after rollback should be no-op, got %v", err)
	}
}

func TestRollbackCloseErrorIsReported(t *testing.T) {
	r := &rec{}
	reg := NewRegistry()
	mustRegister(t, reg, &mockPlugin{
		name: "db", closeErr: errors.New("flush failed"), r: r,
	})
	mustRegister(t, reg, &mockPlugin{
		name: "web", deps: []string{"db"},
		initErr: fmt.Errorf("boom"), r: r,
	})
	mgr := NewManager(reg)
	err := mgr.Start(context.Background())
	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("expected plugin error, got %v", err)
	}
	log.Printf("INPUT web init fails and db close fails\nJUDGEMENT %v | closeErr=%v",
		err, pe.CloseErr)
	if pe.Kind != PluginInitFailed || pe.CloseErr == nil {
		t.Fatalf("init failure must also surface rollback close errors, got kind=%v closeErr=%v",
			pe.Kind, pe.CloseErr)
	}
}

func TestDeterministicTieBreak(t *testing.T) {
	r := &rec{}
	reg := NewRegistry()
	for _, n := range []string{"zeta", "alpha", "mid"} {
		mustRegister(t, reg, &mockPlugin{name: n, r: r})
	}
	order, err := NewManager(reg).InitOrder()
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("INPUT three independent plugins\nJUDGEMENT order=%v", order)
	want := []string{"alpha", "mid", "zeta"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v, want %v", order, want)
	}
}
