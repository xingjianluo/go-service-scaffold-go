// Package plugin provides plugin registration and lifecycle orchestration.
//
// Contract:
//
//   - Register rejects empty names, nil plugins and duplicate names; it never
//     silently overwrites an existing registration.
//   - Init order is derived from Deps() via a topological sort (Kahn), with
//     ties broken alphabetically so results are deterministic.
//   - Missing dependencies and dependency cycles are refused before any
//     plugin is initialized.
//   - If Init fails for any plugin, every already-started plugin is Closed in
//     reverse order; no half-initialized manager remains.
//   - Normal shutdown closes plugins in reverse initialization order.
package plugin

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Plugin is the lifecycle contract every plugin must implement.
type Plugin interface {
	// Name is the unique registration name.
	Name() string
	// Deps lists names of plugins that must be initialized before this one.
	Deps() []string
	// Init starts the plugin. The context may cancel startup.
	Init(ctx context.Context) error
	// Close releases all resources held by the plugin.
	Close() error
}

// FailureKind classifies plugin errors.
type FailureKind int

const (
	PluginFailureUnknown FailureKind = iota
	PluginDuplicate
	PluginMissingDep
	PluginCycle
	PluginInitFailed
	PluginCloseFailed
)

func (k FailureKind) String() string {
	switch k {
	case PluginDuplicate:
		return "duplicate_registration"
	case PluginMissingDep:
		return "missing_dependency"
	case PluginCycle:
		return "dependency_cycle"
	case PluginInitFailed:
		return "init_failed"
	case PluginCloseFailed:
		return "close_failed"
	default:
		return "unknown"
	}
}

// Error wraps plugin failures with a distinguishable kind.
type Error struct {
	// Kind is the machine-readable failure classification.
	Kind FailureKind
	// Plugin is the plugin name involved (empty for registry-wide failures).
	Plugin string
	// Detail carries a human-readable explanation without secret values.
	Detail string
	// WrapErr is the underlying error, if any.
	WrapErr error
	// Rollback lists plugins that were closed while rolling back a failed Start.
	Rollback []string
	// CloseErr aggregates errors observed during rollback/close, if any.
	CloseErr error
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "plugin error: kind=%s", e.Kind)
	if e.Plugin != "" {
		fmt.Fprintf(&b, " plugin=%q", e.Plugin)
	}
	if e.Detail != "" {
		fmt.Fprintf(&b, ": %s", e.Detail)
	}
	if e.WrapErr != nil {
		fmt.Fprintf(&b, ": %v", e.WrapErr)
	}
	if len(e.Rollback) > 0 {
		fmt.Fprintf(&b, " (rolled back: %s)", strings.Join(e.Rollback, ", "))
	}
	if e.CloseErr != nil {
		fmt.Fprintf(&b, "; rollback close errors: %v", e.CloseErr)
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.WrapErr }

// closeError aggregates one or more failed Close calls.
type closeError struct {
	failures map[string]error
}

func (e *closeError) Error() string {
	names := make([]string, 0, len(e.failures))
	for n := range e.failures {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%q: %v", n, e.failures[n]))
	}
	return "close failed for plugin(s) " + strings.Join(parts, "; ")
}

// Registry validates and stores plugin registrations.
type Registry struct {
	plugins map[string]Plugin
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{plugins: make(map[string]Plugin)}
}

// Register adds p. It rejects nil plugins, empty names and duplicate names
// instead of silently replacing a previous registration.
func (r *Registry) Register(p Plugin) error {
	if p == nil {
		return &Error{Kind: PluginDuplicate, Detail: "cannot register nil plugin"}
	}
	name := p.Name()
	if strings.TrimSpace(name) == "" {
		return &Error{Kind: PluginDuplicate, Detail: "plugin name must not be empty"}
	}
	if _, exists := r.plugins[name]; exists {
		return &Error{Kind: PluginDuplicate, Plugin: name,
			Detail: "plugin already registered; duplicate registration is refused"}
	}
	r.plugins[name] = p
	return nil
}

// Get returns the registered plugin with the given name.
func (r *Registry) Get(name string) (Plugin, bool) {
	p, ok := r.plugins[name]
	return p, ok
}

// Names returns all registered plugin names in sorted order.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Manager starts plugins in dependency order and closes them in reverse.
type Manager struct {
	registry *Registry
	order    []string
	active   []Plugin
}

// NewManager creates a Manager over the given registry.
func NewManager(r *Registry) *Manager {
	return &Manager{registry: r}
}

// InitOrder returns the dependency-resolved initialization order.
// It fails with PluginMissingDep or PluginCycle before touching any plugin.
func (m *Manager) InitOrder() ([]string, error) {
	indeg := make(map[string]int, len(m.registry.plugins))
	dependents := make(map[string][]string)

	for name, p := range m.registry.plugins {
		deps := p.Deps()
		seen := make(map[string]struct{}, len(deps))
		var missing []string
		for _, d := range deps {
			if _, dup := seen[d]; dup {
				continue
			}
			seen[d] = struct{}{}
			if d == name {
				// Self-dependency is a cycle; leave a positive indegree.
				indeg[name]++
				continue
			}
			if _, ok := m.registry.plugins[d]; !ok {
				missing = append(missing, d)
				continue
			}
			indeg[name]++
			dependents[d] = append(dependents[d], name)
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return nil, &Error{
				Kind:   PluginMissingDep,
				Plugin: name,
				Detail: "plugin depends on unregistered plugin(s): " + strings.Join(missing, ", "),
			}
		}
	}

	// Kahn's algorithm; ready nodes are kept alphabetically sorted.
	ready := make([]string, 0)
	for name := range m.registry.plugins {
		if indeg[name] == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)

	order := make([]string, 0, len(m.registry.plugins))
	for len(ready) > 0 {
		cur := ready[0]
		ready = ready[1:]
		order = append(order, cur)

		freed := dependents[cur]
		sort.Strings(freed)
		for _, dep := range freed {
			indeg[dep]--
			if indeg[dep] == 0 {
				ready = append(ready, dep)
			}
		}
		sort.Strings(ready)
	}

	if len(order) != len(m.registry.plugins) {
		cyclic := make([]string, 0)
		for name, n := range indeg {
			if n > 0 {
				cyclic = append(cyclic, name)
			}
		}
		sort.Strings(cyclic)
		return nil, &Error{
			Kind:   PluginCycle,
			Detail: "dependency cycle detected among plugin(s): " + strings.Join(cyclic, ", "),
		}
	}
	return order, nil
}

// Start resolves the order and initializes every plugin. On Init failure it
// closes the already-started plugins in reverse order and returns an
// *Error of Kind PluginInitFailed carrying the Rollback list.
func (m *Manager) Start(ctx context.Context) error {
	order, err := m.InitOrder()
	if err != nil {
		return err
	}
	m.order = order
	m.active = m.active[:0]

	for _, name := range order {
		p := m.registry.plugins[name]
		if ierr := p.Init(ctx); ierr != nil {
			rolledBack := m.activeNamesReverse()
			closeErr := m.rollback()
			return &Error{
				Kind:     PluginInitFailed,
				Plugin:   name,
				WrapErr:  ierr,
				Detail:   "initialization failed; already-started plugins were rolled back",
				Rollback: rolledBack,
				CloseErr: closeErr,
			}
		}
		m.active = append(m.active, p)
	}
	return nil
}

// activeNamesReverse returns the active plugin names in reverse start order,
// i.e. the order in which rollback closes them.
func (m *Manager) activeNamesReverse() []string {
	out := make([]string, 0, len(m.active))
	for i := len(m.active) - 1; i >= 0; i-- {
		out = append(out, m.active[i].Name())
	}
	return out
}

// rollback closes active plugins in reverse order and resets manager state.
func (m *Manager) rollback() error {
	failures := make(map[string]error)
	for i := len(m.active) - 1; i >= 0; i-- {
		p := m.active[i]
		if cerr := p.Close(); cerr != nil {
			failures[p.Name()] = cerr
		}
	}
	m.active = m.active[:0]
	m.order = nil
	if len(failures) > 0 {
		return &closeError{failures: failures}
	}
	return nil
}

// Close shuts down active plugins in reverse initialization order.
// It is safe to call after a failed Start: only successfully started plugins
// are closed. All close failures are collected into one PluginCloseFailed
// error rather than aborting the shutdown early.
func (m *Manager) Close() error {
	failures := make(map[string]error)
	closed := make([]string, 0, len(m.active))
	for i := len(m.active) - 1; i >= 0; i-- {
		p := m.active[i]
		closed = append(closed, p.Name())
		if cerr := p.Close(); cerr != nil {
			failures[p.Name()] = cerr
		}
	}
	m.active = m.active[:0]
	m.order = nil
	if len(failures) > 0 {
		return &Error{
			Kind:     PluginCloseFailed,
			Detail:   "one or more plugins failed to close",
			Rollback: closed,
			CloseErr: &closeError{failures: failures},
		}
	}
	return nil
}

// Active returns the names of successfully started plugins in start order.
func (m *Manager) Active() []string {
	out := make([]string, 0, len(m.active))
	for _, p := range m.active {
		out = append(out, p.Name())
	}
	return out
}
