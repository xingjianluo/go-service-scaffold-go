package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Order 是依赖排序后确定的初始化顺序。
type Order struct{ names []string }

// Names 返回初始化顺序中的插件名（关闭顺序为其逆序）。
func (o *Order) Names() []string {
	out := make([]string, len(o.names))
	copy(out, o.names)
	return out
}

// TopoOrder 根据注册表计算满足依赖关系的初始化顺序。
//
// 排序使用 Kahn 算法，并在每一层候选节点中按字典序挑选，
// 从而在依赖关系相同时给出确定、可解释、可复现的顺序。
// 缺依赖返回包装 ErrDependencyMissing 的错误（一次列出全部缺失），
// 有环返回包装 ErrCircularDependency 的错误（列出环上剩余节点）。
func TopoOrder(r *Registry) (*Order, error) {
	names := r.Names()
	indegree := make(map[string]int, len(names))
	dependents := make(map[string][]string, len(names)) // dep -> 依赖它的插件
	for _, name := range names {
		p, _ := r.Get(name)
		deps := p.DependsOn()
		// 去重，避免同一依赖声明两次把入度错误地加 2。
		seen := map[string]struct{}{}
		for _, d := range deps {
			if _, dup := seen[d]; dup {
				continue
			}
			seen[d] = struct{}{}
			if _, ok := r.Get(d); !ok {
				// 收集缺失依赖由下方统一报错更友好，这里先标记为入度 -1 哨兵。
				indegree[name]--
				continue
			}
			indegree[name]++
			dependents[d] = append(dependents[d], name)
		}
	}

	// 依赖缺失检查：单独再扫一遍，报告所有 (插件, 缺失依赖) 对。
	var missing []string
	for _, name := range names {
		p, _ := r.Get(name)
		seen := map[string]struct{}{}
		for _, d := range p.DependsOn() {
			if _, ok := seen[d]; ok {
				continue
			}
			seen[d] = struct{}{}
			if _, ok := r.Get(d); !ok {
				missing = append(missing, fmt.Sprintf("%s -> %s", name, d))
			}
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %v", ErrDependencyMissing, missing)
	}

	// Kahn 算法：从入度为 0 的节点开始。names 已字典序，初始化就绪堆。
	ready := make([]string, 0)
	for _, n := range names {
		if indegree[n] == 0 {
			ready = append(ready, n)
		}
	}

	order := make([]string, 0, len(names))
	for len(ready) > 0 {
		// ready 始终保持字典序：取第一个，插入新就绪节点后重排。
		cur := ready[0]
		ready = ready[1:]
		order = append(order, cur)
		for _, dep := range dependents[cur] {
			indegree[dep]--
			if indegree[dep] == 0 {
				ready = append(ready, dep)
			}
		}
		sortStrings(ready)
	}

	if len(order) != len(names) {
		// 剩余节点都处于某个环中。
		remaining := map[string]struct{}{}
		for _, n := range names {
			remaining[n] = struct{}{}
		}
		for _, n := range order {
			delete(remaining, n)
		}
		cyclic := make([]string, 0, len(remaining))
		for n := range remaining {
			cyclic = append(cyclic, n)
		}
		sortStrings(cyclic)
		return nil, fmt.Errorf("%w: plugins involved: %v", ErrCircularDependency, cyclic)
	}
	return &Order{names: order}, nil
}

// Manager 持有注册表与生命周期状态，负责有序初始化、失败回滚与有序关闭。
type Manager struct {
	reg     *Registry
	mu      sync.Mutex
	running bool
	closed  bool
	order   []string // 已启动插件的初始化顺序
	started map[string]Plugin
}

// NewManager 创建管理器。
func NewManager(r *Registry) *Manager {
	return &Manager{reg: r, started: map[string]Plugin{}}
}

// Start 计算拓扑顺序并依次 Init。
//
// 任一插件 Init 失败：
//  1. 不再继续启动后续插件；
//  2. 对已经 Init 成功的插件严格按逆序调用 Close 回滚；
//  3. 返回包装 ErrInitFailed 的错误（内含失败插件名与回滚中产生的错误），
//     管理器回到"未运行"状态，不残留任何半初始化插件。
//
// 上下文取消同样按失败处理并回滚。
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrPluginClosed
	}
	if m.running {
		m.mu.Unlock()
		return errors.New("plugin: manager already started")
	}
	m.mu.Unlock()

	order, err := TopoOrder(m.reg)
	if err != nil {
		return err
	}

	booted := make([]string, 0, len(order.names))
	for _, name := range order.names {
		if err := ctx.Err(); err != nil {
			return m.rollback(ctx, booted, fmt.Errorf("%w: %s before init: %w", ErrInitFailed, name, err))
		}
		p, _ := m.reg.Get(name)
		if err := p.Init(ctx); err != nil {
			return m.rollback(ctx, booted, fmt.Errorf("%w: plugin %s: %w", ErrInitFailed, name, err))
		}
		booted = append(booted, name)
	}

	m.mu.Lock()
	m.running = true
	m.order = booted
	for _, name := range booted {
		p, _ := m.reg.Get(name)
		m.started[name] = p
	}
	m.mu.Unlock()
	return nil
}

// rollback 以初始化的逆序关闭已启动插件，聚合回滚错误后返回。
func (m *Manager) rollback(ctx context.Context, booted []string, cause error) error {
	var errs []error
	errs = append(errs, cause)
	for i := len(booted) - 1; i >= 0; i-- {
		p, _ := m.reg.Get(booted[i])
		if cerr := p.Close(ctx); cerr != nil {
			errs = append(errs, fmt.Errorf("rollback %s: %w", booted[i], cerr))
		}
	}
	m.mu.Lock()
	m.running = false
	m.order = nil
	m.started = map[string]Plugin{}
	m.mu.Unlock()
	return errors.Join(errs...)
}

// CloseAll 以初始化顺序的逆序关闭全部已启动插件。
// 即使某个 Close 出错也会继续关闭其余插件（尽力释放资源），
// 最后用 errors.Join 聚合所有错误；关闭后管理器不可再次 Start。
func (m *Manager) CloseAll(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrPluginClosed
	}
	m.closed = true
	running := m.running
	order := m.order
	m.running = false
	m.order = nil
	m.started = map[string]Plugin{}
	m.mu.Unlock()

	if !running {
		return nil
	}
	var errs []error
	for i := len(order) - 1; i >= 0; i-- {
		p, _ := m.reg.Get(order[i])
		if err := p.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", order[i], err))
		}
	}
	return errors.Join(errs...)
}

// Started 返回当前已完成 Init 且未被关闭的插件名（按启动顺序）。
func (m *Manager) Started() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.order))
	copy(out, m.order)
	return out
}

// IsRunning 报告管理器是否处于运行态。
func (m *Manager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// Get 返回已启动的插件，供业务按名取用。
func (m *Manager) Get(name string) (Plugin, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.started[name]
	return p, ok
}

// sortStrings 是 sort.Strings 的小包内别名，避免在文件中分散 import。
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
