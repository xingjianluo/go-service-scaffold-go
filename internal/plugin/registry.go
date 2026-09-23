package plugin

import (
	"fmt"
	"sort"
	"sync"
)

// Registry 保存已注册的插件集合，拒绝重复注册与静默覆盖。
// 它是并发安全的：注册通常发生在启动期，但读取（Get/Names）可能发生在热重载中。
type Registry struct {
	mu      sync.RWMutex
	plugins map[string]Plugin
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{plugins: map[string]Plugin{}}
}

// Register 注册一个插件；同名重复注册返回包装 ErrDuplicatePlugin 的错误，
// 绝不静默覆盖旧插件。
func (r *Registry) Register(p Plugin) error {
	if p == nil {
		return fmt.Errorf("%w: plugin must not be nil", ErrDuplicatePlugin)
	}
	name := p.Name()
	if name == "" {
		return fmt.Errorf("%w: plugin name must not be empty", ErrDuplicatePlugin)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.plugins[name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicatePlugin, name)
	}
	r.plugins[name] = p
	return nil
}

// MustRegister 与 Register 相同，但失败时 panic（用于包级 init 等无法处理错误的场景）。
func (r *Registry) MustRegister(p Plugin) {
	if err := r.Register(p); err != nil {
		panic(err)
	}
}

// Get 按名查询插件。
func (r *Registry) Get(name string) (Plugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.plugins[name]
	return p, ok
}

// Names 返回已注册的全部插件名（字典序，保证输出确定）。
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
