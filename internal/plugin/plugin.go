package plugin

import "context"

// Plugin 是可被编排的最小生命周期单元。
type Plugin interface {
	// Name 返回全局唯一的插件名。
	Name() string
	// DependsOn 返回所依赖的其他插件名，这些插件必定先于本插件初始化。
	DependsOn() []string
	// Init 执行初始化；返回错误后框架会回滚此前已启动的插件。
	Init(ctx context.Context) error
	// Close 释放资源；框架保证以初始化顺序的逆序调用。
	Close(ctx context.Context) error
}
