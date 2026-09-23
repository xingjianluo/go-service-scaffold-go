package plugin

// 插件编排的可区分错误原因。
var (
	// ErrDuplicatePlugin 同名插件被重复注册。
	ErrDuplicatePlugin = duplicateError("duplicate plugin")
	// ErrDependencyMissing DependsOn 引用了从未注册的插件。
	ErrDependencyMissing = dependencyMissingError("dependency missing")
	// ErrCircularDependency 插件依赖关系中存在环。
	ErrCircularDependency = circularError("circular dependency")
	// ErrInitFailed 某个插件 Init 返回失败。
	ErrInitFailed = initFailedError("plugin init failed")
	// ErrPluginClosed 插件管理器已关闭后又被调用。
	ErrPluginClosed = closedError("plugin manager closed")
)

type duplicateError string

func (e duplicateError) Error() string { return string(e) }

type dependencyMissingError string

func (e dependencyMissingError) Error() string { return string(e) }

type circularError string

func (e circularError) Error() string { return string(e) }

type initFailedError string

func (e initFailedError) Error() string { return string(e) }

type closedError string

func (e closedError) Error() string { return string(e) }
