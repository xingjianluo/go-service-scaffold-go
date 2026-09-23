package runtime

// State 是一次"完整运行时状态"的不可变快照，热重载时整体替换。
// 具体业务把解析后的配置、插件管理器/客户端等放入 Data。
type State struct {
	// Data 携带业务数据（如 "config"、"manager"）。State 一旦装入 Holder 即视为不可变，
	// 读者只能读取，不得修改 Data 内部对象；需要变化时应构建新 State 整体替换。
	Data map[string]any

	// Close 可选；在旧状态已没有任何读者、被新状态替换或 Holder 关闭时调用。
	// 框架保证：同一 State 的 Close 恰好调用一次，且调用时不存在任何在途读者。
	Close func()
}
