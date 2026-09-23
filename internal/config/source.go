package config

// Source 表示一个配置来源层，统一产出 key -> 原始字符串 token 的映射。
type Source interface {
	// Name 返回层的唯一名称，用于来源解释与日志。
	Name() string
	// Load 读取该层原始数据；可选层不存在时由实现自行决定语义。
	Load() (map[string]string, error)
}

// Layer 是已经读取完成的配置层快照。
type Layer struct {
	Name   string
	Values map[string]string
}
