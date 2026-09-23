package config

import (
	"os"
	"strings"
)

// Env 从环境变量读取最高优先级的一层配置。
// key 转换规则：非字母数字替换为下划线并整体大写，再加前缀。
// 例如前缀 "APP_"、字段 "db.host" -> APP_DB_HOST。
// Env 只接受 Spec 中声明的字段，未声明的环境变量永远不会进入配置。
type Env struct {
	prefix string
	spec   *Spec
	lookup func(key string) (string, bool)
}

// NewEnv 创建环境变量层；lookup 为 nil 时使用 os.LookupEnv（便于测试注入）。
func NewEnv(prefix string, spec *Spec, lookup func(string) (string, bool)) *Env {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	return &Env{prefix: prefix, spec: spec, lookup: lookup}
}

func (e *Env) Name() string { return "env" }

func (e *Env) Load() (map[string]string, error) {
	out := map[string]string{}
	for _, key := range e.spec.Keys() {
		envName := EnvName(e.prefix, key)
		if raw, ok := e.lookup(envName); ok {
			out[key] = raw
		}
	}
	return out, nil
}

// EnvName 暴露确定的字段名 -> 环境变量名转换规则，供文档与测试引用。
func EnvName(prefix, key string) string {
	var b strings.Builder
	b.WriteString(strings.ToUpper(prefix))
	for _, r := range key {
		// 字段 key 约定为 ASCII；小写字母转大写，其余非字母数字统一变下划线。
		if r >= 'a' && r <= 'z' {
			b.WriteRune(r - ('a' - 'A'))
		} else if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
