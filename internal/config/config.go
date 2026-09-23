package config

import (
	"sort"
	"strconv"
	"strings"
)

// redactedPlaceholder 是密钥字段出现在一切输出中的固定占位符。
const redactedPlaceholder = "***"

// Value 是一个已经按规约解析完成的、强类型的配置取值。
type Value struct {
	kind   FieldKind
	str    string
	i      int
	b      bool
	set    bool
	secret bool
}

// parseToken 把来源层中的原始字符串按字段类型解析成 Value。
// 解析失败返回 ErrTypeMismatch；错误中不携带原始 token（密钥可能就在其中）。
func parseToken(f FieldSpec, token string) (Value, error) {
	v := Value{kind: f.Kind, set: true, secret: f.Secret}
	switch f.Kind {
	case KindString:
		v.str = token
	case KindInt:
		n, err := strconv.Atoi(strings.TrimSpace(token))
		if err != nil {
			return Value{}, &FieldError{Code: ErrTypeMismatch, Key: f.Key, Reason: "value is not a valid int"}
		}
		v.i = n
		v.str = token
	case KindBool:
		b, err := strconv.ParseBool(strings.TrimSpace(token))
		if err != nil {
			return Value{}, &FieldError{Code: ErrTypeMismatch, Key: f.Key, Reason: "value is not a valid bool (expect true/false)"}
		}
		v.b = b
		v.str = token
	}
	return v, nil
}

// valueFromDefault 把规约中的类型化默认值转成 Value。
func valueFromDefault(f FieldSpec) Value {
	v := Value{kind: f.Kind, set: true, secret: f.Secret}
	switch t := f.Default.(type) {
	case string:
		v.str = t
	case int:
		v.i = t
		v.str = strconv.Itoa(t)
	case bool:
		v.b = t
		v.str = strconv.FormatBool(t)
	}
	return v
}

// String 返回值的字符串表示；密钥字段固定返回脱敏占位符，
// 因此把整个 Config 打印到日志时密钥不会出现。
func (v Value) String() string {
	if !v.set {
		return ""
	}
	if v.secret {
		return redactedPlaceholder
	}
	return v.str
}

// ExposeString 返回原始字符串，仅允许业务在明确需要时读取密钥原文
// （例如把口令交给下游客户端）。调用方有责任避免再次打印它。
func (v Value) ExposeString() string { return v.str }

// Int 返回整数取值。
func (v Value) Int() int { return v.i }

// Bool 返回布尔取值。
func (v Value) Bool() bool { return v.b }

// IsSet 报告该字段是否在某一层被显式提供或拥有默认值。
func (v Value) IsSet() bool { return v.set }

// IsSecret 报告该字段是否被声明为密钥。
func (v Value) IsSecret() bool { return v.secret }

// Config 是加载完成后的只读配置视图，并记录每个字段的来源层。
type Config struct {
	spec    *Spec
	values  map[string]Value
	origins map[string]string
}

// newConfig 由 Loader 在解析校验全部通过后构造；状态自此不可变。
func newConfig(spec *Spec, values map[string]Value, origins map[string]string) *Config {
	return &Config{spec: spec, values: values, origins: origins}
}

// Get 读取一个字段的解析值。
func (c *Config) Get(key string) (Value, bool) {
	v, ok := c.values[key]
	return v, ok
}

// Origin 返回字段最终取值来自哪一层（默认值/文件名/env）。
// 未设置的字段返回空串。
func (c *Config) Origin(key string) string { return c.origins[key] }

// Origins 返回全部字段的来源层副本（key 字典序无关，map 副本可安全外发）。
func (c *Config) Origins() map[string]string {
	out := make(map[string]string, len(c.origins))
	for k, v := range c.origins {
		out[k] = v
	}
	return out
}

// Redacted 返回所有已设置字段的字符串视图，密钥字段以占位符代替。
func (c *Config) Redacted() map[string]string {
	out := make(map[string]string, len(c.values))
	for k, v := range c.values {
		if v.IsSet() {
			out[k] = v.String()
		}
	}
	return out
}

// String 返回可安全打印的脱敏配置摘要（按 key 字典序，输出来源层）。
func (c *Config) String() string {
	keys := make([]string, 0, len(c.values))
	for k, v := range c.values {
		if v.IsSet() {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("config{")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		v := c.values[k]
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(v.String())
		b.WriteString(" [from:")
		b.WriteString(c.origins[k])
		b.WriteString("]")
	}
	b.WriteString("}")
	return b.String()
}
