package config

import "sort"

// FieldKind 标识一个配置字段的标量类型。
type FieldKind int

const (
	// KindString 字符串类型。
	KindString FieldKind = iota
	// KindInt 整数类型。
	KindInt
	// KindBool 布尔类型。
	KindBool
)

// Validator 在类型解析通过后对取值做进一步的合法性校验。
type Validator func(v Value) error

// FieldSpec 描述一个配置字段的规约：类型、必填、默认值、是否密钥以及自定义校验。
type FieldSpec struct {
	Key      string
	Kind     FieldKind
	Required bool
	Secret   bool
	Default  any
	Validate Validator
	Help     string
}

// Spec 是不可变的字段规约集合。
type Spec struct {
	fields map[string]FieldSpec
}

// NewSpec 创建空的字段规约。
func NewSpec() *Spec {
	return &Spec{fields: map[string]FieldSpec{}}
}

// Add 注册一个字段规约，并返回 Spec 自身以支持链式调用。
// 字段规约是构建期产物，重复 Add 同一个 key 属于编程错误，以 panic 形式尽早暴露。
func (s *Spec) Add(f FieldSpec) *Spec {
	if f.Key == "" {
		panic("config: FieldSpec.Key must not be empty")
	}
	if _, dup := s.fields[f.Key]; dup {
		panic("config: duplicate field spec: " + f.Key)
	}
	if err := validateDefault(f); err != nil {
		panic("config: invalid default for field " + f.Key + ": " + err.Error())
	}
	s.fields[f.Key] = f
	return s
}

// Field 按 key 查询字段规约。
func (s *Spec) Field(key string) (FieldSpec, bool) {
	f, ok := s.fields[key]
	return f, ok
}

// Keys 返回全部字段 key（字典序，保证输出与处理顺序确定）。
func (s *Spec) Keys() []string {
	keys := make([]string, 0, len(s.fields))
	for k := range s.fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func validateDefault(f FieldSpec) error {
	if f.Default == nil {
		return nil
	}
	switch f.Kind {
	case KindString:
		if _, ok := f.Default.(string); !ok {
			return &FieldError{Code: ErrTypeMismatch, Key: f.Key, Reason: "default must be string"}
		}
	case KindInt:
		if _, ok := f.Default.(int); !ok {
			return &FieldError{Code: ErrTypeMismatch, Key: f.Key, Reason: "default must be int"}
		}
	case KindBool:
		if _, ok := f.Default.(bool); !ok {
			return &FieldError{Code: ErrTypeMismatch, Key: f.Key, Reason: "default must be bool"}
		}
	}
	return nil
}
