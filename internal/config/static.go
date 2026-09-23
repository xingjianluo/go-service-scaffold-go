package config

// Static 是内存中的静态配置层（常用于测试或编程式默认值）。
type Static struct {
	name   string
	values map[string]string
}

// NewStatic 创建静态层；values 会被拷贝，后续外部修改不影响本层。
func NewStatic(name string, values map[string]string) *Static {
	cp := make(map[string]string, len(values))
	for k, v := range values {
		cp[k] = v
	}
	return &Static{name: name, values: cp}
}

func (s *Static) Name() string { return s.name }

func (s *Static) Load() (map[string]string, error) {
	out := make(map[string]string, len(s.values))
	for k, v := range s.values {
		out[k] = v
	}
	return out, nil
}
