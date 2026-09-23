package config

import "strings"

// Loader 按固定优先级把多个 Source 与默认值合并，再依据 Spec 解析与校验。
//
// 优先级从低到高（后者覆盖前者）：
//
//	FieldSpec.Default（默认值，最低）
//	-> 第 1 个 AddLayer（如 flags/静态层）
//	-> 第 2 个 AddLayer（如配置文件）
//	-> 最后一个 AddLayer（如环境变量，最高）
//
// 所有层读取成功后才开始合并与校验；任何一层读取失败，整次加载失败。
type Loader struct {
	spec   *Spec
	layers []Source
}

// NewLoader 基于字段规约创建加载器。
func NewLoader(spec *Spec) *Loader {
	return &Loader{spec: spec}
}

// AddLayer 追加一个配置层；先添加的层优先级低，后添加的层优先级高。
func (l *Loader) AddLayer(src Source) *Loader {
	l.layers = append(l.layers, src)
	return l
}

// Load 执行合并、类型解析与校验，产出不可变 Config；失败时返回 *LoadError，
// 其中收集所有字段的问题（不短路），便于一次性给出全部可解释的失败原因。
func (l *Loader) Load() (*Config, error) {
	var fieldErrs []FieldError

	// 1) 读取所有层。层按注册顺序处理；首个读取失败即终止
	//    （此时无法保证合并优先级的语义，部分结果没有意义）。
	snapshots := make([]Layer, 0, len(l.layers))
	for _, src := range l.layers {
		raw, err := src.Load()
		if err != nil {
			if fe, ok := err.(*FieldError); ok {
				fieldErrs = append(fieldErrs, *fe)
				return nil, &LoadError{Errs: fieldErrs}
			}
			fieldErrs = append(fieldErrs, FieldError{
				Code:   ErrSourceFailure,
				Key:    "-",
				Layer:  src.Name(),
				Reason: err.Error(),
			})
			return nil, &LoadError{Errs: fieldErrs}
		}
		snapshots = append(snapshots, Layer{Name: src.Name(), Values: raw})
	}

	// 2) 拒绝未知字段：任何层出现 Spec 未声明的 key 都立即记入错误，
	//    避免拼写错误的配置被静默忽略。
	for _, layer := range snapshots {
		for key := range layer.Values {
			if _, ok := l.spec.Field(key); !ok {
				fieldErrs = append(fieldErrs, FieldError{
					Code:   ErrUnknownField,
					Key:    key,
					Layer:  layer.Name,
					Reason: "field is not declared in spec",
				})
			}
		}
	}

	// 3) 逐字段按优先级合并并解析。
	values := make(map[string]Value, len(l.spec.Keys()))
	origins := make(map[string]string, len(l.spec.Keys()))
	for _, key := range l.spec.Keys() {
		f, _ := l.spec.Field(key)

		// 默认值是最低优先级层。
		if f.Default != nil {
			values[key] = valueFromDefault(f)
			origins[key] = "default"
		}

		// 快照按低 -> 高遍历，高优先级覆盖低优先级。
		var winner string
		var winnerToken string
		for _, layer := range snapshots {
			if tok, ok := layer.Values[key]; ok {
				winnerToken = tok
				winner = layer.Name
			}
		}
		if winner != "" {
			parsed, err := parseToken(f, winnerToken)
			if err != nil {
				fe := err.(*FieldError)
				fe.Layer = winner
				fieldErrs = append(fieldErrs, *fe)
				// 显式提供的值类型非法时，不得回退使用默认值继续跑后续校验，
				// 否则会在一个明确的类型错误之外再产生误导性的第二条错误。
				delete(values, key)
				delete(origins, key)
				continue
			}
			values[key] = parsed
			origins[key] = winner
		}

		// 必填检查：在合并之后进行——任一层提供都算满足。
		if f.Required {
			if v, ok := values[key]; !ok || !v.IsSet() {
				fieldErrs = append(fieldErrs, FieldError{
					Code:   ErrMissingRequired,
					Key:    key,
					Reason: "required field is not set in any layer and has no default",
				})
				continue
			}
		}

		// 自定义合法性校验只在类型解析成功后执行。
		if f.Validate != nil {
			if v, ok := values[key]; ok && v.IsSet() {
				if verr := f.Validate(v); verr != nil {
					reason := safeReason(verr)
					if f.Secret {
						// 框架级兜底：即便 Validator 误把密钥原文写进原因，也在此抹除，
						// 保证密钥绝不出现在错误信息中。
						reason = redactSubstring(reason, v.ExposeString())
					}
					fieldErrs = append(fieldErrs, FieldError{
						Code:   ErrInvalidValue,
						Key:    key,
						Layer:  origins[key],
						Reason: reason,
					})
				}
			}
		}
	}

	if len(fieldErrs) > 0 {
		return nil, &LoadError{Errs: fieldErrs}
	}
	return newConfig(l.spec, values, origins), nil
}

// safeReason 从 Validator 返回的错误中提取原因文本。
// 这里只做直接透传；Validator 的作者被契约要求不得在信息中包含密钥原文，
// 框架另外通过 Value.String()/Config.String() 的脱敏保证输出安全。
func safeReason(err error) string {
	return err.Error()
}

// redactSubstring 把 reason 中出现的所有 secret 原文替换为脱敏占位符；
// secret 为空时不做处理（空串 ReplaceAll 会在每个字符间插入，没有意义）。
func redactSubstring(reason, secret string) string {
	if secret == "" {
		return reason
	}
	return strings.ReplaceAll(reason, secret, redactedPlaceholder)
}
