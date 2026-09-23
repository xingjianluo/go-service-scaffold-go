package config

import "strings"

// ErrorCode 用可区分的错误码标识配置失败的具体原因。
type ErrorCode string

const (
	// ErrMissingRequired 必填字段在所有层中均缺失。
	ErrMissingRequired ErrorCode = "missing_required"
	// ErrTypeMismatch 取值无法解析为字段声明的类型。
	ErrTypeMismatch ErrorCode = "type_mismatch"
	// ErrInvalidValue 取值类型正确但违反 Validator 约束。
	ErrInvalidValue ErrorCode = "invalid_value"
	// ErrUnknownField 出现了规约中未声明的字段。
	ErrUnknownField ErrorCode = "unknown_field"
	// ErrSourceFailure 配置源本身读取失败（如文件不可读）。
	ErrSourceFailure ErrorCode = "source_failure"
)

// FieldError 描述单个字段的配置错误。
// 出于安全要求，错误信息只包含字段名、来源层与原因，绝不包含字段取值，
// 因此即使是密钥字段，也不可能通过错误信息泄露。
type FieldError struct {
	Code   ErrorCode
	Key    string
	Reason string
	Layer  string
}

func (e *FieldError) Error() string {
	var b strings.Builder
	b.WriteString("config: ")
	b.WriteString(string(e.Code))
	b.WriteString(" for key ")
	b.WriteString(e.Key)
	if e.Layer != "" {
		b.WriteString(" in layer ")
		b.WriteString(e.Layer)
	}
	if e.Reason != "" {
		b.WriteString(": ")
		b.WriteString(e.Reason)
	}
	return b.String()
}

// LoadError 聚合同一次加载中的全部字段错误。
type LoadError struct {
	Errs []FieldError
}

func (e *LoadError) Error() string {
	parts := make([]string, 0, len(e.Errs))
	for i := range e.Errs {
		parts = append(parts, e.Errs[i].Error())
	}
	return "config: load failed with " + itoa(len(e.Errs)) + " error(s):\n  - " + strings.Join(parts, "\n  - ")
}

// ByCode 返回第一个携带指定错误码的字段错误，便于调用方区分失败原因。
func (e *LoadError) ByCode(code ErrorCode) (FieldError, bool) {
	for _, fe := range e.Errs {
		if fe.Code == code {
			return fe, true
		}
	}
	return FieldError{}, false
}

// Codes 返回本次加载涉及的全部错误码（按字段错误出现顺序、去重）。
func (e *LoadError) Codes() []ErrorCode {
	seen := map[ErrorCode]struct{}{}
	out := make([]ErrorCode, 0, len(e.Errs))
	for _, fe := range e.Errs {
		if _, ok := seen[fe.Code]; !ok {
			seen[fe.Code] = struct{}{}
			out = append(out, fe.Code)
		}
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
