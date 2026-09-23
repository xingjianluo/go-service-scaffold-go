package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// File 从 JSON 文件读取一层配置（标量字段，编码/json 标准库解析）。
// JSON 中只允许 string / number（整数）/ bool 三种标量值；
// 不支持嵌套对象与数组——所有配置字段都是扁平 key。
type File struct {
	Path     string
	Optional bool
}

func (f *File) Name() string { return "file:" + f.Path }

func (f *File) Load() (map[string]string, error) {
	raw, err := os.ReadFile(f.Path)
	if err != nil {
		if f.Optional && os.IsNotExist(err) {
			// 可选文件不存在等价于空层，不是失败。
			return map[string]string{}, nil
		}
		return nil, &FieldError{
			Code:   ErrSourceFailure,
			Key:    "-",
			Layer:  f.Name(),
			Reason: "cannot read config file: " + err.Error(),
		}
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, &FieldError{
			Code:   ErrSourceFailure,
			Key:    "-",
			Layer:  f.Name(),
			Reason: "config file is not a valid JSON object: " + err.Error(),
		}
	}

	out := make(map[string]string, len(decoded))
	for k, v := range decoded {
		tok, err := scalarToken(k, v)
		if err != nil {
			return nil, &FieldError{
				Code:   ErrSourceFailure,
				Key:    k,
				Layer:  f.Name(),
				Reason: err.Error(),
			}
		}
		out[k] = tok
	}
	return out, nil
}

func scalarToken(key string, v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case bool:
		return strconv.FormatBool(t), nil
	case float64:
		// encoding/json 把数字解码为 float64；要求配置数字必须是整数。
		if t != float64(int64(t)) {
			return "", fmt.Errorf("field %s must be a scalar int/bool/string, got non-integer number", key)
		}
		return strconv.FormatInt(int64(t), 10), nil
	case nil:
		return "", fmt.Errorf("field %s must be a scalar int/bool/string, got null", key)
	default:
		return "", fmt.Errorf("field %s must be a scalar int/bool/string, got composite value", key)
	}
}
