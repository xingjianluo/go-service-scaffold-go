package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// logCase 统一打印测试输入与判定依据，满足"单测日志中需要打印输入与判定依据"。
func logCase(t *testing.T, name string, input string, want string) {
	t.Helper()
	t.Logf("用例 %s | 输入: %s | 判定依据: %s", name, input, want)
}

func testSpec() *Spec {
	return NewSpec().
		Add(FieldSpec{Key: "app.name", Kind: KindString, Required: true}).
		Add(FieldSpec{Key: "server.host", Kind: KindString, Default: "0.0.0.0"}).
		Add(FieldSpec{Key: "server.port", Kind: KindInt, Default: 8080,
			Validate: func(v Value) error {
				if v.Int() < 1 || v.Int() > 65535 {
					return errors.New("port out of range")
				}
				return nil
			}}).
		Add(FieldSpec{Key: "server.debug", Kind: KindBool, Default: false}).
		Add(FieldSpec{Key: "db.password", Kind: KindString, Required: true, Secret: true})
}

func TestLoad_LayeredMergeAndPriority(t *testing.T) {
	logCase(t, "TestLoad_LayeredMergeAndPriority",
		"default{host=0.0.0.0,port=8080} + file{port=9090,debug=true} + env{name=app,port=7070,pw=topsecret}",
		"env 覆盖 file 覆盖 default；Origin 必须指向最终来源层")

	dir := t.TempDir()
	path := filepath.Join(dir, "app.json")
	if err := os.WriteFile(path, []byte(`{
		"server.port": 9090,
		"server.debug": true
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	env := map[string]string{
		"APP_APP_NAME":    "app",
		"APP_SERVER_PORT": "7070", // env 优先级高于文件
		"APP_DB_PASSWORD": "topsecret",
	}
	cfg, err := NewLoader(testSpec()).
		AddLayer(&File{Path: path}).
		AddLayer(NewEnv("APP_", testSpec(), fakeLookup(env))).
		Load()
	if err != nil {
		t.Fatalf("期望加载成功，得到 %v", err)
	}

	if v, _ := cfg.Get("app.name"); v.String() != "app" || cfg.Origin("app.name") != "env" {
		t.Errorf("app.name 判定失败: value=%q origin=%q", v.String(), cfg.Origin("app.name"))
	}
	if v, _ := cfg.Get("server.host"); v.String() != "0.0.0.0" || cfg.Origin("server.host") != "default" {
		t.Errorf("server.host 应取默认值，得到 %q/%q", v.String(), cfg.Origin("server.host"))
	}
	if v, _ := cfg.Get("server.port"); v.Int() != 7070 || cfg.Origin("server.port") != "env" {
		t.Errorf("server.port 应被 env 覆盖为 7070，得到 %d/%q", v.Int(), cfg.Origin("server.port"))
	}
	if v, _ := cfg.Get("server.debug"); v.Bool() != true || cfg.Origin("server.debug") != "file:"+path {
		t.Errorf("server.debug 应来自文件为 true，得到 %v/%q", v.Bool(), cfg.Origin("server.debug"))
	}
	t.Logf("合并结果(脱敏): %s", cfg.String())
}

func TestLoad_MissingRequired(t *testing.T) {
	logCase(t, "TestLoad_MissingRequired", "所有层均不提供 db.password",
		"必填缺失必须返回 ErrMissingRequired，且不被其他错误掩盖")

	_, err := NewLoader(testSpec()).
		AddLayer(NewStatic("flags", map[string]string{"app.name": "x"})).
		Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("期望 *LoadError，得到 %T %v", err, err)
	}
	fe, ok := le.ByCode(ErrMissingRequired)
	if !ok || fe.Key != "db.password" {
		t.Fatalf("期望 db.password 的 missing_required，得到 %+v", le.Errs)
	}
	t.Logf("得到预期错误: %v", fe)
}

func TestLoad_TypeMismatch(t *testing.T) {
	logCase(t, "TestLoad_TypeMismatch", "env APP_SERVER_PORT=abc, APP_SERVER_DEBUG=maybe",
		"无法解析为 int/bool 必须分别报 ErrTypeMismatch，错误一次性收集")

	env := map[string]string{
		"APP_APP_NAME":     "app",
		"APP_DB_PASSWORD":  "x",
		"APP_SERVER_PORT":  "abc",
		"APP_SERVER_DEBUG": "maybe",
	}
	_, err := NewLoader(testSpec()).
		AddLayer(NewEnv("APP_", testSpec(), fakeLookup(env))).
		Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("期望 *LoadError，得到 %v", err)
	}
	count := 0
	for _, fe := range le.Errs {
		if fe.Code == ErrTypeMismatch {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("期望 2 个 type_mismatch，实际 %d: %v", count, le.Errs)
	}
	t.Logf("错误集合: %v", le.Errs)
}

func TestLoad_InvalidValue(t *testing.T) {
	logCase(t, "TestLoad_InvalidValue", "server.port=70000",
		"类型解析成功但超出 Validator 范围必须报 ErrInvalidValue")

	_, err := NewLoader(testSpec()).
		AddLayer(NewStatic("flags", map[string]string{
			"app.name":    "app",
			"db.password": "x",
			"server.port": "70000",
		})).
		Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("期望 *LoadError，得到 %v", err)
	}
	if _, ok := le.ByCode(ErrInvalidValue); !ok {
		t.Fatalf("期望 invalid_value，得到 %v", le.Codes())
	}
}

func TestLoad_UnknownFieldRejected(t *testing.T) {
	logCase(t, "TestLoad_UnknownFieldRejected", "静态层包含未声明字段 typo.port",
		"未知字段必须报 ErrUnknownField，绝不静默忽略")

	_, err := NewLoader(testSpec()).
		AddLayer(NewStatic("flags", map[string]string{
			"app.name":    "app",
			"db.password": "x",
			"typo.port":   "1",
		})).
		Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("期望 *LoadError，得到 %v", err)
	}
	fe, ok := le.ByCode(ErrUnknownField)
	if !ok || fe.Key != "typo.port" {
		t.Fatalf("期望 typo.port 的 unknown_field，得到 %+v", le.Errs)
	}
}

func TestLoad_OptionalFileMissing(t *testing.T) {
	logCase(t, "TestLoad_OptionalFileMissing", "Optional=true 且文件不存在",
		"可选文件缺失等价空层，加载继续；必填仍由其他层满足")

	_, err := NewLoader(testSpec()).
		AddLayer(&File{Path: filepath.Join(t.TempDir(), "nope.json"), Optional: true}).
		AddLayer(NewStatic("flags", map[string]string{"app.name": "a", "db.password": "b"})).
		Load()
	if err != nil {
		t.Fatalf("可选文件缺失不应导致失败，得到 %v", err)
	}
}

func TestLoad_RequiredFileMissingIsSourceFailure(t *testing.T) {
	logCase(t, "TestLoad_RequiredFileMissing", "Optional=false 且文件不存在",
		"必需文件读不了必须报 ErrSourceFailure")

	_, err := NewLoader(testSpec()).
		AddLayer(&File{Path: filepath.Join(t.TempDir(), "nope.json")}).
		Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("期望 *LoadError，得到 %v", err)
	}
	if fe, ok := le.ByCode(ErrSourceFailure); !ok {
		t.Fatalf("期望 source_failure，得到 %v", le.Codes())
	} else if !strings.Contains(fe.Layer, "nope.json") {
		t.Fatalf("来源层名应包含路径，得到 %q", fe.Layer)
	}
}

func TestSecret_NeverAppearsInOutputsOrErrors(t *testing.T) {
	const secret = "SUPER-SECRET-VALUE"
	logCase(t, "TestSecret_NeverAppearsInOutputsOrErrors", "db.password="+secret,
		"Value.String/Config.String/Redacted/任何 FieldError 都不得包含密钥原文")

	cfg, err := NewLoader(testSpec()).
		AddLayer(NewStatic("flags", map[string]string{
			"app.name": "app", "db.password": secret, "server.port": "1",
		})).
		Load()
	if err != nil {
		t.Fatal(err)
	}
	v, _ := cfg.Get("db.password")
	if v.String() != "***" {
		t.Errorf("密钥 String() 必须为 ***, 得到 %q", v.String())
	}
	if v.ExposeString() != secret {
		t.Errorf("ExposeString 应显式返回原文，得到 %q", v.ExposeString())
	}
	for key, rendered := range cfg.Redacted() {
		if strings.Contains(rendered, secret) {
			t.Errorf("Redacted[%s] 泄露密钥: %q", key, rendered)
		}
	}
	if strings.Contains(cfg.String(), secret) {
		t.Errorf("Config.String 泄露密钥: %s", cfg.String())
	}

	// 非法取值触发类型/校验错误路径：密钥同样不得出现在错误里。
	// 这里用"Validator 错误意外拼接原文"的场景验证框架兜底抹除。
	spec := NewSpec().
		Add(FieldSpec{Key: "token", Kind: KindString, Required: true, Secret: true,
			Validate: func(v Value) error {
				// 故意模拟一个不小心把值拼进错误信息的 Validator。
				return errors.New("bad token prefix: " + v.ExposeString())
			}})
	_, err = NewLoader(spec).AddLayer(NewStatic("flags", map[string]string{"token": secret})).Load()
	if err == nil {
		t.Fatal("期望校验失败")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("错误信息泄露密钥原文: %v", err)
	}
	if !strings.Contains(err.Error(), "***") {
		t.Fatalf("密钥应被替换为占位符: %v", err)
	}
	t.Logf("脱敏后的错误信息: %v", err)
}

func TestEnvName_DeterministicMapping(t *testing.T) {
	cases := map[string]string{
		"app.name":    "APP_APP_NAME",
		"server-port": "APP_SERVER_PORT",
		"x1.y2":       "APP_X1_Y2",
	}
	for in, want := range cases {
		logCase(t, "TestEnvName", "prefix=APP_ key="+in, "非字母数字->下划线并整体大写，期望 "+want)
		if got := EnvName("APP_", in); got != want {
			t.Errorf("EnvName(APP_,%q)=%q, want %q", in, got, want)
		}
	}
}

func TestLoad_CollectsMultipleErrors(t *testing.T) {
	logCase(t, "TestLoad_CollectsMultipleErrors", "缺失必填 + 类型错误 + 未知字段",
		"一次加载必须返回全部问题而非短路，且 Codes 可区分")

	_, err := NewLoader(testSpec()).
		AddLayer(NewStatic("flags", map[string]string{"server.port": "abc", "weird": "1"})).
		Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("期望 *LoadError，得到 %v", err)
	}
	wantCodes := map[ErrorCode]bool{
		ErrMissingRequired: true,
		ErrTypeMismatch:    true,
		ErrUnknownField:    true,
	}
	for _, c := range le.Codes() {
		delete(wantCodes, c)
	}
	if len(wantCodes) != 0 {
		t.Fatalf("缺少错误码 %v，实际 %v", wantCodes, le.Codes())
	}
	t.Logf("全部错误: %s", le.Error())
}

func TestLoad_BadOverrideDoesNotFallBackToDefault(t *testing.T) {
	logCase(t, "TestLoad_BadOverrideDoesNotFallBackToDefault",
		"server.port 默认 8080（合法），高优先级层给 abc（非法 int）",
		"必须只报一个 type_mismatch，不得悄悄回退默认值，也不得叠加 validate 错误")

	_, err := NewLoader(testSpec()).
		AddLayer(NewStatic("flags", map[string]string{
			"app.name":    "app",
			"db.password": "x",
			"server.port": "abc",
		})).
		Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("期望 *LoadError，得到 %v", err)
	}
	var portErrs []FieldError
	for _, fe := range le.Errs {
		if fe.Key == "server.port" {
			portErrs = append(portErrs, fe)
		}
	}
	if len(portErrs) != 1 || portErrs[0].Code != ErrTypeMismatch {
		t.Fatalf("server.port 应只有 1 个 type_mismatch，得到 %+v", portErrs)
	}
}

func fakeLookup(env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}
}
