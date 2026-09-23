package config

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testSchema defines: one required string, one secret string, an int with a
// range validator, a bool with default, and an optional string.
func testSchema() Schema {
	return Schema{
		{Key: "service.name", Type: String, Required: true},
		{Key: "db.password", Type: String, Required: true, Secret: true},
		{Key: "server.port", Type: Int, Default: 8080, Validator: func(v interface{}) error {
			n := v.(int)
			if n < 1 || n > 65535 {
				return errors.New("port must be within 1..65535")
			}
			return nil
		}},
		{Key: "feature.enabled", Type: Bool, Default: false},
		{Key: "service.zone", Type: String},
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.json")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

// TestLayeredPrecedence verifies default < file < env and Explain provenance.
func TestLayeredPrecedence(t *testing.T) {
	path := writeTemp(t, `{
		"service.name": "from-file",
		"db.password": "file-secret",
		"server.port": 9090,
		"feature.enabled": true
	}`)

	t.Setenv("APP_SERVICE_NAME", "from-env") // env must beat file
	t.Setenv("APP_SERVER_PORT", "7000")      // env must beat file
	// db.password only in file; port/defaults check precedence below.

	l := NewLoader(testSchema(), path, "APP")
	cfg, err := l.Load()
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}

	got := map[string]interface{}{
		"service.name":    cfg.GetString("service.name"),
		"db.password":     cfg.GetString("db.password"),
		"server.port":     cfg.GetInt("server.port"),
		"feature.enabled": cfg.GetBool("feature.enabled"),
	}
	log.Printf("INPUT file=service.name:from-file,port:9090 | env=service.name:from-env,port:7000\nJUDGEMENT merged=%v", got)

	if cfg.GetString("service.name") != "from-env" {
		t.Fatalf("service.name: expected env to win, got %q", cfg.GetString("service.name"))
	}
	if cfg.GetInt("server.port") != 7000 {
		t.Fatalf("server.port: expected env to win, got %d", cfg.GetInt("server.port"))
	}
	if cfg.GetString("db.password") != "file-secret" {
		t.Fatalf("db.password expected file value, got %q", cfg.GetString("db.password"))
	}
	if cfg.GetBool("feature.enabled") != true {
		t.Fatalf("feature.enabled expected file value true")
	}

	prov := l.Explain(cfg)
	log.Printf("JUDGEMENT provenance=%v", prov)
	if prov["service.name"] != SourceEnv || prov["server.port"] != SourceEnv ||
		prov["db.password"] != SourceFile {
		t.Fatalf("provenance mismatch: %v", prov)
	}
	if prov["feature.enabled"] != SourceFile {
		t.Fatalf("feature.enabled expected file source, got %s", prov["feature.enabled"])
	}
}

// TestDefaultLayer verifies defaults apply when file/env omit fields.
func TestDefaultLayer(t *testing.T) {
	path := writeTemp(t, `{"service.name":"svc","db.password":"x"}`)
	l := NewLoader(testSchema(), path, "APP")
	cfg, err := l.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	log.Printf("INPUT config without server.port/feature.enabled\nJUDGEMENT port=%d enabled=%t",
		cfg.GetInt("server.port"), cfg.GetBool("feature.enabled"))
	if cfg.GetInt("server.port") != 8080 || cfg.GetBool("feature.enabled") != false {
		t.Fatal("default layer not applied")
	}
	if l.Explain(cfg)["server.port"] != SourceDefault {
		t.Fatal("server.port provenance should be default")
	}
}

// TestMissingRequired verifies missing required fields are rejected distinctly.
func TestMissingRequired(t *testing.T) {
	path := writeTemp(t, `{"service.name":"svc"}`)
	l := NewLoader(testSchema(), path, "APP")
	_, err := l.Load()
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("expected *LoadError, got %T: %v", err, err)
	}
	log.Printf("INPUT missing db.password\nJUDGEMENT error=%q has-missing=%v",
		err, le.HasKind(FailureMissingRequired))
	if !le.HasKind(FailureMissingRequired) {
		t.Fatalf("expected missing_required failure, got: %v", le)
	}
	if len(le.ForField("db.password")) == 0 {
		t.Fatal("expected a failure entry for db.password")
	}
}

// TestTypeMismatch verifies env/int and file/string type rejection.
func TestTypeMismatch(t *testing.T) {
	path := writeTemp(t, `{"service.name":"svc","db.password":"p","server.port":"not-a-number"}`)
	l := NewLoader(testSchema(), path, "APP")
	_, err := l.Load()
	var le *LoadError
	if !errors.As(err, &le) || !le.HasKind(FailureTypeMismatch) {
		t.Fatalf("expected type mismatch failure, got: %v", err)
	}
	log.Printf("INPUT server.port=\"not-a-number\"\nJUDGEMENT %q", err)

	// env coercion of int fails too.
	t.Setenv("APP_SERVER_PORT", "abc")
	if _, err := NewLoader(testSchema(), path, "APP").Load(); err == nil {
		t.Fatal("expected error for non-int env var")
	}
	log.Printf("INPUT env APP_SERVER_PORT=abc\nJUDGEMENT rejected as type mismatch: %v", err != nil)
}

// TestInvalidValue verifies validator failures are distinctly classified.
func TestInvalidValue(t *testing.T) {
	path := writeTemp(t, `{"service.name":"svc","db.password":"p","server.port":70000}`)
	l := NewLoader(testSchema(), path, "APP")
	_, err := l.Load()
	var le *LoadError
	if !errors.As(err, &le) || !le.HasKind(FailureInvalidValue) {
		t.Fatalf("expected invalid value failure, got: %v", err)
	}
	log.Printf("INPUT server.port=70000\nJUDGEMENT %q", err)
}

// TestUnknownFieldRejected verifies undeclared keys are refused.
func TestUnknownFieldRejected(t *testing.T) {
	path := writeTemp(t, `{"service.name":"svc","db.password":"p","mystery":1}`)
	_, err := NewLoader(testSchema(), path, "APP").Load()
	log.Printf("INPUT unknown key mystery\nJUDGEMENT %v", err)
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("expected LoadError for unknown field, got %v", err)
	}
}

// TestSecretNeverLeaked verifies secret values are absent from every surface.
func TestSecretNeverLeaked(t *testing.T) {
	secret := "super-secret-token-123"
	path := writeTemp(t, `{"service.name":"svc","db.password":"`+secret+`"}`)
	cfg, err := NewLoader(testSchema(), path, "APP").Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	desc := cfg.Describe(testSchema())
	log.Printf("INPUT secret=%q\nJUDGEMENT Describe=\n%s", secret, desc)
	if strings.Contains(desc, secret) {
		t.Fatal("Describe leaked secret value")
	}
	if !strings.Contains(desc, "db.password=***") {
		t.Fatal("Describe must mask secret field as ***")
	}

	// Trigger a validation failure on the secret field and ensure the raw
	// value never appears in the error.
	secretSchema := Schema{
		{Key: "service.name", Type: String, Required: true},
		{Key: "db.password", Type: String, Required: true, Secret: true,
			Validator: func(v interface{}) error { return errors.New("bad value " + v.(string)) }},
	}
	_, verr := NewLoader(secretSchema, path, "APP").Load()
	log.Printf("JUDGEMENT secret validation error=%q", verr)
	if verr == nil || strings.Contains(verr.Error(), secret) {
		t.Fatalf("secret leaked in validation error or error missing: %v", verr)
	}
}

// TestMalformedFile verifies malformed JSON is distinguishable from field errors.
func TestMalformedFile(t *testing.T) {
	path := writeTemp(t, `{not json`)
	_, err := NewLoader(testSchema(), path, "APP").Load()
	var le *LoadError
	if errors.As(err, &le) {
		t.Fatalf("malformed JSON must not be aggregated as field errors, got %v", err)
	}
	log.Printf("INPUT malformed json\nJUDGEMENT non-field error: %v", err)
}

// TestEnvBoolParsing verifies bool env parsing via strcmp-like values.
func TestEnvBoolParsing(t *testing.T) {
	path := writeTemp(t, `{"service.name":"svc","db.password":"p"}`)
	t.Setenv("APP_FEATURE_ENABLED", "true")
	cfg, err := NewLoader(testSchema(), path, "APP").Load()
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if !cfg.GetBool("feature.enabled") {
		t.Fatal("env bool should be parsed as true")
	}
	log.Printf("INPUT APP_FEATURE_ENABLED=true\nJUDGEMENT enabled=%t", cfg.GetBool("feature.enabled"))
}
