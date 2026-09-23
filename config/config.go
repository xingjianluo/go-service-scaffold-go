// Package config implements layered configuration merging and validation.
//
// Merge precedence (lowest to highest):
//
//	defaults (Schema.Default) < config file (JSON) < environment variables
//
// Every winning value records its Source so the final decision is fully
// explainable. Secret fields are never rendered by Describe, LoadError or
// any error message.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// FieldType enumerates supported configuration value types.
type FieldType int

const (
	String FieldType = iota
	Int
	Bool
)

func (t FieldType) String() string {
	switch t {
	case String:
		return "string"
	case Int:
		return "int"
	case Bool:
		return "bool"
	default:
		return "unknown"
	}
}

// FieldSpec describes one configuration field.
type FieldSpec struct {
	// Key is the dotted configuration key, e.g. "server.port".
	Key string
	// Type is the expected value type.
	Type FieldType
	// Required rejects the load when the field is absent or empty.
	Required bool
	// Secret hides the value from Describe and all error messages.
	Secret bool
	// Default is applied when neither the file nor the environment sets the key.
	Default interface{}
	// Validator, if non-nil, runs after type coercion. Its failure is reported
	// as FailureInvalidValue. It MUST NOT include the raw value in its error
	// text when the field is secret.
	Validator func(value interface{}) error
}

// Schema is the ordered set of field specifications.
type Schema []FieldSpec

// Config holds merged configuration values.
type Config struct {
	values  map[string]interface{}
	sources map[string]Source
}

// Get returns the raw value and whether the key is present.
func (c *Config) Get(key string) (interface{}, bool) {
	v, ok := c.values[key]
	return v, ok
}

// GetString returns the string value or "" when absent/wrong-typed.
func (c *Config) GetString(key string) string {
	if v, ok := c.values[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// GetInt returns the int value or 0 when absent/wrong-typed.
func (c *Config) GetInt(key string) int {
	if v, ok := c.values[key]; ok {
		if n, ok := v.(int); ok {
			return n
		}
	}
	return 0
}

// GetBool returns the bool value or false when absent/wrong-typed.
func (c *Config) GetBool(key string) bool {
	if v, ok := c.values[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// Keys returns all present keys in sorted order.
func (c *Config) Keys() []string {
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// FailureKind classifies configuration errors.
type FailureKind int

const (
	FailureUnknown FailureKind = iota
	FailureMissingRequired
	FailureTypeMismatch
	FailureInvalidValue
)

func (k FailureKind) String() string {
	switch k {
	case FailureMissingRequired:
		return "missing_required"
	case FailureTypeMismatch:
		return "type_mismatch"
	case FailureInvalidValue:
		return "invalid_value"
	default:
		return "unknown"
	}
}

// FieldError is a per-field validation failure. The offending value is never
// included, in particular for secret fields.
type FieldError struct {
	Field  string
	Kind   FailureKind
	Msg    string
	Source Source
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("config field %q: %s: %s (source=%s)",
		e.Field, e.Kind, e.Msg, e.Source)
}

// LoadError aggregates configuration failures and is the error returned by
// Load whenever one or more fields are rejected.
type LoadError struct {
	FieldErrors []*FieldError
}

func (e *LoadError) Error() string {
	parts := make([]string, 0, len(e.FieldErrors))
	for _, fe := range e.FieldErrors {
		parts = append(parts, fe.Error())
	}
	return fmt.Sprintf("configuration rejected (%d problem(s)): %s",
		len(e.FieldErrors), strings.Join(parts, "; "))
}

// HasKind reports whether any aggregated failure has the given kind.
func (e *LoadError) HasKind(kind FailureKind) bool {
	for _, fe := range e.FieldErrors {
		if fe.Kind == kind {
			return true
		}
	}
	return false
}

// ForField returns failures associated with the given field key.
func (e *LoadError) ForField(key string) []*FieldError {
	var out []*FieldError
	for _, fe := range e.FieldErrors {
		if fe.Field == key {
			out = append(out, fe)
		}
	}
	return out
}

// Source records where a value came from.
type Source string

const (
	SourceDefault Source = "default"
	SourceFile    Source = "file"
	SourceEnv     Source = "env"
)

// Provenance explains the winning source of every field.
type Provenance map[string]Source

// Loader merges defaults, file and environment layers.
type Loader struct {
	schema    Schema
	filePath  string
	envPrefix string
	specs     map[string]FieldSpec
}

// NewLoader builds a Loader. filePath may be "" to skip the file layer;
// envPrefix may be "" to read unprefixed variables.
func NewLoader(schema Schema, filePath, envPrefix string) *Loader {
	l := &Loader{
		schema:    schema,
		filePath:  filePath,
		envPrefix: envPrefix,
		specs:     make(map[string]FieldSpec, len(schema)),
	}
	for _, s := range schema {
		l.specs[s.Key] = s
	}
	return l
}

// envName maps a dotted key to its environment variable name:
// prefix + "_" + upper(key with '-' and '.' replaced by '_').
func envName(prefix, key string) string {
	n := strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(key))
	if prefix == "" {
		return n
	}
	return strings.ToUpper(prefix) + "_" + n
}

// coerce converts a layer's raw value to the declared Go type.
// The source label only annotates error messages; values are never quoted.
func coerce(spec FieldSpec, raw interface{}, src Source) (interface{}, *FieldError) {
	switch spec.Type {
	case String:
		if v, ok := raw.(string); ok {
			return v, nil
		}
	case Int:
		switch v := raw.(type) {
		case int:
			return v, nil
		case json.Number:
			n, err := strconv.Atoi(v.String())
			if err == nil {
				return n, nil
			}
		case float64: // tolerated for non-Decoder JSON inputs
			if v == float64(int64(v)) {
				return int(v), nil
			}
		case string: // environment layer
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err == nil {
				return n, nil
			}
		}
	case Bool:
		switch v := raw.(type) {
		case bool:
			return v, nil
		case string: // environment layer
			b, err := strconv.ParseBool(strings.TrimSpace(v))
			if err == nil {
				return b, nil
			}
		}
	}
	return nil, &FieldError{
		Field:  spec.Key,
		Kind:   FailureTypeMismatch,
		Msg:    fmt.Sprintf("expected %s value", spec.Type),
		Source: src,
	}
}

// isEmpty reports whether a present value counts as empty for Required checks.
func isEmpty(v interface{}) bool {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t) == ""
	case nil:
		return true
	default:
		return false
	}
}

// Load performs the layered merge and validates the result.
// Malformed JSON is reported as a plain error (use errors.As against
// *json.SyntaxError to distinguish it); field-level problems are aggregated
// into *LoadError with distinguishable FailureKind values.
func (l *Loader) Load() (*Config, error) {
	values := make(map[string]interface{})
	sources := make(map[string]Source)
	var fieldErrs []*FieldError

	put := func(spec FieldSpec, raw interface{}, src Source) {
		v, ferr := coerce(spec, raw, src)
		if ferr != nil {
			fieldErrs = append(fieldErrs, ferr)
			return
		}
		values[spec.Key] = v
		sources[spec.Key] = src
	}

	// Layer 1: defaults (schema order preserved for stable precedence).
	for _, spec := range l.schema {
		if spec.Default == nil {
			continue
		}
		put(spec, spec.Default, SourceDefault)
	}

	// Layer 2: JSON file.
	if l.filePath != "" {
		raw, err := os.ReadFile(l.filePath)
		switch {
		case err == nil:
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			var fileVals map[string]interface{}
			if perr := dec.Decode(&fileVals); perr != nil {
				return nil, fmt.Errorf("parse config file %q: %w", l.filePath, perr)
			}
			for k, rv := range fileVals {
				spec, known := l.specs[k]
				if !known {
					fieldErrs = append(fieldErrs, &FieldError{
						Field:  k,
						Kind:   FailureInvalidValue,
						Msg:    "unknown configuration field not present in schema",
						Source: SourceFile,
					})
					continue
				}
				put(spec, rv, SourceFile)
			}
		case os.IsNotExist(err):
			// optional file: silently skip
		default:
			return nil, fmt.Errorf("read config file %q: %w", l.filePath, err)
		}
	}

	// Layer 3: environment variables (highest precedence).
	for _, spec := range l.schema {
		name := envName(l.envPrefix, spec.Key)
		raw, ok := os.LookupEnv(name)
		if !ok {
			continue
		}
		put(spec, raw, SourceEnv)
	}

	// Validation pass: required + custom validators.
	for _, spec := range l.schema {
		v, present := values[spec.Key]
		if !present || isEmpty(v) {
			if spec.Required {
				fieldErrs = append(fieldErrs, &FieldError{
					Field:  spec.Key,
					Kind:   FailureMissingRequired,
					Msg:    "required field is not set by file, environment or default",
					Source: sources[spec.Key],
				})
			}
			continue
		}
		if spec.Validator != nil {
			if verr := spec.Validator(v); verr != nil {
				fieldErrs = append(fieldErrs, &FieldError{
					Field:  spec.Key,
					Kind:   FailureInvalidValue,
					Msg:    safeValidatorMsg(spec, verr),
					Source: sources[spec.Key],
				})
			}
		}
	}

	if len(fieldErrs) > 0 {
		sort.Slice(fieldErrs, func(i, j int) bool {
			if fieldErrs[i].Field == fieldErrs[j].Field {
				return fieldErrs[i].Kind < fieldErrs[j].Kind
			}
			return fieldErrs[i].Field < fieldErrs[j].Field
		})
		return nil, &LoadError{FieldErrors: fieldErrs}
	}

	return &Config{values: values, sources: sources}, nil
}

// safeValidatorMsg drops validator text for secret fields defensively,
// in case a validator mistakenly embeds the secret value.
func safeValidatorMsg(spec FieldSpec, err error) string {
	if spec.Secret {
		return "value rejected by validator (value hidden as field is secret)"
	}
	return err.Error()
}

// Explain returns the winning source of every present field.
func (l *Loader) Explain(cfg *Config) Provenance {
	out := make(Provenance, len(cfg.sources))
	for k, s := range cfg.sources {
		out[k] = s
	}
	return out
}

// Describe returns a secret-safe, deterministic, human-readable description:
//
//	server.port=8080 (file)
//	db.password=*** (env)
func (c *Config) Describe(schema Schema) string {
	lines := make([]string, 0, len(schema))
	for _, spec := range schema {
		v, present := c.values[spec.Key]
		switch {
		case !present || isEmpty(v):
			lines = append(lines, fmt.Sprintf("%s=<unset>", spec.Key))
		case spec.Secret:
			lines = append(lines, fmt.Sprintf("%s=*** (%s)", spec.Key, c.sources[spec.Key]))
		default:
			lines = append(lines, fmt.Sprintf("%s=%v (%s)", spec.Key, v, c.sources[spec.Key]))
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}
