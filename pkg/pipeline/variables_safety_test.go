package pipeline_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariables_SchemaDiagnosticsSkipsExternalReferences(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"type":"integer"}`))
	}))
	t.Cleanup(server.Close)

	// Exercise every schema-bearing keyword, including Go containers that are
	// not map[string]any/[]any until they are converted to JSON.
	reference := map[string]string{"$ref": server.URL + "/schema"}
	cases := map[string]map[string]any{
		"root":       {"$ref": server.URL},
		"file":       {"$ref": "file:///bruin-warning-validation-must-not-open-this.json"},
		"items":      {"items": reference},
		"tuple":      {"items": []map[string]string{reference}},
		"properties": {"properties": map[string]map[string]string{"x": reference}},
		"nested":     {"properties": map[string]any{"x": map[string]any{"items": reference}}},
	}
	for _, keyword := range []string{"patternProperties", "definitions", "$defs", "dependencies"} {
		cases[keyword] = map[string]any{keyword: map[string]any{"x": reference}}
	}
	for _, keyword := range []string{"allOf", "anyOf", "oneOf"} {
		cases[keyword] = map[string]any{keyword: []map[string]string{reference}}
	}
	for _, keyword := range []string{"additionalItems", "additionalProperties", "contains", "else", "if", "not", "propertyNames", "then"} {
		cases[keyword] = map[string]any{keyword: reference}
	}
	// Subtests run in parallel after this function returns; cleanups run after them.
	t.Cleanup(func() { assert.Zero(t, requests.Load(), "warning validation must not fetch referenced schemas") })
	for name, definition := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			definition["default"] = 1
			vars := pipeline.Variables{"value": definition}
			assert.Equal(t, []string{
				"variables.value: schema and value validation skipped because $ref is not supported by warning validation",
			}, vars.SchemaDiagnostics())
		})
	}
}

func TestVariables_SchemaDiagnosticsReferenceCycles(t *testing.T) { //nolint:paralleltest // Re-executes the test binary with a reduced stack limit.
	// Isolate this regression: a stack overflow in gojsonschema cannot be
	// recovered and would otherwise kill the entire package's test process.
	const childEnv = "BRUIN_SCHEMA_CYCLE_TEST_CHILD"
	if os.Getenv(childEnv) == "1" {
		debug.SetMaxStack(128 << 10)
		for _, reference := range []string{"#", "#/properties/value"} {
			vars := pipeline.Variables{"value": {"$ref": reference, "default": 1}}
			diagnostics := vars.SchemaDiagnostics()
			require.Len(t, diagnostics, 1)
			assert.Contains(t, diagnostics[0], "validation skipped because $ref")
		}
		vars := pipeline.Variables{
			"first":  {"$ref": "#/properties/second", "default": 1},
			"second": {"$ref": "#/properties/first", "default": 1},
		}
		require.Len(t, vars.SchemaDiagnostics(), 2)
		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestVariables_SchemaDiagnosticsReferenceCycles$")
	command.Env = append(os.Environ(), childEnv+"=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "warning validation must not hang or crash: %s", output[:min(1000, len(output))])
}

func TestVariables_SchemaDiagnosticsAnnotationsCannotReplaceMetaSchema(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)

	t.Cleanup(func() {
		assert.Zero(t, requests.Load(), "annotations must not replace the bundled meta-schema or trigger reference loading")
	})
	for _, keyword := range []string{"examples", "x-custom"} {
		for _, id := range []string{"$id", "id"} {
			t.Run(keyword+"/"+id, func(t *testing.T) {
				t.Parallel()
				vars := pipeline.Variables{"value": {
					"type": "integer", "default": 1,
					keyword: []any{map[string]any{
						id: "http://json-schema.org/draft-07/schema#", "$ref": server.URL,
					}},
				}}
				assert.Empty(t, vars.SchemaDiagnostics())
				vars["value"]["type"] = "invalid"
				diagnostics := vars.SchemaDiagnostics()
				require.Len(t, diagnostics, 1)
				assert.Contains(t, diagnostics[0], "invalid schema")
			})
		}
	}
}

func TestVariables_SchemaDiagnosticsChecksUnreferencedVariables(t *testing.T) {
	t.Parallel()
	vars := pipeline.Variables{
		"referenced": {"$ref": "#/properties/referenced", "default": make(chan int)},
		"count":      {"type": "integer", "default": "not an integer"},
	}
	diagnostics := vars.SchemaDiagnostics()
	require.Len(t, diagnostics, 2)
	assert.Equal(t, "variables.count: value does not satisfy its schema: Invalid type. Expected: integer, given: string", diagnostics[0])
	assert.Contains(t, diagnostics[1], "variables.referenced: schema and value validation skipped")
}

func TestVariables_SchemaDiagnosticsPreservesDataAndRuntimeSchemas(t *testing.T) {
	t.Parallel()
	data := map[string]any{"type": "int", "$ref": "literal", "nested": map[string]any{"type": nil}}
	vars := pipeline.Variables{
		"config": {
			"type": "object",
			"properties": map[string]any{
				"type": map[string]any{"type": "string"},
				"$ref": map[string]any{"type": "string"},
				"nested": map[string]any{
					"type":       "object",
					"properties": map[string]any{"type": map[string]any{"type": nil}},
				},
			},
			"default": data,
			"const":   data,
			"enum":    []any{data},
		},
		"count": {"type": []string{"int", "null"}, "default": 1},
	}
	before, err := json.Marshal(vars)
	require.NoError(t, err)
	schemaBefore, err := json.Marshal(vars.SchemaMap())
	require.NoError(t, err)
	assert.Equal(t, []string{
		`variables.config.properties.nested.properties.type.type: YAML null is treated as JSON Schema type "null"`,
		`variables.count.type[0]: legacy type "int" is treated as "integer"`,
	}, vars.SchemaDiagnostics())
	after, err := json.Marshal(vars)
	require.NoError(t, err)
	schemaAfter, err := json.Marshal(vars.SchemaMap())
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Equal(t, schemaBefore, schemaAfter)
	assert.Equal(t, map[string]any{
		"$schema": "https://json-schema.org/draft-07/schema", "type": "object", "properties": vars,
	}, vars.Schema(), "the public Schema API must continue to preserve defaults and declared types")
}

func TestVariables_SchemaDiagnosticsInvalidJSONIsAWarning(t *testing.T) {
	t.Parallel()
	for keyword, expected := range map[string]string{
		"items":   "variables.value: invalid schema: value is not JSON-encodable (chan int)",
		"default": "variables.value: failed to validate value: value is not JSON-encodable (chan int)",
	} {
		t.Run(keyword, func(t *testing.T) {
			t.Parallel()
			vars := pipeline.Variables{"value": {"default": 1, keyword: make(chan int)}}
			assert.Equal(t, []string{expected}, vars.SchemaDiagnostics())
		})
	}
}

func TestVariables_SchemaDiagnosticsChecksVariablesIndependently(t *testing.T) {
	t.Parallel()
	nonStringKeys := map[any]any{1: "x"}
	cases := map[string]struct {
		broken   map[string]any
		expected string
	}{
		"invalid schema": {
			broken:   map[string]any{"type": "str", "default": "x"},
			expected: "variables.a: invalid schema: type: ",
		},
		"non-JSON value": {
			broken:   map[string]any{"type": "object", "default": nonStringKeys},
			expected: "variables.a: failed to validate value: value is not JSON-encodable (map[interface {}]interface {}): object keys must be strings",
		},
		"non-JSON schema": {
			broken:   map[string]any{"type": "object", "enum": []any{nonStringKeys}, "default": map[string]any{}},
			expected: "variables.a: invalid schema: value is not JSON-encodable (map[interface {}]interface {}): object keys must be strings",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			vars := pipeline.Variables{
				"a": tc.broken,
				"b": {"type": "integer", "enum": []any{1, 2}, "default": 7},
			}
			diagnostics := vars.SchemaDiagnostics()
			require.Len(t, diagnostics, 2)
			assert.True(t, strings.HasPrefix(diagnostics[0], tc.expected), diagnostics[0])
			assert.Equal(t, "variables.b: value does not satisfy its schema: value must be one of the following: 1, 2", diagnostics[1])
		})
	}
}
