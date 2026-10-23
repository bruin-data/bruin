package pipeline_test

import (
	"encoding/json"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestVariables(t *testing.T) {
	t.Parallel()

	t.Run("runtime validation does not enforce schemas", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{"user": {"type": "complex", "default": "alice"}}
		require.NoError(t, vars.Validate())
		require.NoError(t, vars.Merge(map[string]any{"user": 42}))
		require.NoError(t, vars.Validate())
		assert.Equal(t, 42, vars.Value()["user"])
	})
	t.Run("Should return an error if the default is not set", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"user": map[string]any{
				"type": "string",
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must have a default value")
	})
	t.Run("Should return no error if schema is valid", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"user": map[string]any{
				"type":    "string",
				"default": "Jhon Doe",
			},
		}
		err := vars.Validate()
		require.NoError(t, err)
	})
	t.Run("Should use default values to construct the variables", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"user": map[string]any{
				"type":    "string",
				"default": "foo",
			},
			"age": map[string]any{
				"type":    "integer",
				"default": 42,
			},
			"active": map[string]any{
				"type":    "boolean",
				"default": true,
			},
		}
		err := vars.Validate()
		require.NoError(t, err)
		expect := map[string]any{
			"user":   "foo",
			"age":    42,
			"active": true,
		}
		assert.Equal(t, expect, vars.Value())
	})
	t.Run("Should handle nested variables", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"user": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{
						"type": "string",
					},
					"age": map[string]any{
						"type": "number",
					},
				},
				"default": map[string]any{
					"name": "foo",
					"age":  42,
				},
			},
			"active": map[string]any{
				"type":    "boolean",
				"default": true,
			},
		}
		err := vars.Validate()
		require.NoError(t, err)
		expect := map[string]any{
			"user": map[string]any{
				"name": "foo",
				"age":  42,
			},
			"active": true,
		}
		assert.Equal(t, expect, vars.Value())
	})
}

func TestVariables_SchemaMap(t *testing.T) {
	t.Parallel()

	t.Run("returns type definitions without defaults", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"env": map[string]any{
				"type":    "string",
				"default": "dev",
			},
			"count": map[string]any{
				"type":    "integer",
				"default": 42,
			},
		}
		schema := vars.SchemaMap()
		assert.Equal(t, map[string]any{"type": "string"}, schema["env"])
		assert.Equal(t, map[string]any{"type": "integer"}, schema["count"])
	})

	t.Run("preserves extra fields like properties", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"cfg": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"port": map[string]any{"type": "integer"},
				},
				"default": map[string]any{"port": 8080},
			},
		}
		schema := vars.SchemaMap()
		expected := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"port": map[string]any{"type": "integer"},
			},
		}
		assert.Equal(t, expected, schema["cfg"])
	})

	t.Run("empty variables return empty schema", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{}
		schema := vars.SchemaMap()
		assert.Empty(t, schema)
	})

	t.Run("preserves legacy integer types in runtime schemas", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"aggregate_days": {
				"type": "array",
				"items": map[string]any{
					"type": "int",
				},
				"default": []any{1, 2, 3},
			},
		}

		schema := vars.SchemaMap()
		items := schema["aggregate_days"].(map[string]any)["items"].(map[string]any)
		assert.Equal(t, "int", items["type"])
		assert.Equal(t, "int", vars["aggregate_days"]["items"].(map[string]any)["type"])
	})

	t.Run("preserves YAML null in runtime schemas", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"nullable_count": {
				"type":    []any{"integer", nil},
				"enum":    []any{1, nil},
				"default": nil,
			},
		}

		schema := vars.SchemaMap()["nullable_count"].(map[string]any)
		assert.Equal(t, []any{"integer", nil}, schema["type"])
		assert.Equal(t, []any{1, nil}, schema["enum"])
	})

	t.Run("distinguishes schema type keywords from object fields named type", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"config": {
				"type": "object",
				"properties": map[string]any{
					"type": map[string]any{"type": "int"},
				},
				"enum": []any{
					map[string]any{"type": "int"},
				},
				"default": map[string]any{"type": "int"},
			},
		}

		schema := vars.SchemaMap()["config"].(map[string]any)
		properties := schema["properties"].(map[string]any)
		assert.Equal(t, "int", properties["type"].(map[string]any)["type"])
		assert.Equal(t, "int", schema["enum"].([]any)[0].(map[string]any)["type"])
	})
}

func TestVariables_SchemaDiagnostics(t *testing.T) {
	t.Parallel()

	t.Run("warns for legacy int types but validates their defaults", func(t *testing.T) {
		t.Parallel()
		var p pipeline.Pipeline
		err := yaml.Unmarshal([]byte(`
name: ua_data
variables:
  aggregate_days:
    type: array
    items:
      type: int
    default: [1, 2, 3, 7]
  engagement_days:
    type: array
    items:
      type: int
    default: [0, 1, 2, 3, 7]
`), &p)
		require.NoError(t, err)

		diagnostics := p.Variables.SchemaDiagnostics()
		assert.Equal(t, []string{
			`variables.aggregate_days.items.type: legacy type "int" is treated as "integer"`,
			`variables.engagement_days.items.type: legacy type "int" is treated as "integer"`,
		}, diagnostics)
	})

	t.Run("warns when a schema is invalid", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"user": {
				"type":    "complex",
				"default": "alice",
			},
		}

		diagnostics := vars.SchemaDiagnostics()
		require.Len(t, diagnostics, 1)
		assert.Contains(t, diagnostics[0], "invalid schema")
		assert.NotContains(t, diagnostics[0], "\n")
	})

	t.Run("warns for YAML null types but keeps null defaults valid", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"optional_count": {
				"type":    []any{"integer", nil},
				"default": nil,
			},
		}

		assert.Equal(t, []string{
			`variables.optional_count.type[1]: YAML null is treated as JSON Schema type "null"`,
		}, vars.SchemaDiagnostics())
	})

	t.Run("warns when defaults do not satisfy a valid schema", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"environment": {
				"type":    "string",
				"enum":    []any{"dev", "prod"},
				"default": "staging",
			},
		}

		diagnostics := vars.SchemaDiagnostics()
		assert.Equal(t, []string{
			`variables.environment: value does not satisfy its schema: value must be one of the following: "dev", "prod"`,
		}, diagnostics)
	})

	t.Run("returns no diagnostics for valid schemas and defaults", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"count": {
				"type":    "integer",
				"minimum": 1,
				"default": 3,
			},
		}

		assert.Empty(t, vars.SchemaDiagnostics())
	})
}

func TestVariables_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("Should clear variables when empty object is provided", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"existing_var": map[string]any{
				"type":    "string",
				"default": "existing_value",
			},
		}

		assert.Len(t, vars, 1)
		assert.Contains(t, vars, "existing_var")

		// Unmarshal empty object
		err := json.Unmarshal([]byte(`{}`), &vars)
		require.NoError(t, err)

		assert.Empty(t, vars)
	})

	t.Run("Should replace all variables with new ones", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"old_var": map[string]any{
				"type":    "string",
				"default": "old_value",
			},
		}

		assert.Len(t, vars, 1)
		assert.Contains(t, vars, "old_var")

		newVarsJSON := `{"new_var": {"type": "string", "default": "new_value"}}`
		err := json.Unmarshal([]byte(newVarsJSON), &vars)
		require.NoError(t, err)

		assert.Len(t, vars, 1)
		assert.Contains(t, vars, "new_var")
		assert.NotContains(t, vars, "old_var")
		assert.Equal(t, "new_value", vars["new_var"]["default"])
	})

	t.Run("Should handle multiple variables", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{}

		multiVarsJSON := `{
			"var1": {"type": "string", "default": "value1"},
			"var2": {"type": "integer", "default": 42},
			"var3": {"type": "boolean", "default": true}
		}`
		err := json.Unmarshal([]byte(multiVarsJSON), &vars)
		require.NoError(t, err)

		assert.Len(t, vars, 3)
		assert.Contains(t, vars, "var1")
		assert.Contains(t, vars, "var2")
		assert.Contains(t, vars, "var3")
		assert.Equal(t, "value1", vars["var1"]["default"])
		assert.InEpsilon(t, float64(42), vars["var2"]["default"], 0.0001) // JSON numbers are float64
		assert.Equal(t, true, vars["var3"]["default"])
	})

	t.Run("Should handle null as empty object", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"existing_var": map[string]any{
				"type":    "string",
				"default": "existing_value",
			},
		}

		err := json.Unmarshal([]byte(`null`), &vars)
		require.NoError(t, err)

		// Should be empty now
		assert.Empty(t, vars)
	})
}

func TestVariables_Merge(t *testing.T) {
	t.Parallel()

	t.Run("overrides existing defaults", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"foo": map[string]any{
				"type":    "string",
				"default": "old",
			},
			"bar": map[string]any{
				"type":    "integer",
				"default": 1,
			},
		}

		err := vars.Merge(map[string]any{
			"foo": "new",
			"bar": 2,
		})
		require.NoError(t, err)

		assert.Equal(t, "new", vars["foo"]["default"])
		assert.Equal(t, 2, vars["bar"]["default"])
	})

	t.Run("returns error for unknown variables", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"foo": map[string]any{
				"type":    "string",
				"default": "old",
			},
		}

		err := vars.Merge(map[string]any{
			"missing": "value",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no such variable")
	})

	t.Run("preserves other fields", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"foo": map[string]any{
				"type":    "string",
				"default": "old",
			},
		}

		err := vars.Merge(map[string]any{
			"foo": "new",
		})
		require.NoError(t, err)

		assert.Equal(t, "string", vars["foo"]["type"])
		assert.Equal(t, "new", vars["foo"]["default"])
	})
}
