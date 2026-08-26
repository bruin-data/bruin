package pipeline_test

import (
	"encoding/json"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariables(t *testing.T) {
	t.Parallel()

	t.Run("Should return an error if the variables are not valid JSONSchema object", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"user": {
				"type":    "complex",
				"default": "alice",
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variables schema")
		assert.NotContains(t, err.Error(), "\n", "schema errors must stay on a single line to render as one lint issue")
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
	t.Run("Should report the missing default for an empty variable body", func(t *testing.T) {
		t.Parallel()
		// A bare `foo:` in pipeline.yml decodes to a nil body. The missing-default
		// check has to run before schema compilation, otherwise this surfaces as an
		// opaque meta-schema type error instead.
		vars := pipeline.Variables{"user": nil}
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
	t.Run("Should blame the default, not the schema, when a default is not JSON-encodable", func(t *testing.T) {
		t.Parallel()
		// YAML decodes non-string mapping keys into map[any]any, which json.Marshal
		// rejects. Defaults stay out of the compiled schema document so this reads as
		// a problem with the default rather than with the variable's schema.
		vars := pipeline.Variables{
			"budgets": {
				"type":    "object",
				"default": map[any]any{2024: 100},
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variable defaults")
		assert.Contains(t, err.Error(), "object keys must be strings")
		assert.NotContains(t, err.Error(), "invalid variables schema")
	})
	t.Run("Should return an error if a default does not satisfy its enum", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"seat_denominator": {
				"type":    "string",
				"enum":    []any{"reachable", "contracted"},
				"default": "Contracted",
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variable defaults")
		assert.Contains(t, err.Error(), "seat_denominator")
	})
	t.Run("Should return an error if a default is outside numeric bounds", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"forecast_horizon_days": {
				"type":    "integer",
				"minimum": 7,
				"maximum": 90,
				"default": 0,
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variable defaults")
		assert.Contains(t, err.Error(), "forecast_horizon_days")
	})
	t.Run("Should validate nested defaults", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"users": {
				"type": "array",
				"items": map[string]any{
					"type":     "object",
					"required": []any{"name"},
					"properties": map[string]any{
						"name": map[string]any{"type": "string"},
					},
				},
				"default": []any{map[string]any{"age": 42}},
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "users.0")
		assert.Contains(t, err.Error(), "name")
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

	t.Run("rejects an override that does not satisfy its enum", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"seat_denominator": {
				"type":    "string",
				"enum":    []any{"reachable", "contracted"},
				"default": "reachable",
			},
		}

		err := vars.Merge(map[string]any{"seat_denominator": "Contracted"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema validation failed")
		assert.Contains(t, err.Error(), "seat_denominator")
		assert.Equal(t, "reachable", vars["seat_denominator"]["default"])
	})

	t.Run("rejects overrides outside numeric bounds", func(t *testing.T) {
		t.Parallel()

		for _, value := range []int{0, 9999} {
			vars := pipeline.Variables{
				"forecast_horizon_days": {
					"type":    "integer",
					"minimum": 7,
					"maximum": 90,
					"default": 30,
				},
			}

			err := vars.Merge(map[string]any{"forecast_horizon_days": value})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "forecast_horizon_days")
			assert.Equal(t, 30, vars["forecast_horizon_days"]["default"])
		}
	})

	t.Run("overriding a variable declared with an empty body does not panic", func(t *testing.T) {
		t.Parallel()

		// A bare `foo:` in pipeline.yml decodes to a nil map, which cannot be
		// assigned to.
		vars := pipeline.Variables{"foo": nil}

		err := vars.Merge(map[string]any{"foo": 1})
		require.NoError(t, err)
		assert.Equal(t, 1, vars["foo"]["default"])
	})

	t.Run("does not apply any overrides when one is invalid", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"environment": {
				"type":    "string",
				"default": "dev",
			},
			"forecast_horizon_days": {
				"type":    "integer",
				"minimum": 7,
				"default": 30,
			},
		}

		err := vars.Merge(map[string]any{
			"environment":           "prod",
			"forecast_horizon_days": 0,
		})
		require.Error(t, err)
		assert.Equal(t, "dev", vars["environment"]["default"])
		assert.Equal(t, 30, vars["forecast_horizon_days"]["default"])
	})
}
