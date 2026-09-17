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
	t.Run("unknown type values stay permitted so in-progress templates still parse", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"taxi_types": map[string]any{
				"type":    "TODO",
				"default": "TODO",
			},
		}
		require.NoError(t, vars.Validate())
	})
}

func TestVariables_ValidateConstraints(t *testing.T) {
	t.Parallel()

	t.Run("rejects a default that is not in enum", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"seat_denominator": map[string]any{
				"type":    "string",
				"enum":    []any{"reachable", "contracted"},
				"default": "Contracted",
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), `invalid variable "seat_denominator"`)
		assert.Contains(t, err.Error(), "not one of the allowed values")
	})

	t.Run("accepts a default that is in enum", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"seat_denominator": map[string]any{
				"type":    "string",
				"enum":    []any{"reachable", "contracted"},
				"default": "reachable",
			},
		}
		require.NoError(t, vars.Validate())
	})

	t.Run("rejects a default below minimum", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"forecast_horizon_days": map[string]any{
				"type":    "integer",
				"minimum": 7,
				"maximum": 90,
				"default": 0,
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "below minimum")
	})

	t.Run("rejects a default above maximum", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"forecast_horizon_days": map[string]any{
				"type":    "integer",
				"minimum": 7,
				"maximum": 90,
				"default": 9999,
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "above maximum")
	})

	t.Run("accepts a default on the inclusive bounds", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"forecast_horizon_days": map[string]any{
				"type":    "integer",
				"minimum": 7,
				"maximum": 90,
				"default": 7,
			},
		}
		require.NoError(t, vars.Validate())
	})

	t.Run("rejects a default that does not match const", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"env": map[string]any{
				"type":    "string",
				"const":   "prod",
				"default": "staging",
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match const")
	})

	t.Run("rejects a default with the wrong type", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"forecast_horizon_days": map[string]any{
				"type":    "integer",
				"default": "30",
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "type mismatch")
		assert.Contains(t, err.Error(), "expected integer")
	})

	t.Run("treats whole-number floats as integers for JSON-decoded defaults", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"forecast_horizon_days": map[string]any{
				"type":    "integer",
				"minimum": 7.0,
				"maximum": 90.0,
				"default": 30.0,
			},
		}
		require.NoError(t, vars.Validate())
	})

	t.Run("treats integer and whole-number float enum members as equal", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"tier": map[string]any{
				"type":    "integer",
				"enum":    []any{1, 2, 3},
				"default": float64(2),
			},
		}
		require.NoError(t, vars.Validate())
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

	t.Run("rejects an enum override that differs only by case", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"seat_denominator": map[string]any{
				"type":    "string",
				"enum":    []any{"reachable", "contracted"},
				"default": "reachable",
			},
		}

		err := vars.Merge(map[string]any{"seat_denominator": "Contracted"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `invalid variable "seat_denominator"`)
		assert.Contains(t, err.Error(), "not one of the allowed values")
		assert.Equal(t, "reachable", vars["seat_denominator"]["default"], "invalid override must not replace the default")
	})

	t.Run("rejects an override outside numeric bounds", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"forecast_horizon_days": map[string]any{
				"type":    "integer",
				"minimum": 7,
				"maximum": 90,
				"default": 30,
			},
		}

		err := vars.Merge(map[string]any{"forecast_horizon_days": 0})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "below minimum")
		assert.Equal(t, 30, vars["forecast_horizon_days"]["default"])

		err = vars.Merge(map[string]any{"forecast_horizon_days": 9999})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "above maximum")
		assert.Equal(t, 30, vars["forecast_horizon_days"]["default"])
	})

	t.Run("does not apply any override when one value is invalid", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"env": map[string]any{
				"type":    "string",
				"enum":    []any{"dev", "prod"},
				"default": "dev",
			},
			"count": map[string]any{
				"type":    "integer",
				"minimum": 1,
				"default": 1,
			},
		}

		err := vars.Merge(map[string]any{
			"env":   "staging",
			"count": 5,
		})
		require.Error(t, err)
		assert.Equal(t, "dev", vars["env"]["default"])
		assert.Equal(t, 1, vars["count"]["default"])
	})

	t.Run("accepts a valid enum and integer override", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"seat_denominator": map[string]any{
				"type":    "string",
				"enum":    []any{"reachable", "contracted"},
				"default": "reachable",
			},
			"forecast_horizon_days": map[string]any{
				"type":    "integer",
				"minimum": 7,
				"maximum": 90,
				"default": 30,
			},
		}

		err := vars.Merge(map[string]any{
			"seat_denominator":      "contracted",
			"forecast_horizon_days": int64(14),
		})
		require.NoError(t, err)
		assert.Equal(t, "contracted", vars["seat_denominator"]["default"])
		assert.Equal(t, int64(14), vars["forecast_horizon_days"]["default"])
	})

	t.Run("does not panic when merging into an empty-bodied variable", func(t *testing.T) {
		t.Parallel()

		vars := pipeline.Variables{
			"foo": nil,
		}

		err := vars.Merge(map[string]any{"foo": "bar"})
		require.NoError(t, err)
		assert.Equal(t, "bar", vars["foo"]["default"])
	})
}

func TestVariables_ValidateYAMLPipeline(t *testing.T) {
	t.Parallel()

	t.Run("enforces enum and bounds from pipeline.yml", func(t *testing.T) {
		t.Parallel()

		var parsed struct {
			Variables pipeline.Variables `yaml:"variables"`
		}
		err := yaml.Unmarshal([]byte(`
variables:
  seat_denominator:
    type: string
    enum: [reachable, contracted]
    default: reachable
  forecast_horizon_days:
    type: integer
    minimum: 7
    maximum: 90
    default: 30
`), &parsed)
		require.NoError(t, err)
		require.NoError(t, parsed.Variables.Validate())

		err = parsed.Variables.Merge(map[string]any{"seat_denominator": "Contracted"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not one of the allowed values")
	})
}
