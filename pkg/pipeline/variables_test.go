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

	// TODO: restore this test when meta-schema validation is implemented
	// t.Run("Should return an error if the variables are not valid JSONSchema object", func(t *testing.T) {
	// 	t.Parallel()
	// 	vars := pipeline.Variables{
	// 		"user": {
	// 			"type": "complex",
	// 		},
	// 	}
	// 	err := vars.Validate()
	// 	require.Error(t, err)
	// 	assert.Contains(t, err.Error(), "invalid variables schema")
	// })
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

		assert.Len(t, vars, 0)
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
		assert.Equal(t, float64(42), vars["var2"]["default"]) // JSON numbers are float64
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
		assert.Len(t, vars, 0)
	})
}
