package pipeline_test

import (
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
				"type": "complex",
			},
		}
		err := vars.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variables schema")
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
