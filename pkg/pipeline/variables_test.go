package pipeline_test

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariables(t *testing.T) {
	t.Parallel()
	t.Run("Should return an error if the variables are not valid JSONSchema", func(t *testing.T) {
		t.Parallel()
		vars := pipeline.Variables{
			"user": "foo",
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
	t.Run("Should use default values to contruct the variables", func(t *testing.T) {
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
		assert.Equal(t, vars.Value(), map[string]any{
			"user":   "foo",
			"age":    42,
			"active": true,
		})
	})
}
