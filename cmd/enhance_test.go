package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestGetAssetConnectionName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		expected string
	}{
		{
			name: "returns connection field when set",
			asset: &pipeline.Asset{
				Connection: "my_database",
			},
			expected: "my_database",
		},
		{
			name: "returns connection from parameters when connection field is empty",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"connection": "param_database",
				},
			},
			expected: "param_database",
		},
		{
			name: "prefers connection field over parameters",
			asset: &pipeline.Asset{
				Connection: "my_database",
				Parameters: map[string]string{
					"connection": "param_database",
				},
			},
			expected: "my_database",
		},
		{
			name: "returns empty string when no connection is set",
			asset: &pipeline.Asset{
				Name: "test_asset",
			},
			expected: "",
		},
		{
			name: "returns empty string when parameters is nil",
			asset: &pipeline.Asset{
				Name:       "test_asset",
				Parameters: nil,
			},
			expected: "",
		},
		{
			name: "returns empty string when connection parameter is empty",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"connection": "",
				},
			},
			expected: "",
		},
		{
			name: "returns empty string when parameters has other keys but not connection",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"other_param": "value",
				},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := getAssetConnectionName(tt.asset)
			assert.Equal(t, tt.expected, result)
		})
	}
}
