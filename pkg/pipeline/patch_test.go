package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeline_Patch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		original    Pipeline
		patchData   map[string]interface{}
		expected    Pipeline
		expectError bool
	}{
		{
			name: "patch name and retries",
			original: Pipeline{
				Name:     "original-pipeline",
				Retries:  3,
				Schedule: "hourly",
			},
			patchData: map[string]interface{}{
				"name":    "patched-pipeline",
				"retries": 5,
			},
			expected: Pipeline{
				Name:        "patched-pipeline",
				Retries:     5,
				Schedule:    "hourly", // Should be preserved
				Concurrency: 1,        // Default value set by UnmarshalJSON
			},
			expectError: false,
		},
		{
			name: "patch concurrency and schedule",
			original: Pipeline{
				Name:        "test-pipeline",
				Concurrency: 1,
				Schedule:    "daily",
			},
			patchData: map[string]interface{}{
				"concurrency": 10,
				"schedule":    "hourly",
			},
			expected: Pipeline{
				Name:        "test-pipeline", // Should be preserved
				Concurrency: 10,
				Schedule:    "hourly",
			},
			expectError: false,
		},
		{
			name: "patch with assets",
			original: Pipeline{
				Name: "simple-pipeline",
			},
			patchData: map[string]interface{}{
				"name": "pipeline-with-assets",
				"assets": []map[string]interface{}{
					{
						"name": "test-asset",
						"type": "python",
					},
				},
			},
			expected: Pipeline{
				Name:        "pipeline-with-assets",
				Concurrency: 1, // Default value set by UnmarshalJSON
				Assets: []*Asset{
					{
						Name: "test-asset",
						Type: "python",
					},
				},
			},
			expectError: false,
		},
		{
			name: "patch with invalid JSON field",
			original: Pipeline{
				Name: "test-pipeline",
			},
			patchData: map[string]interface{}{
				"invalid_field": "should be ignored",
			},
			expected: Pipeline{
				Name:        "test-pipeline", // Should remain unchanged
				Concurrency: 1,               // Default value set by UnmarshalJSON
			},
			expectError: false,
		},
		{
			name: "patch with invalid retries type",
			original: Pipeline{
				Name: "test-pipeline",
			},
			patchData: map[string]interface{}{
				"retries": "not-a-number",
			},
			expected: Pipeline{
				Name: "test-pipeline",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a copy of the original pipeline
			p := tt.original

			// Convert patch data to JSON
			patchJSON, err := json.Marshal(tt.patchData)
			require.NoError(t, err)

			// Apply the patch
			err = json.Unmarshal(patchJSON, &p)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Verify the patch was applied correctly
			assert.Equal(t, tt.expected.Name, p.Name)
			assert.Equal(t, tt.expected.Retries, p.Retries)
			assert.Equal(t, tt.expected.Concurrency, p.Concurrency)
			assert.Equal(t, tt.expected.Schedule, p.Schedule)

			// Verify assets if they were patched
			if len(tt.expected.Assets) > 0 {
				assert.Equal(t, len(tt.expected.Assets), len(p.Assets))
				for i, expectedAsset := range tt.expected.Assets {
					assert.Equal(t, expectedAsset.Name, p.Assets[i].Name)
					assert.Equal(t, expectedAsset.Type, p.Assets[i].Type)
				}
			}
		})
	}
}

func TestPipeline_PatchPreservesExistingFields(t *testing.T) {
	t.Parallel()

	original := Pipeline{
		Name:        "original-pipeline",
		Retries:     3,
		Concurrency: 5,
		Schedule:    "hourly",
		StartDate:   "2024-01-01",
		Catchup:     true,
		Agent:       true,
		DefaultConnections: EmptyStringMap{
			"gcp": "my-connection",
		},
		Variables: Variables{
			"env": map[string]any{
				"default": "production",
			},
		},
		Tags:    EmptyStringArray{"important", "data"},
		Domains: EmptyStringArray{"analytics"},
		Meta: EmptyStringMap{
			"owner": "data-team",
		},
	}

	// Patch only the name
	patchData := map[string]interface{}{
		"name": "patched-pipeline",
	}

	patchJSON, err := json.Marshal(patchData)
	require.NoError(t, err)

	err = json.Unmarshal(patchJSON, &original)
	require.NoError(t, err)

	// Verify only the name changed, everything else preserved
	assert.Equal(t, "patched-pipeline", original.Name)
	assert.Equal(t, 3, original.Retries)
	assert.Equal(t, 5, original.Concurrency)
	assert.Equal(t, "hourly", string(original.Schedule))
	assert.Equal(t, "2024-01-01", original.StartDate)
	assert.True(t, original.Catchup)
	assert.True(t, original.Agent)
	assert.Equal(t, "my-connection", original.DefaultConnections["gcp"])
	assert.Equal(t, "production", original.Variables["env"]["default"])
	assert.Contains(t, []string(original.Tags), "important")
	assert.Contains(t, []string(original.Tags), "data")
	assert.Contains(t, []string(original.Domains), "analytics")
	assert.Equal(t, "data-team", original.Meta["owner"])
}
