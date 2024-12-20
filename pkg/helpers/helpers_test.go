package helpers

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetIngestrDestinationType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asset   *pipeline.Asset
		want    pipeline.AssetType
		wantErr bool
	}{
		{
			name: "postgres",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"destination": "postgres",
				},
			},
			want: pipeline.AssetTypePostgresQuery,
		},
		{
			name: "gcp",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"destination": "bigquery",
				},
			},
			want: pipeline.AssetTypeBigqueryQuery,
		},
		{
			name: "not found",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"destination": "sqlite",
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assetType, err := GetIngestrDestinationType(tc.asset)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				assert.Equal(t, tc.want, assetType)
			}
		})
	}
}

func Test_WriteJSONToFile(t *testing.T) {
	t.Parallel()

	type testData struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	data := testData{
		Name:  "Test",
		Value: 123,
	}

	filename := "test_output.json"

	// Call the function with the actual filesystem
	err := WriteJSONToFile(data, filename)
	require.NoError(t, err)

	// Verify the file exists in the actual filesystem
	_, err = os.Stat(filename)
	require.NoError(t, err)

	// Read the file and verify its contents
	fileContent, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedContent, err := json.MarshalIndent(data, "", "  ")
	require.NoError(t, err)

	assert.Equal(t, string(expectedContent), string(fileContent))

	// Clean up the file after test
	err = os.Remove(filename)
	require.NoError(t, err)
}
