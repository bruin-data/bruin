package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bruin-data/bruin/pkg/python"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatAssetSecrets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "omitted inject_as stays omitted",
			input: "  - key: MY_API_KEY\n",
			want:  "  - key: MY_API_KEY\n",
		},
		{
			name:  "redundant inject_as is removed",
			input: "  - key: MY_API_KEY\n    inject_as: MY_API_KEY\n",
			want:  "  - key: MY_API_KEY\n",
		},
		{
			name:  "different inject_as is preserved",
			input: "  - key: my-api-key\n    inject_as: MY_API_KEY\n",
			want:  "  - key: my-api-key\n    inject_as: MY_API_KEY\n",
		},
		{
			name:  "case difference is preserved",
			input: "  - key: my_api_key\n    inject_as: MY_API_KEY\n",
			want:  "  - key: my_api_key\n    inject_as: MY_API_KEY\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assetPath := filepath.Join(t.TempDir(), "secret.py")
			content := fmt.Sprintf("\"\"\"@bruin\nname: test.secret\nsecrets:\n%s@bruin\"\"\"\n\nprint('hello')\n", tt.input)
			require.NoError(t, os.WriteFile(assetPath, []byte(content), 0o644))

			asset, err := formatAsset(assetPath)
			require.NoError(t, err)

			formatted, err := os.ReadFile(assetPath)
			require.NoError(t, err)
			assert.Contains(t, string(formatted), "secrets:\n"+tt.want+"\n@bruin")
			assert.Contains(t, string(formatted), "print('hello')\n")

			reparsed, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath, nil)
			require.NoError(t, err)
			assert.Equal(t, asset.Secrets, reparsed.Secrets)

			changed, err := shouldFileChange(assetPath)
			require.NoError(t, err)
			assert.False(t, changed, "formatting should be idempotent")
		})
	}
}

func TestCheckLint(t *testing.T) {
	t.Parallel()
	var testDatadir string
	if runtime.GOOS == osWindows {
		testDatadir = "testdata\\unformatted-pipeline\\assets"
	} else {
		testDatadir = "testdata/unformatted-pipeline/assets"
	}

	tests := []struct {
		name          string
		assetFilePath string
		expectError   bool
		expectChange  bool
	}{
		{
			name:          "Valid Asset",
			assetFilePath: filepath.Join(testDatadir, "valid_asset.sql"),
			expectError:   false,
			expectChange:  false,
		},
		{
			name:          "Needs Reformatting",
			assetFilePath: filepath.Join(testDatadir, "needs_reformatting.sql"),
			expectError:   false,
			expectChange:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Debug: Log the resolved path
			t.Logf("Testing file path: %s", tc.assetFilePath)

			// Ensure test file exists
			_, err := os.Stat(tc.assetFilePath)
			require.NoError(t, err, "Test file does not exist")

			// Act: Run the check-lint functionality
			changed, err := shouldFileChange(tc.assetFilePath)

			// Assert: Check for expected results
			if tc.expectError {
				require.Error(t, err, "Expected an error but didn't get one")
			} else {
				require.NoError(t, err, "Did not expect an error but got one")
			}

			require.Equal(t, tc.expectChange, changed, "Unexpected change detection result")
		})
	}
}

func TestFindSQLFilesWithDialectsUsesTSQLForFabric(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	assetsDir := filepath.Join(tempDir, "assets")
	require.NoError(t, os.MkdirAll(assetsDir, 0o755))

	fabricAssetPath := filepath.Join(assetsDir, "fabric_asset.sql")
	require.NoError(t, os.WriteFile(fabricAssetPath, []byte(`/* @bruin
name: test.fabric_asset
type: fabric.sql
@bruin */

SELECT 1;
`), 0o644))

	legacyFabricAssetPath := filepath.Join(assetsDir, "legacy_fabric_asset.sql")
	require.NoError(t, os.WriteFile(legacyFabricAssetPath, []byte(`/* @bruin
name: test.legacy_fabric_asset
type: fw.sql
@bruin */

SELECT 1;
`), 0o644))

	got, err := findSQLFilesWithDialects(tempDir)
	require.NoError(t, err)

	assert.ElementsMatch(t, []python.SQLFileInfo{
		{FilePath: fabricAssetPath, Dialect: "tsql"},
		{FilePath: legacyFabricAssetPath, Dialect: "tsql"},
	}, got)
}
