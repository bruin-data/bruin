package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckLint(t *testing.T) {
	tests := []struct {
		name          string
		assetFilePath string
		expectError   bool
		expectChange  bool
	}{
		{
			name:          "Valid Asset",
			assetFilePath: "./testdata/unformatted-pipeline/assets/valid_asset.sql",
			expectError:   false,
			expectChange:  false,
		},
		{
			name:          "Invalid Pipeline",
			assetFilePath: "./testdata/unformatted-pipeline/assets/broken_asset.sql",
			expectError:   true,
			expectChange:  false,
		},
		{
			name:          "Needs Reformatting",
			assetFilePath: "./testdata/unformatted-pipeline/assets/needs_reformatting.sql",
			expectError:   false,
			expectChange:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup: Ensure testdata directory and file exist
			absPath, err := filepath.Abs(tc.assetFilePath)
			assert.NoError(t, err, "Failed to get absolute path")

			_, err = os.Stat(absPath)
			assert.NoError(t, err, "Test file does not exist")

			// Act: Run the check-lint functionality
			changed, err := shouldFileChange(absPath)

			// Assert: Check for expected results
			if tc.expectError {
				assert.Error(t, err, "Expected an error but didn't get one")
			} else {
				assert.NoError(t, err, "Did not expect an error but got one")
			}

			assert.Equal(t, tc.expectChange, changed, "Unexpected change detection result")
		})
	}
}
