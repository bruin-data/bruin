package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckLint(t *testing.T) {
	t.Parallel() // Enables parallel execution for the entire test

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
			name:          "Needs Reformatting",
			assetFilePath: "./testdata/unformatted-pipeline/assets/needs_reformatting.sql",
			expectError:   false,
			expectChange:  true,
		},
	}

	for _, tc := range tests {
		tc := tc // Capture the loop variable for parallel tests
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel() // Enables parallel execution for subtests

			// Setup: Ensure testdata directory and file exist
			absPath, err := filepath.Abs(tc.assetFilePath)
			require.NoError(t, err, "Failed to get absolute path")

			_, err = os.Stat(absPath)
			require.NoError(t, err, "Test file does not exist")

			// Act: Run the check-lint functionality
			changed, err := shouldFileChange(absPath)

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
