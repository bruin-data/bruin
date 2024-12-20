package cmd

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckLint(t *testing.T) {
	t.Parallel()
	var testDatadir string
	if runtime.GOOS == "windows" {
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
