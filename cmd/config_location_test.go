package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestValidateDefaultConfigFileLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setup            func(t *testing.T) (repoRoot string, inputPath string)
		errorContains    string
		errorNotContains string
		expectError      bool
	}{
		{
			name: "valid when only root config exists",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				repoRoot := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(repoRoot, ".bruin.yml"), []byte("default_environment: default\n"), 0o644))

				inputPath := filepath.Join(repoRoot, "my-pipeline")
				require.NoError(t, os.MkdirAll(inputPath, 0o755))

				return repoRoot, inputPath
			},
		},
		{
			name: "errors when config exists in nested path but missing at root",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				repoRoot := t.TempDir()
				pipelineDir := filepath.Join(repoRoot, "my-pipeline")
				require.NoError(t, os.MkdirAll(pipelineDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, ".bruin.yml"), []byte("default_environment: default\n"), 0o644))

				return repoRoot, pipelineDir
			},
			expectError:      true,
			errorContains:    "could not find '.bruin.yml' in repository root",
			errorNotContains: "found multiple '.bruin.yml' files",
		},
		{
			name: "errors when both root and nested configs exist",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				repoRoot := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(repoRoot, ".bruin.yml"), []byte("default_environment: default\n"), 0o644))

				pipelineDir := filepath.Join(repoRoot, "my-pipeline")
				require.NoError(t, os.MkdirAll(pipelineDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, ".bruin.yml"), []byte("default_environment: default\n"), 0o644))

				return repoRoot, pipelineDir
			},
			expectError:   true,
			errorContains: "found multiple '.bruin.yml' files for this run",
		},
		{
			name: "no error when there is no config anywhere in path",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				repoRoot := t.TempDir()
				inputPath := filepath.Join(repoRoot, "my-pipeline")
				require.NoError(t, os.MkdirAll(inputPath, 0o755))
				return repoRoot, inputPath
			},
		},
		{
			name: "works when input path points to a file",
			setup: func(t *testing.T) (string, string) {
				t.Helper()
				repoRoot := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(repoRoot, ".bruin.yml"), []byte("default_environment: default\n"), 0o644))

				pipelineDir := filepath.Join(repoRoot, "my-pipeline")
				assetDir := filepath.Join(pipelineDir, "assets")
				require.NoError(t, os.MkdirAll(assetDir, 0o755))

				inputPath := filepath.Join(assetDir, "my_asset.sql")
				require.NoError(t, os.WriteFile(inputPath, []byte("select 1"), 0o644))
				return repoRoot, inputPath
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repoRoot, inputPath := tt.setup(t)
			err := validateDefaultConfigFileLocation(afero.NewOsFs(), repoRoot, inputPath)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
				if tt.errorNotContains != "" {
					require.NotContains(t, err.Error(), tt.errorNotContains)
				}
				return
			}

			require.NoError(t, err)
		})
	}
}