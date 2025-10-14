package main_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPatchPipeline_BasicFunctionality(t *testing.T) {
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)
	tests := []struct {
		name           string
		pipelinePath   string
		patchBody      map[string]interface{}
		expectedFields map[string]interface{}
	}{
		{
			name:         "patch simple pipeline name and retries",
			pipelinePath: filepath.Join(currentFolder, "../pkg/pipeline/testdata/persist/simple-pipeline.yml"),
			patchBody: map[string]interface{}{
				"name":    "patched-simple-pipeline",
				"retries": 5,
			},
			expectedFields: map[string]interface{}{
				"name":    "patched-simple-pipeline",
				"retries": 5,
			},
		},
		{
			name:         "patch complex pipeline concurrency and schedule",
			pipelinePath: filepath.Join(currentFolder, "../pkg/pipeline/testdata/persist/complex-pipeline.yml"),
			patchBody: map[string]interface{}{
				"concurrency": 10,
				"schedule":    "daily",
			},
			expectedFields: map[string]interface{}{
				"concurrency": 10,
				"schedule":    "daily",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

			originalContent, err := os.ReadFile(tt.pipelinePath)
			require.NoError(t, err)
			err = os.WriteFile(tempPipelinePath, originalContent, 0o644)
			require.NoError(t, err)

			patchJSON, err := json.Marshal(tt.patchBody)
			require.NoError(t, err)

			task := e2e.Task{
				Name:    tt.name,
				Command: binary,
				Args:    []string{"internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			}

			err = task.Run()
			require.NoError(t, err)

			patchedContent, err := os.ReadFile(tempPipelinePath)
			require.NoError(t, err)

			var pipelineData map[string]interface{}
			err = yaml.Unmarshal(patchedContent, &pipelineData)
			require.NoError(t, err)

			for field, expectedValue := range tt.expectedFields {
				assert.Equal(t, expectedValue, pipelineData[field])
			}
		})
	}
}

func TestPatchPipeline_WithAssets(t *testing.T) {
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tempDir := t.TempDir()
	tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

	originalContent := `name: test-pipeline
schedule: daily`
	err = os.WriteFile(tempPipelinePath, []byte(originalContent), 0o644)
	require.NoError(t, err)

	patchBody := map[string]interface{}{
		"name": "pipeline-with-assets",
		"assets": []map[string]interface{}{
			{
				"name": "test-asset",
				"type": "python",
			},
		},
	}

	patchJSON, err := json.Marshal(patchBody)
	require.NoError(t, err)

	task := e2e.Task{
		Name:    "patch-pipeline-with-assets",
		Command: binary,
		Args:    []string{"internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath},
		Expected: e2e.Output{
			ExitCode: 0,
		},
		WorkingDir: currentFolder,
		Asserts: []func(*e2e.Task) error{
			e2e.AssertByExitCode,
		},
	}

	err = task.Run()
	require.NoError(t, err)

	fileContent, err := os.ReadFile(tempPipelinePath)
	require.NoError(t, err)
	assert.Contains(t, string(fileContent), "assets:")
	assert.Contains(t, string(fileContent), "test-asset")
	assert.Contains(t, string(fileContent), "python")
}

func TestPatchPipeline_PreservesExistingFields(t *testing.T) {
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tempDir := t.TempDir()
	tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

	originalContent, err := os.ReadFile(filepath.Join(currentFolder, "../pkg/pipeline/testdata/persist/complex-pipeline.yml"))
	require.NoError(t, err)
	err = os.WriteFile(tempPipelinePath, originalContent, 0o644)
	require.NoError(t, err)

	patchBody := map[string]interface{}{
		"name": "updated-complex-pipeline",
	}

	patchJSON, err := json.Marshal(patchBody)
	require.NoError(t, err)

	task := e2e.Task{
		Name:    "patch-pipeline-preserves-fields",
		Command: binary,
		Args:    []string{"internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath},
		Expected: e2e.Output{
			ExitCode: 0,
		},
		WorkingDir: currentFolder,
		Asserts: []func(*e2e.Task) error{
			e2e.AssertByExitCode,
		},
	}

	err = task.Run()
	require.NoError(t, err)

	fileContent, err := os.ReadFile(tempPipelinePath)
	require.NoError(t, err)
	assert.Contains(t, string(fileContent), "updated-complex-pipeline")
	assert.Contains(t, string(fileContent), "hourly")
	assert.Contains(t, string(fileContent), "2024-01-01")
	assert.Contains(t, string(fileContent), "retries: 3")
	assert.Contains(t, string(fileContent), "concurrency: 5")
}

func TestPatchPipeline_OnlyPipelineOption(t *testing.T) {
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tempDir := t.TempDir()
	tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

	originalContent := `name: test-pipeline
schedule: daily`
	err = os.WriteFile(tempPipelinePath, []byte(originalContent), 0o644)
	require.NoError(t, err)

	assetsDir := filepath.Join(tempDir, "assets")
	err = os.MkdirAll(assetsDir, 0o755)
	require.NoError(t, err)

	assetFile := filepath.Join(assetsDir, "test-asset.yml")
	assetContent := `name: filesystem-asset
type: python`
	err = os.WriteFile(assetFile, []byte(assetContent), 0o644)
	require.NoError(t, err)

	patchBody := map[string]interface{}{
		"name": "pipeline-with-patch-assets",
		"assets": []map[string]interface{}{
			{
				"name": "patch-asset",
				"type": "bq.sql",
			},
		},
	}

	patchJSON, err := json.Marshal(patchBody)
	require.NoError(t, err)

	task := e2e.Task{
		Name:    "patch-pipeline-only-pipeline",
		Command: binary,
		Args:    []string{"internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath},
		Expected: e2e.Output{
			ExitCode: 0,
		},
		WorkingDir: currentFolder,
		Asserts: []func(*e2e.Task) error{
			e2e.AssertByExitCode,
		},
	}

	err = task.Run()
	require.NoError(t, err)

	fileContent, err := os.ReadFile(tempPipelinePath)
	require.NoError(t, err)
	assert.Contains(t, string(fileContent), "assets:")
	assert.Contains(t, string(fileContent), "patch-asset")
	assert.Contains(t, string(fileContent), "bq.sql")
	assert.NotContains(t, string(fileContent), "filesystem-asset")
}