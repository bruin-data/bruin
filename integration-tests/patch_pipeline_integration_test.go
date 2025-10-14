package main_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	executableName    = "bruin"
	executableNameWin = "bruin.exe"
	windowsOS         = "windows"
)

func TestPatchPipeline_Workflow(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := executableName
	if runtime.GOOS == windowsOS {
		executable = executableNameWin
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tempDir := t.TempDir()
	testDir := filepath.Join(tempDir, "test-patch-pipeline")

	workflow := e2e.Workflow{
		Name: "patch_pipeline_workflow",
		Steps: []e2e.Task{
			{
				Name:    "create test directory",
				Command: "mkdir",
				Args:    []string{"-p", testDir},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
			{
				Name:       "initialize git repository",
				Command:    "git",
				Args:       []string{"init"},
				WorkingDir: testDir,
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
			{
				Name:       "copy simple pipeline",
				Command:    "cp",
				Args:       []string{filepath.Join(currentFolder, "../pkg/pipeline/testdata/persist/simple-pipeline.yml"), "pipeline.yml"},
				WorkingDir: testDir,
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
			{
				Name:    "patch pipeline name and add retries",
				Command: binary,
				Args:    []string{"internal", "patch-pipeline", "--body", `{"name": "patched-pipeline", "retries": 5}`, filepath.Join(testDir, "pipeline.yml")},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
			{
				Name:    "patch pipeline concurrency and schedule",
				Command: binary,
				Args:    []string{"internal", "patch-pipeline", "--body", `{"concurrency": 10, "schedule": "daily"}`, filepath.Join(testDir, "pipeline.yml")},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
			{
				Name:    "patch pipeline with assets",
				Command: binary,
				Args:    []string{"internal", "patch-pipeline", "--body", `{"name": "final-pipeline", "assets": [{"name": "test-asset", "type": "python"}]}`, filepath.Join(testDir, "pipeline.yml")},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
	}

	err = workflow.Run()
	require.NoError(t, err)

	pipelinePath := filepath.Join(testDir, "pipeline.yml")
	fileContent, err := os.ReadFile(pipelinePath)
	require.NoError(t, err)

	// Parse the YAML to verify the final state
	var pipelineData map[string]interface{}
	err = yaml.Unmarshal(fileContent, &pipelineData)
	require.NoError(t, err)

	// Verify all patches were applied correctly
	assert.Equal(t, "final-pipeline", pipelineData["name"])
	assert.Equal(t, 5, pipelineData["retries"])
	assert.Equal(t, 10, pipelineData["concurrency"])
	assert.Equal(t, "daily", pipelineData["schedule"])

	// Verify assets were added
	assets, ok := pipelineData["assets"].([]interface{})
	require.True(t, ok, "assets should be present")
	require.Len(t, assets, 1, "should have exactly one asset")

	asset, ok := assets[0].(map[string]interface{})
	require.True(t, ok, "asset should be a map")
	assert.Equal(t, "test-asset", asset["name"])
	assert.Equal(t, "python", asset["type"])

	// Verify original fields are preserved
	assert.Equal(t, "my-connection", pipelineData["default_connections"].(map[string]interface{})["gcp"])
	assert.Equal(t, "2023-01-01", pipelineData["start_date"])
}
