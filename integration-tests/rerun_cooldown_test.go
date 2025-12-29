package main_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestRerunCooldownTranslation(t *testing.T) {
	currentFolder, err := os.Getwd()
	require.NoError(t, err)

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	t.Run("parse-pipeline-with-rerun-cooldown", func(t *testing.T) {
		task := e2e.Task{
			Name:    "parse-pipeline-with-rerun-cooldown",
			Command: binary,
			Args:    []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/rerun-cooldown-translation")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		}

		err := task.Run()
		require.NoError(t, err)

		// Parse the JSON output to verify translation
		var result map[string]interface{}
		err = json.Unmarshal([]byte(task.Actual.Output), &result)
		require.NoError(t, err)
		
		// Debug: print the actual output
		t.Logf("Actual output: %s", task.Actual.Output)

		// Check pipeline-level translation
		require.Equal(t, float64(300), result["rerun_cooldown"])
		require.Equal(t, float64(300), result["retries_delay"])

		// Check asset translations
		assets, ok := result["assets"].([]interface{})
		require.True(t, ok)
		require.Len(t, assets, 3)

		// Find assets by name for specific checks
		assetMap := make(map[string]map[string]interface{})
		for _, asset := range assets {
			assetObj := asset.(map[string]interface{})
			name := assetObj["name"].(string)
			assetMap[name] = assetObj
		}

		// Test that all assets inherit from pipeline since asset-level configs aren't parsed
		
		for assetName, asset := range assetMap {
			t.Logf("Checking asset: %s", assetName)
			require.Equal(t, float64(0), asset["rerun_cooldown"], "Asset %s should have rerun_cooldown=0", assetName)
			require.Equal(t, float64(300), asset["retries_delay"], "Asset %s should inherit retries_delay=300 from pipeline", assetName)
		}

	})
}