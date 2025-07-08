package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type CloudConfig struct {
	DefaultEnvironment string                 `yaml:"default_environment"`
	Environments       map[string]Environment `yaml:"environments"`
}

type Environment struct {
	Connections map[string]interface{} `yaml:"connections"`
}

var platformConnectionMap = map[string]string{
	"bigquery":  "gcp",
	"snowflake": "snowflake",
}

func getAvailablePlatforms(configPath string) (map[string]bool, error) {
	available := make(map[string]bool)

	data, err := os.ReadFile(configPath)
	if err != nil {
		return available, err
	}

	var config CloudConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return available, err
	}

	defaultEnv := config.DefaultEnvironment
	if defaultEnv == "" {
		defaultEnv = "default"
	}

	env, exists := config.Environments[defaultEnv]
	if !exists {
		return available, nil
	}

	for platform, connectionType := range platformConnectionMap {
		if _, hasConnection := env.Connections[connectionType]; hasConnection {
			available[platform] = true
		}
	}

	return available, nil
}

func runTestsInDirectory(t *testing.T, dir string, platformName string) {
	cmd := exec.Command("go", "test", "-v", "./...")
	cmd.Dir = dir
	cmd.Env = os.Environ()

	output, err := cmd.CombinedOutput()

	t.Logf("%s test output:\n%s", platformName, string(output))

	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() != 0 {
				t.Errorf("%s tests failed with exit code %d", platformName, exitError.ExitCode())
			}
		} else {
			t.Errorf("%s tests failed to execute: %v", platformName, err)
		}
	} else {
		t.Logf("✅ %s tests completed successfully", platformName)
	}
}

func TestCloudIntegration(t *testing.T) {
	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	configPath := filepath.Join(currentFolder, ".bruin.cloud.yml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Cloud configuration file not found - skipping cloud integration tests")
		return
	}

	availablePlatforms, err := getAvailablePlatforms(configPath)
	require.NoError(t, err, "Failed to parse cloud configuration")

	t.Run("BigQuery", func(t *testing.T) {
		if !availablePlatforms["bigquery"] {
			t.Skip("Skipping BigQuery tests - no connection configured")
			return
		}

		// Validate BigQuery test environment
		bigqueryDir := filepath.Join(currentFolder, "bigquery")
		require.DirExists(t, bigqueryDir, "BigQuery test directory should exist")

		testFile := filepath.Join(bigqueryDir, "bigquery_test.go")
		require.FileExists(t, testFile, "BigQuery test file should exist")

		t.Logf("BigQuery platform is available - running integration tests")

		runTestsInDirectory(t, bigqueryDir, "BigQuery")
	})

	t.Run("Snowflake", func(t *testing.T) {
		if !availablePlatforms["snowflake"] {
			t.Skip("Skipping Snowflake tests - no connection configured")
			return
		}

		// Validate Snowflake test environment
		snowflakeDir := filepath.Join(currentFolder, "snowflake")
		require.DirExists(t, snowflakeDir, "Snowflake test directory should exist")

		testFile := filepath.Join(snowflakeDir, "snowflake_test.go")
		require.FileExists(t, testFile, "Snowflake test file should exist")

		t.Logf("Snowflake platform is available - running integration tests")

		runTestsInDirectory(t, snowflakeDir, "Snowflake")
	})
}
