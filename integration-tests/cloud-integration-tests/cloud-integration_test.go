package main

import (
	"errors"
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
	"bigquery":         "google_cloud_platform",
	"snowflake":        "snowflake",
	"postgres":         "postgres",
	"redshift":         "redshift",
	"athena":           "athena",
	"databricks":       "databricks",
	"fabric_warehouse": "fabric_warehouse",
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
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
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
	t.Parallel()

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
		t.Parallel()

		if !availablePlatforms["bigquery"] {
			t.Skip("Skipping BigQuery tests - no connection configured")
			return
		}

		bigqueryDir := filepath.Join(currentFolder, "bigquery")
		require.DirExists(t, bigqueryDir, "BigQuery test directory should exist")

		testFile := filepath.Join(bigqueryDir, "bigquery_test.go")
		require.FileExists(t, testFile, "BigQuery test file should exist")

		t.Logf("BigQuery platform is available - running integration tests")

		runTestsInDirectory(t, bigqueryDir, "BigQuery")
	})

	t.Run("Snowflake", func(t *testing.T) {
		t.Parallel()

		if !availablePlatforms["snowflake"] {
			t.Skip("Skipping Snowflake tests - no connection configured")
			return
		}

		snowflakeDir := filepath.Join(currentFolder, "snowflake")
		require.DirExists(t, snowflakeDir, "Snowflake test directory should exist")

		testFile := filepath.Join(snowflakeDir, "snowflake_test.go")
		require.FileExists(t, testFile, "Snowflake test file should exist")

		t.Logf("Snowflake platform is available - running integration tests")

		runTestsInDirectory(t, snowflakeDir, "Snowflake")
	})

	t.Run("Postgres", func(t *testing.T) {
		t.Parallel()

		if !availablePlatforms["postgres"] {
			t.Skip("Skipping Postgres tests - no connection configured")
			return
		}

		postgresDir := filepath.Join(currentFolder, "postgres")
		require.DirExists(t, postgresDir, "Postgres test directory should exist")

		testFile := filepath.Join(postgresDir, "postgres_test.go")
		require.FileExists(t, testFile, "Postgres test file should exist")

		t.Logf("Postgres platform is available - running integration tests")

		runTestsInDirectory(t, postgresDir, "Postgres")
	})

	t.Run("Redshift", func(t *testing.T) {
		t.Parallel()

		if !availablePlatforms["redshift"] {
			t.Skip("Skipping Redshift tests - no connection configured")
			return
		}

		redshiftDir := filepath.Join(currentFolder, "redshift")
		require.DirExists(t, redshiftDir, "Redshift test directory should exist")

		testFile := filepath.Join(redshiftDir, "redshift_test.go")
		require.FileExists(t, testFile, "Redshift test file should exist")

		t.Logf("Redshift platform is available - running integration tests")

		runTestsInDirectory(t, redshiftDir, "Redshift")
	})

	t.Run("Athena", func(t *testing.T) {
		t.Parallel()

		if !availablePlatforms["athena"] {
			t.Skip("Skipping Athena tests - no connection configured")
			return
		}

		athenaDir := filepath.Join(currentFolder, "athena")
		require.DirExists(t, athenaDir, "Athena test directory should exist")

		testFile := filepath.Join(athenaDir, "athena_test.go")
		require.FileExists(t, testFile, "Athena test file should exist")

		t.Logf("Athena platform is available - running integration tests")

		runTestsInDirectory(t, athenaDir, "Athena")
	})

	t.Run("Databricks", func(t *testing.T) {
		t.Parallel()

		if !availablePlatforms["databricks"] {
			t.Skip("Skipping Databricks tests - no connection configured")
			return
		}

		databricksDir := filepath.Join(currentFolder, "databricks")
		require.DirExists(t, databricksDir, "Databricks test directory should exist")

		testFile := filepath.Join(databricksDir, "databricks_test.go")
		require.FileExists(t, testFile, "Databricks test file should exist")

		t.Logf("Databricks platform is available - running integration tests")

		runTestsInDirectory(t, databricksDir, "Databricks")
	})

	t.Run("FabricWarehouse", func(t *testing.T) {
		t.Parallel()

		if !availablePlatforms["fabric_warehouse"] {
			t.Skip("Skipping Fabric Warehouse tests - no connection configured")
			return
		}

		fabricDir := filepath.Join(currentFolder, "fabric_warehouse")
		require.DirExists(t, fabricDir, "Fabric Warehouse test directory should exist")

		testFile := filepath.Join(fabricDir, "fabric_warehouse_test.go")
		require.FileExists(t, testFile, "Fabric Warehouse test file should exist")

		t.Logf("Fabric Warehouse platform is available - running integration tests")

		runTestsInDirectory(t, fabricDir, "Fabric Warehouse")
	})
}
