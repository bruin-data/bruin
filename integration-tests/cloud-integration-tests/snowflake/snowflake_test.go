package snowflake

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/stretchr/testify/require"
)

// TestSnowflakeIndividualTasks runs individual Snowflake integration tests
func TestSnowflakeIndividualTasks(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	// Go back to project root from snowflake subdirectory
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	tasks := getSnowflakeTasks(binary, currentFolder)

	for _, task := range tasks {
		t.Run(task.Name, func(t *testing.T) {
			t.Parallel()

			err := task.Run()
			if task.Expected.ExitCode != 0 {
				require.Error(t, err, "Expected task to fail but it succeeded")
			} else {
				require.NoError(t, err, "Task failed unexpectedly: %v", err)
			}
		})
	}
}

// TestSnowflakeWorkflows runs multi-step workflow tests
func TestSnowflakeWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	workflows := getWorkflows(binary, currentFolder)

	for _, workflow := range workflows {
		t.Run(workflow.Name, func(t *testing.T) {
			t.Parallel()

			err := workflow.Run()
			require.NoError(t, err, "Workflow %s failed: %v", workflow.Name, err)

			t.Logf("Workflow '%s' completed successfully", workflow.Name)
		})
	}
}

// RunSnowflakeIndividualTasks runs individual Snowflake integration tests (can be called externally)
func RunSnowflakeIndividualTasks(t *testing.T) {
	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	// Go back to project root from snowflake subdirectory
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	tasks := getSnowflakeTasks(binary, currentFolder)

	for _, task := range tasks {
		t.Run(task.Name, func(t *testing.T) {
			err := task.Run()
			if task.Expected.ExitCode != 0 {
				require.Error(t, err, "Expected task to fail but it succeeded")
			} else {
				require.NoError(t, err, "Task failed unexpectedly: %v", err)
			}
		})
	}
}

// RunSnowflakeWorkflows runs multi-step workflow tests (can be called externally)
func RunSnowflakeWorkflows(t *testing.T) {
	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	workflows := getWorkflows(binary, currentFolder)

	for _, workflow := range workflows {
		t.Run(workflow.Name, func(t *testing.T) {
			err := workflow.Run()
			require.NoError(t, err, "Workflow %s failed: %v", workflow.Name, err)

			t.Logf("Workflow '%s' completed successfully", workflow.Name)
		})
	}
}

// getSnowflakeTasks returns the list of individual Snowflake integration tests
func getSnowflakeTasks(binary string, currentFolder string) []e2e.Task {
	// Get project root from currentFolder (which is snowflake directory)
	projectRoot := filepath.Join(currentFolder, "../../../")
	configFile := filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")

	return []e2e.Task{
		{
			Name:    "snowflake-query-asset",
			Command: binary,
			Args: []string{
				"query",
				"--config-file", configFile,
				"--env", "default",
				"--output", "json",
				"--asset", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql"),
			},
			Env: []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/expected.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "snowflake-run-pipeline",
			Command: binary,
			Args: []string{
				"run",
				"--config-file", configFile,
				"--env", "default",
				"--full-refresh",
				filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline"),
			},
			Env: []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "snowflake-run-single-asset",
			Command: binary,
			Args: []string{
				"run",
				"--config-file", configFile,
				"--env", "default",
				filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql"),
			},
			Env: []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
	}
}

func getWorkflows(binary string, currentFolder string) []e2e.Workflow {
	projectRoot := filepath.Join(currentFolder, "../../../")
	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	workflows := []e2e.Workflow{
		{
			Name: "snowflake-products-create-and-validate",
			Steps: []e2e.Task{
				{
					Name:    "create the initial products table",
					Command: binary,
					Args:    append([]string{"run", "--full-refresh", "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")}, configFlags...),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "query the products table",
					Command: binary,
					Args:    append([]string{"query", "--connection", "snowflake-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM products ORDER BY PRODUCT_ID;", "--output", "csv"}, configFlags...),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/expected_products_table.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
			},
		},
	}

	return workflows
}
