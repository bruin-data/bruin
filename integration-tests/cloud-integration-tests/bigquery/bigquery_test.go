package bigquery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestBigQueryIndividualTasks(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	tasks := getBigQueryTasks(binary, currentFolder)

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

func TestBigQueryWorkflows(t *testing.T) {
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

func RunBigQueryIndividualTasks(t *testing.T) {
	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	// Go back to project root from bigquery subdirectory
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	tasks := getBigQueryTasks(binary, currentFolder)

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

func RunBigQueryWorkflows(t *testing.T) {
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

func getBigQueryTasks(binary string, currentFolder string) []e2e.Task {
	projectRoot := filepath.Join(currentFolder, "../../../")
	configFile := filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")

	return []e2e.Task{
		{
			Name:    "bigquery-run-pipeline",
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
			Name:    "bigquery-run-single-asset",
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
			Name: "bigquery-products-create-and-validate",
			Steps: []e2e.Task{
				{
					Name:    "create the initial products table",
					Command: binary,
					Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")),
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
					Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM dataset.products ORDER BY PRODUCT_ID;", "--output", "csv"),
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
		{
			Name: "[bigquery] SCD2 by column workflow",
			Steps: []e2e.Task{
				{
					Name:    "scd2-by-column: restore menu asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/menu_original.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2-by-column: create the initial table",
					Command: binary,
					Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline")),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2-by-column: query the initial table",
					Command: binary,
					Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/expected_initial.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
				{
					Name:    "scd2-by-column: copy updated menu data",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/menu_updated.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2-by-column: run SCD2 materialization",
					Command: binary,
					Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2-by-column: query the final SCD2 table",
					Command: binary,
					Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/final_expected.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
			},
		},
		{
			Name: "[bigquery] SCD2 by time workflow",
			Steps: []e2e.Task{
				{
					Name:    "scd2-by-time: restore products asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/products_original.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2-by-time: create the initial products table",
					Command: binary,
					Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline")),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2-by-time: update products with new data",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/products_updated.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "scd2-by-time: run SCD2 by time materialization",
					Command: binary,
					Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
			},
		},
	}

	return workflows
}
