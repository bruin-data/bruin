package bigquery

import (
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/e2e"
)

func TestConnection(binary string, currentFolder string) []e2e.Task {
	configFlags := []string{"--config-file", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	tasks := []e2e.Task{
		{
			Name:    "[bigquery] query 'select 1'",
			Command: binary,
			Args:    []string{"query", "--env", "default", "--connection", "gcp", "--query", "SELECT 1", "--output", "json"},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   `{"columns":[{"name":"f0_","type":"INTEGER"}],"rows":[[1]],"connectionName":"gcp","query":"SELECT 1"}`,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
	}

	for i := range tasks {
		tasks[i].Args = append(tasks[i].Args, configFlags...)
	}

	return tasks
}

func GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
	configFlags := []string{"--config-file", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	workflows := []e2e.Workflow{
		{
			Name: "[bigquery] SCD2 by column workflow",
			Steps: []e2e.Task{
				{
					Name:    "restore menu asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/resources/menu_original.sql"), filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-column-pipeline/assets/menu.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "create the initial table",
					Command: binary,
					Args:    append([]string{"run", "--full-refresh", "--env", "default", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-column-pipeline")}, configFlags...),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "query the initial table",
					Command: binary,
					Args:    append([]string{"query", "--env", "default", "--asset", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"}, configFlags...),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/expected_initial.csv"),
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByCSV,
					},
				},
				{
					Name:    "copy updated menu data",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/resources/menu_updated.sql"), filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-column-pipeline/assets/menu.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "run SCD2 materialization",
					Command: binary,
					Args:    append([]string{"run", "--env", "default", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-column-pipeline/assets/menu.sql")}, configFlags...),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "query the final SCD2 table",
					Command: binary,
					Args:    append([]string{"query", "--env", "default", "--asset", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"}, configFlags...),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
						CSVFile:  filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/final_expected.csv"),
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
					Name:    "restore products asset to initial state",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/resources/products_original.sql"), filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "create the initial products table",
					Command: binary,
					Args:    append([]string{"run", "--full-refresh", "--env", "default", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-time-pipeline")}, configFlags...),
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "update products with new data",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/resources/products_updated.sql"), filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-time-pipeline/assets/products.sql")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "run SCD2 by time materialization",
					Command: binary,
					Args:    append([]string{"run", "--env", "default", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/bigquery/big-test-pipes/scd2-by-time-pipeline/assets/products.sql")}, configFlags...),
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
