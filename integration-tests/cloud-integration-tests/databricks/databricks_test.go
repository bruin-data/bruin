package databricks

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestDatabricksWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	tempDir := t.TempDir()

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "databricks-products-create-and-validate",
			workflow: e2e.Workflow{
				Name: "databricks-products-create-and-validate",
				Steps: []e2e.Task{
					{
						Name:    "drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "confirm the table is dropped",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT * FROM test.products;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "create the initial products table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")),
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, stock FROM test.products ORDER BY product_id;", "--output", "csv"),
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
		},
		{
			name: "table-sensor",
			workflow: e2e.Workflow{
				Name: "table-sensor",
				Steps: []e2e.Task{
					{
						Name:    "table-sensor: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.datatable;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "table-sensor: confirm the table is dropped",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT * FROM test.datatable;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "table-sensor: run the table sensor",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--sensor-mode", "wait", "--timeout", "10", filepath.Join(currentFolder, "test-pipelines/table-sensor-pipeline/assets/table_sensor.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"Poking: test.datatable", "Failed: test.sensor"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "table-sensor: create the table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/table-sensor-pipeline/assets/create_table.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.datatable"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "table-sensor: run the table sensor",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--sensor-mode", "wait", "--timeout", "20", filepath.Join(currentFolder, "test-pipelines/table-sensor-pipeline/assets/table_sensor.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Poking: test.datatable", "Finished: test.sensor"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "table-sensor: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.datatable;"),
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
		},
		{
			name: "truncate-insert-materialization",
			workflow: e2e.Workflow{
				Name: "truncate-insert-materialization",
				Steps: []e2e.Task{
					{
						Name:    "truncate-insert: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-truncate-insert")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "truncate-insert: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-truncate-insert"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "truncate-insert: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline"), filepath.Join(tempDir, "test-truncate-insert")},
						WorkingDir: filepath.Join(tempDir, "test-truncate-insert"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "truncate-insert: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_truncate;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "truncate-insert: create initial table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-truncate-insert/truncate-insert-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_truncate"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "truncate-insert: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, stock FROM test.products_truncate ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-truncate-insert/truncate-insert-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "truncate-insert: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-truncate-insert/truncate-insert-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "truncate-insert: run with truncate+insert strategy",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-truncate-insert/truncate-insert-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_truncate"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "truncate-insert: query updated table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, stock FROM test.products_truncate ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-truncate-insert/truncate-insert-pipeline/expectations/updated_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "truncate-insert: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_truncate;"),
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
		},
		{
			name: "append-materialization",
			workflow: e2e.Workflow{
				Name: "append-materialization",
				Steps: []e2e.Task{
					{
						Name:    "append: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-append")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "append: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-append"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "append: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/append-pipeline"), filepath.Join(tempDir, "test-append")},
						WorkingDir: filepath.Join(tempDir, "test-append"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "append: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_append;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "append: create initial table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-append/append-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_append"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "append: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_append ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-append/append-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "append: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/append-pipeline/resources/products_append.sql"), filepath.Join(tempDir, "test-append/append-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "append: run append strategy",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-append/append-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_append"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "append: query appended table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_append ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-append/append-pipeline/expectations/appended_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "append: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_append;"),
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
		},
		{
			name: "merge-materialization",
			workflow: e2e.Workflow{
				Name: "merge-materialization",
				Steps: []e2e.Task{
					{
						Name:    "merge: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-merge")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "merge: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-merge"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "merge: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/merge-pipeline"), filepath.Join(tempDir, "test-merge")},
						WorkingDir: filepath.Join(tempDir, "test-merge"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "merge: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_merge;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "merge: create initial table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-merge/merge-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_merge"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "merge: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_merge ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-merge/merge-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "merge: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/merge-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-merge/merge-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "merge: run merge strategy",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-merge/merge-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_merge"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "merge: query merged table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_merge ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-merge/merge-pipeline/expectations/merged_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "merge: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_merge;"),
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
		},
		{
			name: "delete-insert-materialization",
			workflow: e2e.Workflow{
				Name: "delete-insert-materialization",
				Steps: []e2e.Task{
					{
						Name:    "delete-insert: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-delete-insert")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "delete-insert: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-delete-insert"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "delete-insert: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline"), filepath.Join(tempDir, "test-delete-insert")},
						WorkingDir: filepath.Join(tempDir, "test-delete-insert"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "delete-insert: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_delete_insert;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "delete-insert: create initial table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-delete-insert/delete-insert-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_delete_insert"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "delete-insert: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, dt FROM test.products_delete_insert ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-delete-insert/delete-insert-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "delete-insert: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-delete-insert/delete-insert-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "delete-insert: run delete+insert for date 2024-01-15",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-delete-insert/delete-insert-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_delete_insert"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "delete-insert: query updated table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, dt FROM test.products_delete_insert ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-delete-insert/delete-insert-pipeline/expectations/updated_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "delete-insert: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_delete_insert;"),
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
		},
		{
			name: "time-interval-materialization",
			workflow: e2e.Workflow{
				Name: "time-interval-materialization",
				Steps: []e2e.Task{
					{
						Name:    "time-interval: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-time-interval")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "time-interval: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-time-interval"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "time-interval: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline"), filepath.Join(tempDir, "test-time-interval")},
						WorkingDir: filepath.Join(tempDir, "test-time-interval"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time-interval: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_time_interval;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time-interval: create initial table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--start-date", "2024-01-01", "--end-date", "2024-01-31", filepath.Join(tempDir, "test-time-interval/time-interval-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_time_interval"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "time-interval: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, dt FROM test.products_time_interval ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-time-interval/time-interval-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "time-interval: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-time-interval/time-interval-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time-interval: run for specific time range",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--start-date", "2024-01-15", "--end-date", "2024-01-18", filepath.Join(tempDir, "test-time-interval/time-interval-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_time_interval"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "time-interval: query updated table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, dt FROM test.products_time_interval ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-time-interval/time-interval-pipeline/expectations/updated_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "time-interval: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_time_interval;"),
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
		},
		{
			name: "ddl-materialization",
			workflow: e2e.Workflow{
				Name: "ddl-materialization",
				Steps: []e2e.Task{
					{
						Name:    "ddl: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_ddl;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "ddl: confirm the table is dropped",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT * FROM test.products_ddl;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "ddl: create table with DDL strategy",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/ddl-pipeline/assets/products_ddl.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_ddl"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "ddl: verify table exists and is empty",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT COUNT(*) as count FROM test.products_ddl;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"count", "0"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "ddl: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_ddl;"),
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
		},
		{
			name: "scd2-by-column",
			workflow: e2e.Workflow{
				Name: "scd2-by-column",
				Steps: []e2e.Task{
					{
						Name:    "scd2-by-column: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-scd2-by-column")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-by-column: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-scd2-by-column"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-by-column: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline"), filepath.Join(tempDir, "test-scd2-by-column")},
						WorkingDir: filepath.Join(tempDir, "test-scd2-by-column"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-by-column: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.menu;"),
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
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.menu"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-column: query the initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/expectations/scd2_by_col_expected_initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-column: copy menu_updated_01.sql to menu.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/menu_updated_01.sql"), filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-by-column: run menu_updated_01.sql with SCD2 materialization",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.menu"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-column: query the updated table 01",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_01.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-column: copy menu_updated_02.sql to menu.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/menu_updated_02.sql"), filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-by-column: run menu_updated_02.sql with SCD2 materialization",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.menu"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-column: query the updated table 02",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_02.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-column: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.menu;"),
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
		},
		{
			name: "scd2-by-time",
			workflow: e2e.Workflow{
				Name: "scd2-by-time",
				Steps: []e2e.Task{
					{
						Name:    "scd2-by-time: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-scd2-by-time")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-by-time: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-scd2-by-time"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-by-time: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline"), filepath.Join(tempDir, "test-scd2-by-time")},
						WorkingDir: filepath.Join(tempDir, "test-scd2-by-time"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-by-time: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_scd2_time;"),
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
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_scd2_time"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-time: query the initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, stock, _is_current, _valid_from FROM test.products_scd2_time ORDER BY product_id, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/expectations/scd2_by_time_expected_initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-time: copy products_updated_01.sql to products.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/products_updated_01.sql"), filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-by-time: run products_updated_01.sql with SCD2 materialization",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_scd2_time"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-time: query the updated table 01",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, stock, _is_current, _valid_from FROM test.products_scd2_time ORDER BY product_id, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_01.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-time: copy products_updated_02.sql to products.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/products_updated_02.sql"), filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-by-time: run products_updated_02.sql with SCD2 materialization",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_scd2_time"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-time: query the updated table 02",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, stock, _is_current, _valid_from FROM test.products_scd2_time ORDER BY product_id, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_02.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-time: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_scd2_time;"),
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
		},
		{
			name: "view-materialization",
			workflow: e2e.Workflow{
				Name: "view-materialization",
				Steps: []e2e.Task{
					{
						Name:    "view: drop the view if exists",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP VIEW IF EXISTS test.products_view;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "view: create the view",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/view-pipeline/assets/products_view.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_view"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "view: query the view",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_view ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/view-pipeline/expectations/expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "view: run again to verify idempotency",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/view-pipeline/assets/products_view.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_view"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "view: query again to verify same results",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_view ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/view-pipeline/expectations/expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "view: drop the view",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP VIEW IF EXISTS test.products_view;"),
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
		},
		{
			name: "partition-materialization",
			workflow: e2e.Workflow{
				Name: "partition-materialization",
				Steps: []e2e.Task{
					{
						Name:    "partition: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_partitioned;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "partition: create partitioned table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/partition-pipeline/assets/products_partitioned.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_partitioned"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "partition: query the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, category FROM test.products_partitioned ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/partition-pipeline/expectations/expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "partition: verify partitioning exists",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DESCRIBE DETAIL test.products_partitioned;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"category"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "partition: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_partitioned;"),
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
		},
		{
			name: "time-interval-timestamp-materialization",
			workflow: e2e.Workflow{
				Name: "time-interval-timestamp-materialization",
				Steps: []e2e.Task{
					{
						Name:    "time-interval-ts: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-time-interval-ts")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "time-interval-ts: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-time-interval-ts"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "time-interval-ts: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/time-interval-timestamp-pipeline"), filepath.Join(tempDir, "test-time-interval-ts")},
						WorkingDir: filepath.Join(tempDir, "test-time-interval-ts"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time-interval-ts: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.events_time_interval;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time-interval-ts: create initial table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--start-date", "2024-01-01T00:00:00", "--end-date", "2024-01-02T23:59:59", filepath.Join(tempDir, "test-time-interval-ts/time-interval-timestamp-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.events_time_interval"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "time-interval-ts: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT event_id, event_name, user_id, event_timestamp FROM test.events_time_interval ORDER BY event_timestamp, event_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-time-interval-ts/time-interval-timestamp-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "time-interval-ts: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/time-interval-timestamp-pipeline/resources/events_updated.sql"), filepath.Join(tempDir, "test-time-interval-ts/time-interval-timestamp-pipeline/assets/events.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time-interval-ts: run for specific time range (Jan 2nd)",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--start-date", "2024-01-02T00:00:00", "--end-date", "2024-01-02T23:59:59", filepath.Join(tempDir, "test-time-interval-ts/time-interval-timestamp-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.events_time_interval"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "time-interval-ts: query updated table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT event_id, event_name, user_id, event_timestamp FROM test.events_time_interval ORDER BY event_timestamp, event_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-time-interval-ts/time-interval-timestamp-pipeline/expectations/updated_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "time-interval-ts: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.events_time_interval;"),
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
		},
		{
			name: "create-replace-materialization",
			workflow: e2e.Workflow{
				Name: "create-replace-materialization",
				Steps: []e2e.Task{
					{
						Name:    "create-replace: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempDir, "test-create-replace")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "create-replace: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "test-create-replace"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "create-replace: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/create-replace-pipeline"), filepath.Join(tempDir, "test-create-replace")},
						WorkingDir: filepath.Join(tempDir, "test-create-replace"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "create-replace: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_create_replace;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "create-replace: create initial table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-create-replace/create-replace-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_create_replace"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "create-replace: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_create_replace ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-create-replace/create-replace-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "create-replace: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/create-replace-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-create-replace/create-replace-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "create-replace: run with create+replace strategy (should fully replace)",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-create-replace/create-replace-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_create_replace"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "create-replace: query replaced table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price FROM test.products_create_replace ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(tempDir, "test-create-replace/create-replace-pipeline/expectations/replaced_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "create-replace: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_create_replace;"),
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
		},
		{
			name: "idempotency-merge-materialization",
			workflow: e2e.Workflow{
				Name: "idempotency-merge-materialization",
				Steps: []e2e.Task{
					{
						Name:    "idempotency: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_idempotency;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "idempotency: create initial table with merge",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/idempotency-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_idempotency"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "idempotency: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, stock FROM test.products_idempotency ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/idempotency-pipeline/expectations/expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "idempotency: run merge again (same data)",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/idempotency-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_idempotency"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "idempotency: verify same results after second run",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, stock FROM test.products_idempotency ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/idempotency-pipeline/expectations/expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "idempotency: run merge third time",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/idempotency-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products_idempotency"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "idempotency: verify same results after third run",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, stock FROM test.products_idempotency ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/idempotency-pipeline/expectations/expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "idempotency: verify row count is still 3",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT COUNT(*) as count FROM test.products_idempotency;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"count", "3"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "idempotency: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "DROP TABLE IF EXISTS test.products_idempotency;"),
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
		},
		{
			name: "error-merge-no-primary-key",
			workflow: e2e.Workflow{
				Name: "error-merge-no-primary-key",
				Steps: []e2e.Task{
					{
						Name:    "error-merge: run merge without primary key should fail",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/error-tests/assets/merge_no_primary_key.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"primary"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
				},
			},
		},
		{
			name: "error-delete-insert-no-incremental-key",
			workflow: e2e.Workflow{
				Name: "error-delete-insert-no-incremental-key",
				Steps: []e2e.Task{
					{
						Name:    "error-delete-insert: run without incremental key should fail",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/error-tests/assets/delete_insert_no_incremental_key.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"incremental"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
				},
			},
		},
		{
			name: "error-scd2-no-primary-key",
			workflow: e2e.Workflow{
				Name: "error-scd2-no-primary-key",
				Steps: []e2e.Task{
					{
						Name:    "error-scd2: run scd2 without primary key should fail",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/error-tests/assets/scd2_no_primary_key.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"primary"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
				},
			},
		},
		{
			name: "error-time-interval-no-incremental-key",
			workflow: e2e.Workflow{
				Name: "error-time-interval-no-incremental-key",
				Steps: []e2e.Task{
					{
						Name:    "error-time-interval: run without incremental key should fail",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/error-tests/assets/time_interval_no_incremental_key.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"incremental"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.workflow.Run()
			require.NoError(t, err, "Workflow %s failed: %v", tt.workflow.Name, err)

			t.Logf("Workflow '%s' completed successfully", tt.workflow.Name)
		})
	}
}

func TestDatabricksIndividualTasks(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	tasks := []e2e.Task{
		{
			Name:    "create the initial products individual table",
			Command: binary,
			Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products_individual.sql")),
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "query the products individual table",
			Command: binary,
			Args:    append(append([]string{"query"}, configFlags...), "--connection", "databricks-default", "--query", "SELECT product_id, product_name, price, stock FROM test.products_individual ORDER BY product_id;", "--output", "csv"),
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
	}

	for _, task := range tasks {
		t.Run(task.Name, func(t *testing.T) {
			t.Parallel()

			err := task.Run()
			require.NoError(t, err, "Task %s failed: %v", task.Name, err)

			t.Logf("Task '%s' completed successfully", task.Name)
		})
	}
}
