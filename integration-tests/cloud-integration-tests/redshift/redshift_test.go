package postgres

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestRedshiftWorkflows(t *testing.T) {
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
			name: "redshift-products-create-and-validate",
			workflow: e2e.Workflow{
				Name: "redshift-products-create-and-validate",
				Steps: []e2e.Task{
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM public.products ORDER BY PRODUCT_ID;", "--output", "csv"),
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
			name: "scd2_by_column",
			workflow: e2e.Workflow{
				Name: "scd2_by_column",
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_initial.csv"),
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_01.csv"),
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_02.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-column: drop the table (expect error but table will be dropped)",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS test.menu;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1, // Expect failure due to "field descriptions not available for DDL statements" - same as PostgreSQL
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode, // Assert that it fails as expected
						},
					},
					{
						Name:    "scd2-by-column: confirm the table is dropped",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT * FROM test.menu;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1, // Should fail because table doesn't exist
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
						Name:    "scd2-by-time: create the initial products table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-time: query the initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_initial.csv"),
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
						Name:    "scd2-by-time: update table with SCD2 materialization 1",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-by-time: query the updated table 01",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_01.csv"),
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
						Name:    "scd2-by-time: update table with SCD2 materialization 2",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: test.products"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "scd2-by-time: query the updated table 02",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_02.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-by-time: drop the table (expect error but table will be dropped)",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS test.products;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1, // Expect failure due to "field descriptions not available for DDL statements" - same as PostgreSQL
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode, // Assert that it fails as expected
						},
					},
					{
						Name:    "scd2-by-time: confirm the table is dropped",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT * FROM test.products;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1, // Should fail because table doesn't exist
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS dataset.datatable;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "table-sensor: confirm the table is dropped",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT * FROM dataset.datatable;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"relation", "does not exist"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "table-sensor: run the table sensor",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--sensor-mode", "wait", "--timeout", "10", filepath.Join(currentFolder, "test-pipelines/table-sensor-pipeline/assets/table_sensor.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"[dataset.sensor] Poking: dataset.datatable", "Failed: dataset.sensor"},
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
							Contains: []string{"Finished: dataset.datatable"},
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
							Contains: []string{"[dataset.sensor] Poking: dataset.datatable", "Finished: dataset.sensor"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "table-sensor: drop the table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS dataset.datatable;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
							Contains: []string{"field descriptions are not available"},
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
			name: "truncate-insert-materialization",
			workflow: e2e.Workflow{
				Name: "truncate-insert-materialization",
				Steps: []e2e.Task{
					{
						Name:    "truncate-insert: restore initial SQL",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/resources/products_initial.sql"), filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_truncate"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "truncate-insert: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price, stock FROM public.products_truncate ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "truncate-insert: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/resources/products_updated.sql"), filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_truncate"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "truncate-insert: query updated table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price, stock FROM public.products_truncate ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/truncate-insert-pipeline/expectations/updated_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "truncate-insert: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS public.products_truncate;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
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
						Name:    "append: restore initial SQL",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/append-pipeline/resources/products_initial.sql"), filepath.Join(currentFolder, "test-pipelines/append-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/append-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_append"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "append: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price FROM public.products_append ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/append-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "append: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/append-pipeline/resources/products_append.sql"), filepath.Join(currentFolder, "test-pipelines/append-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/append-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_append"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "append: query appended table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price FROM public.products_append ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/append-pipeline/expectations/appended_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "append: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS public.products_append;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
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
						Name:    "merge: restore initial SQL",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/merge-pipeline/resources/products_initial.sql"), filepath.Join(currentFolder, "test-pipelines/merge-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/merge-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_merge"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "merge: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price FROM public.products_merge ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/merge-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "merge: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/merge-pipeline/resources/products_updated.sql"), filepath.Join(currentFolder, "test-pipelines/merge-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/merge-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_merge"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "merge: query merged table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price FROM public.products_merge ORDER BY product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/merge-pipeline/expectations/merged_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "merge: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS public.products_merge;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
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
						Name:    "delete-insert: restore initial SQL",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/resources/products_initial.sql"), filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_delete_insert"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "delete-insert: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price, dt FROM public.products_delete_insert ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "delete-insert: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/resources/products_updated.sql"), filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_delete_insert"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "delete-insert: query updated table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price, dt FROM public.products_delete_insert ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/delete-insert-pipeline/expectations/updated_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "delete-insert: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS public.products_delete_insert;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
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
						Name:    "time-interval: restore initial SQL",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/resources/products_initial.sql"), filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--start-date", "2024-01-01", "--end-date", "2024-01-31", filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_time_interval"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "time-interval: query initial table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price, dt FROM public.products_time_interval ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/expectations/initial_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "time-interval: copy updated SQL to asset",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/resources/products_updated.sql"), filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/assets/products.sql")},
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
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--start-date", "2024-01-15", "--end-date", "2024-01-18", filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_time_interval"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "time-interval: query updated table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT product_id, product_name, price, dt FROM public.products_time_interval ORDER BY dt, product_id;", "--output", "csv"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/time-interval-pipeline/expectations/updated_expected.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "time-interval: drop table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS public.products_time_interval;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
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
						Name:    "ddl: create table with DDL strategy",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/ddl-pipeline/assets/products_ddl.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: public.products_ddl"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "ddl: verify table exists and is empty",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT COUNT(*) as count FROM public.products_ddl;", "--output", "csv"),
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
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "DROP TABLE IF EXISTS public.products_ddl;"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
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

func TestRedshiftIndividualTasks(t *testing.T) {
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
			Args:    append(append([]string{"query"}, configFlags...), "--connection", "redshift-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM public.products_individual ORDER BY PRODUCT_ID;", "--output", "csv"),
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
