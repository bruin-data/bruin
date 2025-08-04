package postgres

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestPostgresWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		// {
		// 	name: "postgres-products-create-and-validate",
		// 	workflow: e2e.Workflow{
		// 		Name: "postgres-products-create-and-validate",
		// 		Steps: []e2e.Task{
		// 			{
		// 				Name:    "create the initial products table",
		// 				Command: binary,
		// 				Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "query the products table",
		// 				Command: binary,
		// 				Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM public.products ORDER BY PRODUCT_ID;", "--output", "csv"),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 					CSVFile:  filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/expected_products_table.csv"),
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 					e2e.AssertByCSV,
		// 				},
		// 			},
		// 		},
		// 	},
		// },
		// {
		// 	name: "scd2_by_column",
		// 	workflow: e2e.Workflow{
		// 		Name: "scd2_by_column",
		// 		Steps: []e2e.Task{
		// 			{
		// 				Name:    "scd2-by-column: restore menu asset to initial state",
		// 				Command: "cp",
		// 				Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/menu_original.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: create the initial table",
		// 				Command: binary,
		// 				Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline")),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: query the initial table",
		// 				Command: binary,
		// 				Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 					CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_initial.csv"),
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 					e2e.AssertByCSV,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: copy menu_updated_01.sql to menu.sql",
		// 				Command: "cp",
		// 				Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/menu_updated_01.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: run menu_updated_01.sql with SCD2 materialization",
		// 				Command: binary,
		// 				Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: query the updated table 01",
		// 				Command: binary,
		// 				Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 					CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_01.csv"),
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 					e2e.AssertByCSV,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: copy menu_updated_02.sql to menu.sql",
		// 				Command: "cp",
		// 				Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/menu_updated_02.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: run menu_updated_02.sql with SCD2 materialization",
		// 				Command: binary,
		// 				Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: query the updated table 02",
		// 				Command: binary,
		// 				Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 					CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_02.csv"),
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 					e2e.AssertByCSV,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: drop the table (expect error but table will be dropped)",
		// 				Command: binary,
		// 				Args:    []string{"query", "--config-file", filepath.Join(currentFolder, "../.bruin.cloud.yml"), "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "DROP TABLE IF EXISTS test.menu;"},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 1, // Expect failure due to "field descriptions not available for DDL statements" - specific to PostgresSQL driver
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode, // Assert that it fails as expected
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-column: confirm the table is dropped",
		// 				Command: binary,
		// 				Args:    []string{"query", "--config-file", filepath.Join(currentFolder, "../.bruin.cloud.yml"), "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT * FROM test.menu;"},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 1,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 		},
		// 	},
		// },
		// {
		// 	name: "SCD2 by time",
		// 	workflow: e2e.Workflow{
		// 		Name: "SCD2 by time",
		// 		Steps: []e2e.Task{
		// 			{
		// 				Name:    "scd2-by-time: restore products asset to initial state",
		// 				Command: "cp",
		// 				Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/products_original.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: create the initial products table",
		// 				Command: binary,
		// 				Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline")),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: query the initial table",
		// 				Command: binary,
		// 				Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 					CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_initial.csv"),
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 					e2e.AssertByCSV,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: copy products_updated_01.sql to products.sql",
		// 				Command: "cp",
		// 				Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/products_updated_01.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: run products_updated_01.sql with SCD2 materialization",
		// 				Command: binary,
		// 				Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: query the updated table 01",
		// 				Command: binary,
		// 				Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 					CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_01.csv"),
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 					e2e.AssertByCSV,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: copy products_updated_02.sql to products.sql",
		// 				Command: "cp",
		// 				Args:    []string{filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/resources/products_updated_02.sql"), filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: run products_updated_02.sql with SCD2 materialization",
		// 				Command: binary,
		// 				Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: query the updated table 02",
		// 				Command: binary,
		// 				Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 0,
		// 					CSVFile:  filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_02.csv"),
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 					e2e.AssertByCSV,
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: drop the table",
		// 				Command: binary,
		// 				Args:    []string{"query", "--config-file", filepath.Join(currentFolder, "../.bruin.cloud.yml"), "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "DROP TABLE IF EXISTS test.products;"},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 1, // Expect failure due to "field descriptions not available for DDL statements" - specific to PostgresSQL driver
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode, // Assert that it fails as expected
		// 				},
		// 			},
		// 			{
		// 				Name:    "scd2-by-time: confirm the table is dropped",
		// 				Command: binary,
		// 				Args:    []string{"query", "--config-file", filepath.Join(currentFolder, "../.bruin.cloud.yml"), "--asset", filepath.Join(currentFolder, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT * FROM test.products;"},
		// 				Env:     []string{},
		// 				Expected: e2e.Output{
		// 					ExitCode: 1,
		// 				},
		// 				Asserts: []func(*e2e.Task) error{
		// 					e2e.AssertByExitCode,
		// 				},
		// 			},
		// 		},
		// 	},
		// },
		{
			name: "postgres-metadata-push",
			workflow: e2e.Workflow{
					Name: "postgres-metadata-push",
					Steps: []e2e.Task{
						{
							Name:    "metadata-push: drop table if exists",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query", "DROP TABLE IF EXISTS test_metadata.sample_data;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "metadata-push: copy sample_data_original.sql to sample_data.sql",
							Command: "cp",
							Args:    []string{filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/resources/sample_data_original.sql"), filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/assets/sample_data.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "metadata-push: create the initial table",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/assets/sample_data.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "metadata-push: query the initial table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query","SELECT table_schema, table_name, column_name, col_description(c.oid, cols.ordinal_position) AS column_description FROM information_schema.columns AS cols JOIN pg_class AS c ON c.relname = cols.table_name WHERE cols.table_schema = 'test_metadata' AND cols.table_name = 'sample_data';", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/expectations/sample_data_metadata_original.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "metadata-push: copy sample_data_updated.sql to sample_data.sql",
							Command: "cp",
							Args:    []string{filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/resources/sample_data_updated.sql"), filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/assets/sample_data.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "metadata-push: run sample_data.sql with push-metadata",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--push-metadata", "--only", "push-metadata", "--env", "default", filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/assets/sample_data.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "metadata-push: query the updated metadata",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query", "SELECT table_schema, table_name, column_name, col_description(c.oid, cols.ordinal_position) AS column_description FROM information_schema.columns AS cols JOIN pg_class AS c ON c.relname = cols.table_name WHERE cols.table_schema = 'test_metadata' AND cols.table_name = 'sample_data';", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/expectations/sample_data_metadata_updated.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "metadata-push: check only metadata was pushed",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query", "SELECT * FROM test_metadata.sample_data;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/expectations/sample_data.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "metadata-push: restore sample_data to original state",
							Command: "cp",
							Args:    []string{filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/resources/sample_data_original.sql"), filepath.Join(currentFolder, "test-pipelines/metadata-push-pipeline/assets/sample_data.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
						},
						{
							Name:    "metadata-push: drop table if exists",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query", "DROP TABLE IF EXISTS test_metadata.sample_data;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "metadata-push: confirm the table is dropped",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query", "SELECT * FROM test_metadata.sample_data;"),
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

			// // Print args for each step in the workflow
			// for i, step := range tt.workflow.Steps {
			// 	fmt.Printf("Step %d: %s\n", i+1, step.Name)
			// 	fmt.Printf("Args: %v\n", step.Args)
			// }
			
			err := tt.workflow.Run()
			require.NoError(t, err, "Workflow %s failed: %v", tt.workflow.Name, err)

			t.Logf("Workflow '%s' completed successfully", tt.workflow.Name)
		})
	}
}

func TestPostgresIndividualTasks(t *testing.T) {
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
			Args:    append(append([]string{"query"}, configFlags...), "--connection", "postgres-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM public.products_individual ORDER BY PRODUCT_ID;", "--output", "csv"),
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
