package bigquery

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/stretchr/testify/require"
)

func TestBigQueryIndividualTasks(t *testing.T) {
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
			Name:    "bigquery-run-pipeline",
			Command: binary,
			Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")),
			Env:     []string{},
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
			Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")),
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "dry-run-bad-asset-path",
			Command: binary,
			Args:    append(append([]string{"internal", "asset-metadata"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/fault-dry-run-pipeline/assets/non_existent_asset.sql")),
			Expected: e2e.Output{
				ExitCode: 1,
				Output:   "Please provide a valid asset path",
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "dry-run-non-bq-asset",
			Command: binary,
			Args:    append(append([]string{"internal", "asset-metadata"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/fault-dry-run-pipeline/assets/non_bq_asset.sql")),
			Expected: e2e.Output{
				ExitCode: 1,
				Output:   "asset-metadata is only available for BigQuery SQL assets",
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "dry-run-malformed-asset",
			Command: binary,
			Args:    append(append([]string{"internal", "asset-metadata"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/fault-dry-run-pipeline/assets/malformed_asset.sql")),
			Expected: e2e.Output{
				ExitCode: 1,
				Output:   "no query found in asset",
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "dry-run-empty-asset",
			Command: binary,
			Args:    append(append([]string{"internal", "asset-metadata"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/fault-dry-run-pipeline/assets/empty.sql")),
			Expected: e2e.Output{
				ExitCode: 1,
				Output:   "no query found in asset: empty.sql",
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
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

func TestBigQueryWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	tests := []struct {
		name     string
		workflow func(tempDir string, configFlags []string, binary string) e2e.Workflow
	}{
		{
			name: "bigquery-ddl-create-and-validate",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-ddl-create-and-validate",
					Steps: []e2e.Task{
						{
							Name:    "drop the initial DDL table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.ddl_drop_pipeline_ddl;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "create the initial DDL table",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/assets/ddl_table.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "move the ingest_data.sql to the assets folder",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/resources/ingest_data.sql"), filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/assets/ddl_table.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "run the ingest_data.sql",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/assets/ddl_table.sql")),
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
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT * FROM cloud_integration_test.ddl_drop_pipeline_ddl ORDER BY company", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/expectations/expect.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "move the ingest_data.sql to the assets folder",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/resources/ddl_table.sql"), filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/assets/ddl_table.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "run the ddl table with full refresh",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/assets/ddl_table.sql")),
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
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT * FROM cloud_integration_test.ddl_drop_pipeline_ddl ORDER BY company", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/ddl_drop_pipeline/expectations/expect.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "cleanup: drop DDL table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.ddl_drop_pipeline_ddl;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-products-create-and-validate",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-products-create-and-validate",
					Steps: []e2e.Task{
						{
							Name:    "create the initial products table",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/asset-query-pipeline/assets/products.sql")),
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
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK FROM cloud_integration_test.asset_query_products ORDER BY PRODUCT_ID;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/asset-query-pipeline/expected_products_table.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "cleanup: drop products table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.asset_query_products;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-merge-with-nulls",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-merge-with-nulls",
					Steps: []e2e.Task{
						{
							Name:    "create the initial table",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", filepath.Join(tempDir, "test-pipelines/nullable-pipeline/assets/nulltable.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "query the table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT * FROM cloud_integration_test.merge_with_nulls_nulltable ORDER BY id;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/nullable-pipeline/expectations/initial.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "copy nulltable_updated.sql to nulltable.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/nullable-pipeline/resources/nulltable_merge.sql"), filepath.Join(tempDir, "test-pipelines/nullable-pipeline/assets/nulltable.sql")},
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "update table with merge",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), filepath.Join(tempDir, "test-pipelines/nullable-pipeline/assets/nulltable.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "query the table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT * FROM cloud_integration_test.merge_with_nulls_nulltable ORDER BY id;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/nullable-pipeline/expectations/updated.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "cleanup: drop the table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.merge_with_nulls_nulltable;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-merge-materialization",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-merge-materialization",
					Steps: []e2e.Task{
						{
							Name:     "merge: drop the table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.merge_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "merge: create initial table",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/merge-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "merge: query initial table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price FROM cloud_integration_test.merge_materialization_products ORDER BY product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/merge-pipeline/expectations/initial_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "merge: copy updated SQL to asset",
							Command:  "cp",
							Args:     []string{filepath.Join(tempDir, "test-pipelines/merge-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-pipelines/merge-pipeline/assets/products.sql")},
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "merge: run merge strategy",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/merge-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "merge: query merged table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price FROM cloud_integration_test.merge_materialization_products ORDER BY product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/merge-pipeline/expectations/merged_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "cleanup: drop merge table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.merge_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "append-materialization",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "append-materialization",
					Steps: []e2e.Task{
						{
							Name:     "append: drop the table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.append_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "append: create initial table",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/append-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "append: query initial table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price FROM cloud_integration_test.append_materialization_products ORDER BY product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/append-pipeline/expectations/initial_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "append: copy updated SQL to asset",
							Command:  "cp",
							Args:     []string{filepath.Join(tempDir, "test-pipelines/append-pipeline/resources/products_append.sql"), filepath.Join(tempDir, "test-pipelines/append-pipeline/assets/products.sql")},
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "append: run append strategy",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/append-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "append: query appended table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price FROM cloud_integration_test.append_materialization_products ORDER BY product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/append-pipeline/expectations/appended_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "cleanup: drop append table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.append_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "truncate-insert-materialization",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "truncate-insert-materialization",
					Steps: []e2e.Task{
						{
							Name:     "truncate-insert: drop the table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.truncate_insert_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "truncate-insert: create initial table",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/truncate-insert-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "truncate-insert: query initial table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price, stock FROM cloud_integration_test.truncate_insert_materialization_products ORDER BY product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/truncate-insert-pipeline/expectations/initial_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "truncate-insert: copy updated SQL to asset",
							Command:  "cp",
							Args:     []string{filepath.Join(tempDir, "test-pipelines/truncate-insert-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-pipelines/truncate-insert-pipeline/assets/products.sql")},
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "truncate-insert: run truncate+insert strategy",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/truncate-insert-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "truncate-insert: query updated table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price, stock FROM cloud_integration_test.truncate_insert_materialization_products ORDER BY product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/truncate-insert-pipeline/expectations/updated_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "cleanup: drop truncate table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.truncate_insert_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "delete-insert-materialization",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "delete-insert-materialization",
					Steps: []e2e.Task{
						{
							Name:     "delete-insert: drop the table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.delete_insert_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "delete-insert: create initial table",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/delete-insert-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "delete-insert: query initial table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price, dt FROM cloud_integration_test.delete_insert_materialization_products ORDER BY dt, product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/delete-insert-pipeline/expectations/initial_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "delete-insert: copy updated SQL to asset",
							Command:  "cp",
							Args:     []string{filepath.Join(tempDir, "test-pipelines/delete-insert-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-pipelines/delete-insert-pipeline/assets/products.sql")},
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "delete-insert: run delete+insert",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/delete-insert-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "delete-insert: query updated table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price, dt FROM cloud_integration_test.delete_insert_materialization_products ORDER BY dt, product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/delete-insert-pipeline/expectations/updated_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "cleanup: drop delete-insert table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.delete_insert_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "time-interval-materialization",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "time-interval-materialization",
					Steps: []e2e.Task{
						{
							Name:     "time-interval: drop the table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.time_interval_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "time-interval: create initial table",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--start-date", "2024-01-01", "--end-date", "2024-01-31", filepath.Join(tempDir, "test-pipelines/time-interval-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "time-interval: query initial table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price, dt FROM cloud_integration_test.time_interval_materialization_products ORDER BY dt, product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/time-interval-pipeline/expectations/initial_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "time-interval: copy updated SQL to asset",
							Command:  "cp",
							Args:     []string{filepath.Join(tempDir, "test-pipelines/time-interval-pipeline/resources/products_updated.sql"), filepath.Join(tempDir, "test-pipelines/time-interval-pipeline/assets/products.sql")},
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "time-interval: run for specific time range",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", "--start-date", "2024-01-15", "--end-date", "2024-01-18", filepath.Join(tempDir, "test-pipelines/time-interval-pipeline")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "time-interval: query updated table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT product_id, product_name, price, dt FROM cloud_integration_test.time_interval_materialization_products ORDER BY dt, product_id;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, CSVFile: filepath.Join(tempDir, "test-pipelines/time-interval-pipeline/expectations/updated_expected.csv")},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByCSV},
						},
						{
							Name:     "cleanup: drop time-interval table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.time_interval_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "table-sensor",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "table-sensor",
					Steps: []e2e.Task{
						{
							Name:     "table-sensor: drop the table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.table_sensor_datatable;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "table-sensor: run the table sensor (expect failure)",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", "--sensor-mode", "wait", "--timeout", "10", filepath.Join(tempDir, "test-pipelines/table-sensor-pipeline/assets/table_sensor.sql")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 1, Contains: []string{"Poking: cloud_integration_test.table_sensor_datatable", "Failed: cloud_integration_test.table_sensor_sensor"}},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByContains},
						},
						{
							Name:     "table-sensor: create the table",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/table-sensor-pipeline/assets/create_table.sql")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, Contains: []string{"Finished: cloud_integration_test.table_sensor_datatable"}},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByContains},
						},
						{
							Name:     "table-sensor: run the table sensor (expect success)",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", "--sensor-mode", "wait", "--timeout", "20", filepath.Join(tempDir, "test-pipelines/table-sensor-pipeline/assets/table_sensor.sql")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, Contains: []string{"Poking: cloud_integration_test.table_sensor_datatable", "Finished: cloud_integration_test.table_sensor_sensor"}},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByContains},
						},
						{
							Name:     "cleanup: drop sensor table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.table_sensor_datatable;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "ddl-materialization",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "ddl-materialization",
					Steps: []e2e.Task{
						{
							Name:     "ddl: drop the table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.ddl_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "ddl: confirm the table is dropped",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT * FROM cloud_integration_test.ddl_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 1},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
						{
							Name:     "ddl: create table with DDL strategy",
							Command:  binary,
							Args:     append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/ddl-pipeline/assets/products_ddl.sql")),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, Contains: []string{"Finished: cloud_integration_test.ddl_materialization_products"}},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByContains},
						},
						{
							Name:     "ddl: verify table exists and is empty",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT COUNT(*) as count FROM cloud_integration_test.ddl_materialization_products;", "--output", "csv"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0, Contains: []string{"count", "0"}},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode, e2e.AssertByContains},
						},
						{
							Name:     "cleanup: drop table",
							Command:  binary,
							Args:     append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.ddl_materialization_products;"),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "bigquery-scd2-by-column",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-scd2-by-column",
					Steps: []e2e.Task{
						{
							Name:    "scd2-by-column: drop table if exists",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "DROP TABLE IF EXISTS cloud_integration_test.scd2_by_column_menu;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: restore menu asset to initial state",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/menu_original.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
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
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline")),
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
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM cloud_integration_test.scd2_by_column_menu ORDER BY ID, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_initial.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-column: copy menu_updated_01.sql to menu.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/menu_updated_01.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
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
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: query the updated table 01",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM cloud_integration_test.scd2_by_column_menu ORDER BY ID, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_01.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-column: copy menu_updated_02.sql to menu.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/menu_updated_02.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")},
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
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: query the updated table 02",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/assets/menu.sql"), "--query", "SELECT ID, Name, Price, _is_current FROM cloud_integration_test.scd2_by_column_menu ORDER BY ID, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_02.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-column: drop the table (expect error but table will be dropped)",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.scd2_by_column_menu;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-column: confirm the table is dropped",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT * FROM cloud_integration_test.scd2_by_column_menu;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1, // Should fail because table doesn't exist
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-scd2-by-time",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-scd2-by-time",
					Steps: []e2e.Task{
						{
							Name:    "scd2-by-time: drop table if exists",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "DROP TABLE IF EXISTS cloud_integration_test.scd2_by_time_products;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: restore products asset to initial state",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/products_original.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
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
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: query the initial table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM cloud_integration_test.scd2_by_time_products ORDER BY product_id, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_initial.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-time: copy products_updated_01.sql to products.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/products_updated_01.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
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
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: query the updated table 01",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM cloud_integration_test.scd2_by_time_products ORDER BY product_id, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_01.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-time: copy products_updated_02.sql to products.sql",
							Command: "cp",
							Args:    []string{filepath.Join(tempDir, "test-pipelines/scd2-pipelines/resources/products_updated_02.sql"), filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")},
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
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: query the updated table 02",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/assets/products.sql"), "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM cloud_integration_test.scd2_by_time_products ORDER BY product_id, _valid_from;", "--output", "csv"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/scd2-pipelines/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_02.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "scd2-by-time: drop the table (expect error but table will be dropped)",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.scd2_by_time_products;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "scd2-by-time: confirm the table is dropped",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "SELECT * FROM cloud_integration_test.scd2_by_time_products;"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 1, // Should fail because table doesn't exist
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-dry-run-pipeline",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-dry-run-pipeline",
					Steps: []e2e.Task{
						{
							Name:    "dry-run-pipeline: run the asset",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), filepath.Join(tempDir, "test-pipelines/dry-run-pipeline/assets/dry_run_table.sql")),
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:          "dry-run-pipeline: query the asset metadata",
							Command:       binary,
							Args:          append(append([]string{"internal", "asset-metadata"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/dry-run-pipeline/assets/select.sql")),
							SkipJSONNodes: []string{`"ProjectID"`},
							Expected: e2e.Output{
								ExitCode: 0,
								Output:   helpers.ReadFile(filepath.Join(tempDir, "test-pipelines/dry-run-pipeline/expectations/results.json")),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByOutputJSON,
							},
						},
						{
							Name:          "dry-run-pipeline: query the sensor metadata",
							Command:       binary,
							Args:          append(append([]string{"internal", "asset-metadata"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/dry-run-pipeline/assets/select_sensor.asset.yml")),
							SkipJSONNodes: []string{`"ProjectID"`},
							Expected: e2e.Output{
								ExitCode: 0,
								Output:   helpers.ReadFile(filepath.Join(tempDir, "test-pipelines/dry-run-pipeline/expectations/results.json")),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByOutputJSON,
							},
						},
						{
							Name:    "cleanup: drop the table",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", "DROP TABLE IF EXISTS cloud_integration_test.dry_run_table;"),
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
					},
				}
			},
		},
		{
			name: "bigquery-drop-on-mismatch",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "bigquery-drop-on-mismatch",
					Steps: []e2e.Task{
						{
							Name:    "drop-on-mismatch: drop schema if exists",
							Command: binary,
							Args: append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", `
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_compose;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_01;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_02;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_trunc_01;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_trunc_02;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_trunc_03;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_01;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_02;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_03;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_04;
							`),
							Env: []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "drop-on-mismatch: run the pipeline",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--start-date", "2024-01-01", "--end-date", "2024-01-02", filepath.Join(tempDir, "test-pipelines/drop-on-mismatch-pipeline")),
							Expected: e2e.Output{
								ExitCode: 0,
								Contains: []string{"Interval: 2024-01-01T00:00:00Z - 2024-01-02T00:00:00Z", "Assets executed", "11 succeeded"},
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByContains,
							},
						},
						{
							Name:    "drop-on-mismatch: run the pipeline with full refresh",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--start-date", "2025-01-01", "--end-date", "2025-01-02", "--env", "default", filepath.Join(tempDir, "test-pipelines/drop-on-mismatch-pipeline")),
							Expected: e2e.Output{
								ExitCode: 0,
								Contains: []string{"Successfully"},
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByContains,
							},
						},
						{
							Name:    "drop-on-mismatch: query the schema to check that no new tables were created	",
							Command: binary,
							Args: append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", `
								SELECT COUNT(*) as tables_with_2025_data
								FROM (
									SELECT 'drop_on_mismatch_compose' as table_name, COUNT(*) as cnt FROM cloud_integration_test.drop_on_mismatch_compose WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_date_01', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_date_01 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_date_02', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_date_02 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_date_trunc_01', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_date_trunc_01 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_date_trunc_02', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_date_trunc_02 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_date_trunc_03', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_date_trunc_03 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_ts', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_ts WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_ts_truncate_01', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_ts_truncate_01 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_ts_truncate_02', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_ts_truncate_02 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_ts_truncate_03', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_ts_truncate_03 WHERE created_at = '2025-01-01'
									UNION ALL
									SELECT 'drop_on_mismatch_ts_truncate_04', COUNT(*) FROM cloud_integration_test.drop_on_mismatch_ts_truncate_04 WHERE created_at = '2025-01-01'
								)
								WHERE cnt > 0;
							`, "--output", "csv"),
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/drop-on-mismatch-pipeline/expectations/drop_on_mismatch_expected.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "cleanup: drop bq_test tables",
							Command: binary,
							Args: append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--query", `
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_compose;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_01;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_02;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_trunc_01;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_trunc_02;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_date_trunc_03;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_01;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_02;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_03;
								DROP TABLE IF EXISTS cloud_integration_test.drop_on_mismatch_ts_truncate_04;
							`),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
		{
			name: "merge-sql-pipeline",
			workflow: func(tempDir string, configFlags []string, binary string) e2e.Workflow {
				return e2e.Workflow{
					Name: "merge-sql-pipeline",
					Steps: []e2e.Task{
						{
							Name:    "merge-sql-pipeline: bootstrap initial data",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--asset", filepath.Join(tempDir, "test-pipelines/merge-sql-pipeline/assets/bootstrap_table.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "merge-sql-pipeline: run merge asset",
							Command: binary,
							Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-pipelines/merge-sql-pipeline/assets/merge_asset.sql")),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
							},
						},
						{
							Name:    "merge-sql-pipeline: validate merged result",
							Command: binary,
							Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--output", "csv", "--query", "SELECT pk, col_a, col_b, col_c, col_d FROM cloud_integration_test.merge_sql_target_table ORDER BY pk"),
							Env:     []string{},
							Expected: e2e.Output{
								ExitCode: 0,
								CSVFile:  filepath.Join(tempDir, "test-pipelines/merge-sql-pipeline/expectations/target_table.csv"),
							},
							Asserts: []func(*e2e.Task) error{
								e2e.AssertByExitCode,
								e2e.AssertByCSV,
							},
						},
						{
							Name:    "cleanup: drop merge-sql tables",
							Command: binary,
							Args: append(append([]string{"query"}, configFlags...),
								"--connection", "gcp-default",
								"--query", `
									DROP TABLE IF EXISTS cloud_integration_test.merge_sql_initial_data;
									DROP TABLE IF EXISTS cloud_integration_test.merge_sql_updated_source;
									DROP TABLE IF EXISTS cloud_integration_test.merge_sql_target_table;
								`,
							),
							Env:      []string{},
							Expected: e2e.Output{ExitCode: 0},
							Asserts:  []func(*e2e.Task) error{e2e.AssertByExitCode},
						},
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tempDir := t.TempDir()

			srcPipelines := filepath.Join(currentFolder, "test-pipelines")
			destPipelines := filepath.Join(tempDir, "test-pipelines")
			err := copyDir(srcPipelines, destPipelines)
			require.NoError(t, err, "Failed to copy test-pipelines to tempDir")
			runGitInitInTempPipelines(t, tempDir)

			configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}
			workflow := tt.workflow(tempDir, configFlags, binary)
			err = workflow.Run()
			require.NoError(t, err, "Workflow %s failed: %v", workflow.Name, err)
			t.Logf("Workflow '%s' completed successfully", workflow.Name)
		})
	}
}

func copyDir(src string, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(dst, relPath)
		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		dstFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		defer dstFile.Close()
		_, err = io.Copy(dstFile, srcFile)
		return err
	})
}

func runGitInitInTempPipelines(t *testing.T, tempDir string) {
	gitDir := filepath.Join(tempDir, "test-pipelines")
	cmd := exec.Command("git", "init")
	cmd.Dir = gitDir
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to run 'git init' in %s: %s", gitDir, string(output))
}
