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
			Args:    append(append([]string{"run"}, configFlags...), "--env", "default", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline")),
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

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "bigquery-products-create-and-validate",
			workflow: e2e.Workflow{
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
		},
		{
			name: "[bigquery] SCD2 by column workflow",
			workflow: e2e.Workflow{
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
		},
		{
			name: "[bigquery] SCD2 by time workflow",
			workflow: e2e.Workflow{
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
		},
		{
			name: "[bigquery] dry run asset cost estimation workflow",
			workflow: e2e.Workflow{
				Name: "[bigquery] dry run asset cost estimation workflow",
				Steps: []e2e.Task{
					{
						Name:    "dry-run-pipeline: create sample data table",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(currentFolder, "test-pipelines/dry-run-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "dry-run: estimate cost of querying the created asset",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/dry-run-pipeline/assets/sample_data.sql"), "--dry-run", "--output", "json"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							// Expected output should contain dry run metadata JSON for valid query with actual cost
							Contains: []string{
								"\"total_bytes_processed\":0",
								"\"total_bytes_billed\":0",
								"\"total_slot_ms\":0",
								"\"estimated_cost_usd\":0",
								"\"is_valid\":true",
							},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "dry-run: test cost estimation with custom query on asset",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--env", "default", "--asset", filepath.Join(currentFolder, "test-pipelines/dry-run-pipeline/assets/sample_data.sql"), "--query", "SELECT id, name FROM dataset.sample_data WHERE value > 200", "--dry-run", "--output", "json"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							// Expected output should contain dry run metadata JSON for custom query on real table
							Contains: []string{
								"\"total_bytes_processed\":225",
								"\"total_bytes_billed\":0",
								"\"total_slot_ms\":0",
								"\"estimated_cost_usd\":1.2789769243681803e-9",
								"\"is_valid\":true",
							},
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
			name: "[bigquery] dry run metadata workflow",
			workflow: e2e.Workflow{
				Name: "[bigquery] dry run metadata workflow",
				Steps: []e2e.Task{
					{
						Name:    "dry-run: validate JSON structure for non-existent table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--dry-run", "--query", "SELECT id, name, value, category FROM dataset.sample_data WHERE value > 200 ORDER BY id;", "--output", "json"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							// Expected output should contain dry run metadata JSON for invalid query (table doesn't exist)
							Contains: []string{
								"\"is_valid\":false",
								"\"total_bytes_processed\":0", // No data processed for invalid query
								"\"estimated_cost_usd\":0",    // No cost for invalid query
								"\"validation_error\":",       // Should contain validation error
								"Table",                       // Error message should mention table
							},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "dry-run: validate JSON structure for complex query with non-existent table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--dry-run", "--query", "WITH category_stats AS (SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM dataset.sample_data GROUP BY category) SELECT * FROM category_stats WHERE count > 2;", "--output", "json"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							// Expected output should contain dry run metadata JSON for invalid query (table doesn't exist)
							Contains: []string{
								"\"is_valid\":false",
								"\"total_bytes_processed\":0", // No data processed for invalid query
								"\"estimated_cost_usd\":0",    // No cost for invalid query
								"\"validation_error\":",       // Should contain validation error
							},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "dry-run: validate error handling for clearly invalid query",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", "gcp-default", "--dry-run", "--query", "SELECT * FROM nonexistent_table;", "--output", "json"),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0, // Should not fail, but return validation error in metadata
							// Expected output should contain validation error with zero cost
							Contains: []string{
								"\"is_valid\":false",
								"\"total_bytes_processed\":0", // No data processed for invalid query
								"\"estimated_cost_usd\":0",    // No cost for invalid query
								"\"validation_error\":",       // Should contain validation error
							},
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
