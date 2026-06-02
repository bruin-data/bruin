package dremio

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

const (
	connectionName = "dremio-default"
	// schema is the writable, Iceberg-capable Dremio source/space that the test
	// tables are created in. It must exist in the target Dremio instance (see
	// README.md). Change it here if your writable source has a different name.
	schema = "bruin_test"
)

// table returns a fully-quoted "schema"."name" identifier for use in queries.
func table(name string) string {
	return fmt.Sprintf("%q.%q", schema, name)
}

// dropTable is a best-effort cleanup/idempotency step (no asserts, so the exit
// code is ignored regardless of whether the table already exists).
func dropTable(binary string, configFlags []string, name string) e2e.Task {
	return e2e.Task{
		Name:    "drop " + name,
		Command: binary,
		Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--query", "DROP TABLE IF EXISTS "+table(name)),
		Env:     []string{},
	}
}

func TestDremioWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Join(currentFolder, "../../../")
	binary := filepath.Join(projectRoot, "bin/bruin")

	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}
	pipelines := filepath.Join(currentFolder, "test-pipelines")

	tempDir := t.TempDir()

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "connection-test",
			workflow: e2e.Workflow{
				Name: "connection-test",
				Steps: []e2e.Task{
					{
						Name:    "ping the dremio connection",
						Command: binary,
						Args:    append(append([]string{"connections"}, configFlags...), "test", "--name", connectionName),
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
			name: "checks-and-custom-checks",
			workflow: e2e.Workflow{
				Name: "checks-and-custom-checks",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "products"),
					{
						Name:    "create products with column and custom checks",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, "checks-pipeline/assets/products.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.products"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "query the products table",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", fmt.Sprintf("SELECT product_id, product_name FROM %s ORDER BY product_id", table("products"))),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Laptop", "Smartphone", "Headphones", "Monitor"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					dropTable(binary, configFlags, "products"),
				},
			},
		},
		{
			name: "failing-check-fails-the-run",
			workflow: e2e.Workflow{
				Name: "failing-check-fails-the-run",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "bad_price"),
					{
						Name:    "run asset whose positive check must fail",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, "failing-check-pipeline/assets/bad_price.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					dropTable(binary, configFlags, "bad_price"),
				},
			},
		},
		{
			name: "append",
			workflow: e2e.Workflow{
				Name: "append",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "events"),
					{
						Name:    "seed the events table (full refresh)",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, "append-pipeline/assets/events.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.events"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "append another batch",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(pipelines, "append-pipeline/assets/events.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.events"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "row count is doubled to 6",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", "SELECT COUNT(*) AS cnt FROM "+table("events")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"6"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					dropTable(binary, configFlags, "events"),
				},
			},
		},
		{
			name: "delete-insert",
			workflow: e2e.Workflow{
				Name: "delete-insert",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "inventory"),
					{
						Name:    "seed the inventory table (full refresh)",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, "incremental-pipeline/assets/inventory.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.inventory"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "re-run delete+insert (idempotent)",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(pipelines, "incremental-pipeline/assets/inventory.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.inventory"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "row count stays 3",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", "SELECT COUNT(*) AS cnt FROM "+table("inventory")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"3"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					dropTable(binary, configFlags, "inventory"),
				},
			},
		},
		{
			name: "truncate-insert",
			workflow: e2e.Workflow{
				Name: "truncate-insert",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "staging"),
					{
						Name:    "seed the staging table (full refresh)",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, "truncate-insert-pipeline/assets/staging.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.staging"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "re-run truncate+insert",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(pipelines, "truncate-insert-pipeline/assets/staging.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.staging"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "row count stays 4",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", "SELECT COUNT(*) AS cnt FROM "+table("staging")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"4"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					dropTable(binary, configFlags, "staging"),
				},
			},
		},
		{
			name: "view",
			workflow: e2e.Workflow{
				Name: "view",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "metrics_src"),
					{
						Name:    "run the source table and the view",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(pipelines, "view-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.metrics_src", "Finished: bruin_test.metrics_view"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "query the view (only rows with value > 6)",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", "SELECT metric_name FROM "+table("metrics_view")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"visits"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "drop the view",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--query", "DROP VIEW IF EXISTS "+table("metrics_view")),
						Env:     []string{},
					},
					dropTable(binary, configFlags, "metrics_src"),
				},
			},
		},
		{
			name: "ddl",
			workflow: e2e.Workflow{
				Name: "ddl",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "ddl_table"),
					{
						Name:    "create an empty table from a DDL definition",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(pipelines, "ddl-pipeline/assets/ddl_table.sql")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.ddl_table"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "the DDL table exists and is empty",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--output", "csv", "--query", "SELECT COUNT(*) AS cnt FROM "+table("ddl_table")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"0"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					dropTable(binary, configFlags, "ddl_table"),
				},
			},
		},
		{
			name: "query-sensor",
			workflow: e2e.Workflow{
				Name: "query-sensor",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "sensor_table"),
					{
						Name:    "run the sensor target table and the sensor",
						Command: binary,
						Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", "--sensor-mode", "wait", "--timeout", "30", filepath.Join(pipelines, "query-sensor-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"Finished: bruin_test.sensor"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					dropTable(binary, configFlags, "sensor_table"),
				},
			},
		},
		{
			name: "import-database",
			workflow: e2e.Workflow{
				Name: "import-database",
				Steps: []e2e.Task{
					dropTable(binary, configFlags, "import_src"),
					{
						Name:    "create a table to be discovered by import",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", connectionName, "--query", fmt.Sprintf("CREATE TABLE %s AS SELECT 1 AS id, 'x' AS label", table("import_src"))),
						Env:     []string{},
					},
					{
						Name:    "prepare a temp pipeline directory",
						Command: "cp",
						Args:    []string{"-a", filepath.Join(pipelines, "import-pipeline"), tempDir},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempDir, "import-pipeline"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "import the schema as assets",
						Command: binary,
						Args:    append(append([]string{"import"}, configFlags...), "database", "--connection", connectionName, "--schema", schema, filepath.Join(tempDir, "import-pipeline")),
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					dropTable(binary, configFlags, "import_src"),
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
