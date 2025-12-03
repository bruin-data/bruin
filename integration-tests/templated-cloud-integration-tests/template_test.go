package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

// buildAdditionalWorkflows creates workflow definitions for tests beyond scd2-by-column and scd2-by-time
// This function can be expanded to add more workflow types as templates are created
func buildAdditionalWorkflows(platform PlatformConfig, platformName string, tempDir string, currentFolder string, binary string, configFlags []string, testAvailability map[string][]string, isTestAvailable func(string, string) bool) []struct {
	name     string
	workflow e2e.Workflow
} {
	var additionalTests []struct {
		name     string
		workflow e2e.Workflow
	}

	// For now, we'll add placeholder workflows that can be expanded
	// Each test type will need its own template structure similar to scd2-by-column

	// TODO: Add implementations for:
	// - ddl-create-and-validate
	// - products-create-and-validate
	// - merge-with-nulls
	// - dry-run (BigQuery only)
	// - drop-on-mismatch
	// - merge-sql

	// These will require creating templates similar to scd2-by-column-pipeline
	// For now, we return empty slice - tests can be added incrementally

	return additionalTests
}

func TestWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	projectRoot := filepath.Join(currentFolder, "../../")
	binary := filepath.Join(projectRoot, "bin/bruin")
	configFlags := []string{"--config-file", filepath.Join(currentFolder, ".bruin.cloud.yml")}

	// Get available platforms from cloud config
	configPath := filepath.Join(currentFolder, ".bruin.cloud.yml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Cloud configuration file not found - skipping templated cloud integration tests")
		return
	}

	availablePlatforms, err := getAvailablePlatforms(configPath)
	require.NoError(t, err, "Failed to parse cloud configuration")

	// Test platforms in order: postgres, snowflake, bigquery
	testPlatforms := []string{"postgres", "snowflake", "bigquery"}

	for _, platformName := range testPlatforms {
		platformName := platformName // capture loop variable
		t.Run(platformName, func(t *testing.T) {
			t.Parallel()

			// Each platform gets its own temp directory
			tempDir := t.TempDir()

			// Check if platform is available
			if !availablePlatforms[platformName] {
				t.Skipf("Skipping %s tests - no connection configured", platformName)
				return
			}

			// Get platform config
			platform, ok := GetPlatformConfig(platformName)
			if !ok {
				t.Fatalf("Platform config not found for: %s", platformName)
			}

			tests := []struct {
				name               string
				workflow           e2e.Workflow
				availablePlatforms []string // List of platforms where this test is available
			}{
				{
					name:               platform.Name + "-scd2-by-column",
					availablePlatforms: []string{"postgres", "snowflake", "bigquery"},
					workflow: e2e.Workflow{
						Name: platform.Name + "-scd2-by-column",
						Steps: []e2e.Task{
							// All platforms have an initial drop table step
							{
								Name:    "scd2-by-column: drop table if exists",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "DROP TABLE IF EXISTS test.menu;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: platform.DropTableExitCode,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Setup steps - all platforms use tempDir (standardized on Postgres)
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
								Args:       []string{"-a", filepath.Join(currentFolder, "templates/scd2-by-column-pipeline"), filepath.Join(tempDir, "test-scd2-by-column")},
								WorkingDir: filepath.Join(tempDir, "test-scd2-by-column"),
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:    "scd2-by-column: patch pipeline.yml with platform defaults",
								Command: binary,
								Args: []string{
									"internal", "patch-pipeline",
									filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/pipeline.yml"),
									"--body", fmt.Sprintf(`{"default_connections":{"%s":"%s"},"default":{"type":"%s"}}`, platform.PlatformConnection, platform.Connection, platform.AssetType),
								},
								Env: []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Create initial table
							{
								Name:    "scd2-by-column: create the initial table",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline")),
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
							// Query initial table
							{
								Name:    "scd2-by-column: query the initial table",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT id, name, price, _is_current FROM test.menu ORDER BY id, _valid_from;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/scd2-by-column-pipeline/scd2_by_col_expected_initial.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy menu_updated_01.sql (copy from template)
							{
								Name:    "scd2-by-column: copy menu_updated_01.sql to menu.sql",
								Command: "cp",
								Args:    []string{filepath.Join(currentFolder, "templates/scd2-by-column-pipeline/resources/menu_updated_01.sql"), filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")},
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Run menu_updated_01.sql
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
							// Query updated table 01
							{
								Name:    "scd2-by-column: query the updated table 01",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT id, name, price, _is_current FROM test.menu ORDER BY id, _valid_from;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/scd2-by-column-pipeline/scd2_by_col_expected_updated_01.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy menu_updated_02.sql (copy from template)
							{
								Name:    "scd2-by-column: copy menu_updated_02.sql to menu.sql",
								Command: "cp",
								Args:    []string{filepath.Join(currentFolder, "templates/scd2-by-column-pipeline/resources/menu_updated_02.sql"), filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")},
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Run menu_updated_02.sql
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
							// Query updated table 02
							{
								Name:    "scd2-by-column: query the updated table 02",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT id, name, price, _is_current FROM test.menu ORDER BY id, _valid_from;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/scd2-by-column-pipeline/scd2_by_col_expected_updated_02.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Drop table
							{
								Name:    "scd2-by-column: drop the table (expect error but table will be dropped)",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "DROP TABLE IF EXISTS test.menu;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: platform.DropTableExitCode,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Confirm table is dropped
							{
								Name:    "scd2-by-column: confirm the table is dropped",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT * FROM test.menu;"),
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
					name:               platform.Name + "-scd2-by-time",
					availablePlatforms: []string{"postgres", "snowflake", "bigquery"},
					workflow: e2e.Workflow{
						Name: platform.Name + "-scd2-by-time",
						Steps: []e2e.Task{
							// All platforms have an initial drop table step
							{
								Name:    "scd2-by-time: drop table if exists",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "DROP TABLE IF EXISTS test.products;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: platform.DropTableExitCode,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Setup steps - all platforms use tempDir (standardized on Postgres)
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
								Args:       []string{"-a", filepath.Join(currentFolder, "templates/scd2-by-time-pipeline"), filepath.Join(tempDir, "test-scd2-by-time")},
								WorkingDir: filepath.Join(tempDir, "test-scd2-by-time"),
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:    "scd2-by-time: patch pipeline.yml with platform defaults",
								Command: binary,
								Args: []string{
									"internal", "patch-pipeline",
									filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/pipeline.yml"),
									"--body", fmt.Sprintf(`{"default_connections":{"%s":"%s"},"default":{"type":"%s"}}`, platform.PlatformConnection, platform.Connection, platform.AssetType),
								},
								Env: []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Create initial table
							{
								Name:    "scd2-by-time: create the initial table",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline")),
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
							// Query initial table
							{
								Name:    "scd2-by-time: query the initial table",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT product_id, product_name, stock, _is_current, _valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/scd2-by-time-pipeline/scd2_by_time_expected_initial.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy products_updated_01.sql (copy from template)
							{
								Name:    "scd2-by-time: copy products_updated_01.sql to products.sql",
								Command: "cp",
								Args:    []string{filepath.Join(currentFolder, "templates/scd2-by-time-pipeline/resources/products_updated_01.sql"), filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")},
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Run products_updated_01.sql
							{
								Name:    "scd2-by-time: run products_updated_01.sql with SCD2 materialization",
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
							// Query updated table 01
							{
								Name:    "scd2-by-time: query the updated table 01",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT product_id, product_name, stock, _is_current, _valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/scd2-by-time-pipeline/scd2_by_time_expected_update_01.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy products_updated_02.sql (copy from template)
							{
								Name:    "scd2-by-time: copy products_updated_02.sql to products.sql",
								Command: "cp",
								Args:    []string{filepath.Join(currentFolder, "templates/scd2-by-time-pipeline/resources/products_updated_02.sql"), filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")},
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Run products_updated_02.sql
							{
								Name:    "scd2-by-time: run products_updated_02.sql with SCD2 materialization",
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
							// Query updated table 02
							{
								Name:    "scd2-by-time: query the updated table 02",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT product_id, product_name, stock, _is_current, _valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/scd2-by-time-pipeline/scd2_by_time_expected_update_02.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Drop table
							{
								Name:    "scd2-by-time: drop the table (expect error but table will be dropped)",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "DROP TABLE IF EXISTS test.products;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: platform.DropTableExitCode,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							// Confirm table is dropped
							{
								Name:    "scd2-by-time: confirm the table is dropped",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT * FROM test.products;"),
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
				// BigQuery-specific test: merge-with-nulls
				{
					name:               platform.Name + "-merge-with-nulls",
					availablePlatforms: []string{"bigquery"},
					workflow: e2e.Workflow{
						Name: platform.Name + "-merge-with-nulls",
						Steps: []e2e.Task{
							{
								Name:    "merge-with-nulls: create test directory",
								Command: "mkdir",
								Args:    []string{"-p", filepath.Join(tempDir, "test-nullable-pipeline")},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:       "merge-with-nulls: initialize git repository",
								Command:    "git",
								Args:       []string{"init"},
								WorkingDir: filepath.Join(tempDir, "test-nullable-pipeline"),
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:       "merge-with-nulls: copy pipeline files",
								Command:    "cp",
								Args:       []string{"-a", filepath.Join(currentFolder, "templates/nullable-pipeline"), filepath.Join(tempDir, "test-nullable-pipeline")},
								WorkingDir: filepath.Join(tempDir, "test-nullable-pipeline"),
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:    "merge-with-nulls: patch pipeline.yml with platform defaults",
								Command: binary,
								Args: []string{
									"internal", "patch-pipeline",
									filepath.Join(tempDir, "test-nullable-pipeline/nullable-pipeline/pipeline.yml"),
									"--body", fmt.Sprintf(`{"default_connections":{"%s":"%s"},"default":{"type":"%s"}}`, platform.PlatformConnection, platform.Connection, platform.AssetType),
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
								Name:    "merge-with-nulls: create the initial table",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--full-refresh", "--env", "default", filepath.Join(tempDir, "test-nullable-pipeline/nullable-pipeline/assets/nulltable.sql")),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:    "merge-with-nulls: query the initial table",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT * FROM dataset.nulltable ORDER BY id;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/nullable-pipeline/initial.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							{
								Name:    "merge-with-nulls: copy nulltable_merge.sql to nulltable.sql",
								Command: "cp",
								Args:    []string{filepath.Join(tempDir, "test-nullable-pipeline/nullable-pipeline/resources/nulltable_merge.sql"), filepath.Join(tempDir, "test-nullable-pipeline/nullable-pipeline/assets/nulltable.sql")},
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:    "merge-with-nulls: update table with merge",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--env", "default", filepath.Join(tempDir, "test-nullable-pipeline/nullable-pipeline/assets/nulltable.sql")),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
								},
							},
							{
								Name:    "merge-with-nulls: query the updated table",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT * FROM dataset.nulltable ORDER BY id;", "--output", "csv"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(currentFolder, "templates/expectations/nullable-pipeline/updated.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							{
								Name:    "merge-with-nulls: drop the table",
								Command: binary,
								Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "DROP TABLE IF EXISTS dataset.nulltable;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: platform.DropTableExitCode,
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
				tt := tt // capture loop variable
				t.Run(tt.name, func(t *testing.T) {
					// Skip test if not available for this platform
					available := false
					for _, p := range tt.availablePlatforms {
						if p == platformName {
							available = true
							break
						}
					}
					if !available {
						t.Skipf("Skipping %s - not available for platform %s", tt.name, platformName)
						return
					}

					err := tt.workflow.Run()
					require.NoError(t, err, "Workflow %s failed: %v", tt.workflow.Name, err)

					t.Logf("Workflow '%s' completed successfully", tt.workflow.Name)
				})
			}
		})
	}
}

func TestPingConnections(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	projectRoot := filepath.Join(currentFolder, "../../")
	binary := filepath.Join(projectRoot, "bin/bruin")
	configFlags := []string{"--config-file", filepath.Join(currentFolder, ".bruin.cloud.yml")}

	// Get available platforms from cloud config
	configPath := filepath.Join(currentFolder, ".bruin.cloud.yml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Cloud configuration file not found - skipping connection ping tests")
		return
	}

	availablePlatforms, err := getAvailablePlatforms(configPath)
	require.NoError(t, err, "Failed to parse cloud configuration")

	// Test all configured platforms
	testPlatforms := []string{"postgres", "snowflake", "bigquery", "athena"}

	for _, platformName := range testPlatforms {
		platformName := platformName // capture loop variable
		t.Run(platformName, func(t *testing.T) {
			t.Parallel()

			// Check if platform is available
			if !availablePlatforms[platformName] {
				t.Skipf("Skipping %s - no connection configured", platformName)
				return
			}

			// Get platform config
			platform, ok := GetPlatformConfig(platformName)
			if !ok {
				t.Fatalf("Platform config not found for: %s", platformName)
			}

			// Ping connection with SELECT 1
			workflow := e2e.Workflow{
				Name: platform.Name + "-ping",
				Steps: []e2e.Task{
					{
						Name:    "ping connection",
						Command: binary,
						Args:    append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", "SELECT 1;"),
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

			err := workflow.Run()
			require.NoError(t, err, "Failed to ping connection for %s: %v", platformName, err)

			t.Logf("Successfully pinged connection for %s", platformName)
		})
	}
}
