package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestWorkflows(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	require.NoError(t, err, "Failed to get current working directory")

	projectRoot := filepath.Join(currentFolder, "../../")
	binary := filepath.Join(projectRoot, "bin/bruin")
	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

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

			// All platforms use tempDir structure (standardized on Postgres approach)
			// Define workflows for both scd2-by-column and scd2-by-time

			// SCD2-by-column setup
			scd2ByColumnTemplateDir := filepath.Join(currentFolder, "templates/scd2-by-column-pipeline")
			scd2ByColumnPipelineDir := filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline")
			scd2ByColumnAssetPath := filepath.Join(scd2ByColumnPipelineDir, "assets/menu.sql")
			scd2ByColumnExpectationsDir := filepath.Join(scd2ByColumnPipelineDir, "expectations")
			scd2ByColumnResourcesTemplateDir := filepath.Join(scd2ByColumnTemplateDir, "resources")

			// SCD2-by-time setup
			scd2ByTimeTemplateDir := filepath.Join(currentFolder, "templates/scd2-by-time-pipeline")
			scd2ByTimePipelineDir := filepath.Join(tempDir, "test-scd2-by-time/scd2-by-time-pipeline")
			scd2ByTimeAssetPath := filepath.Join(scd2ByTimePipelineDir, "assets/products.sql")
			scd2ByTimeExpectationsDir := filepath.Join(scd2ByTimePipelineDir, "expectations")
			scd2ByTimeResourcesTemplateDir := filepath.Join(scd2ByTimeTemplateDir, "resources")

			tests := []struct {
				name     string
				workflow e2e.Workflow
			}{
				{
					name: platform.Name + "-scd2-by-column",
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
								Name:    "scd2-by-column: generate pipeline from template",
								Command: "sh",
								Args:    []string{"-c", "true"}, // Placeholder command
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									func(task *e2e.Task) error {
										// Generate pipeline from template
										return generatePipelineFromTemplate(scd2ByColumnTemplateDir, scd2ByColumnPipelineDir, platform, platformName, "scd2-by-column-pipeline")
									},
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
									Contains: []string{platform.FinishedMessagePattern},
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
									CSVFile:  filepath.Join(scd2ByColumnExpectationsDir, "scd2_by_col_expected_initial.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy menu_updated_01.sql (copy from template and customize)
							{
								Name:    "scd2-by-column: copy menu_updated_01.sql from template",
								Command: "sh",
								Args:    []string{"-c", "true"}, // Placeholder command
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									func(task *e2e.Task) error {
										return copyResourceFile(filepath.Join(scd2ByColumnResourcesTemplateDir, "menu_updated_01.sql"), scd2ByColumnAssetPath, platform)
									},
								},
							},
							// Run menu_updated_01.sql
							{
								Name:    "scd2-by-column: run menu_updated_01.sql with SCD2 materialization",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--env", "default", scd2ByColumnAssetPath),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									Contains: []string{platform.FinishedMessagePattern},
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
									CSVFile:  filepath.Join(scd2ByColumnExpectationsDir, "scd2_by_col_expected_updated_01.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy menu_updated_02.sql (copy from template and customize)
							{
								Name:    "scd2-by-column: copy menu_updated_02.sql from template",
								Command: "sh",
								Args:    []string{"-c", "true"}, // Placeholder command
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									func(task *e2e.Task) error {
										return copyResourceFile(filepath.Join(scd2ByColumnResourcesTemplateDir, "menu_updated_02.sql"), scd2ByColumnAssetPath, platform)
									},
								},
							},
							// Run menu_updated_02.sql
							{
								Name:    "scd2-by-column: run menu_updated_02.sql with SCD2 materialization",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--env", "default", scd2ByColumnAssetPath),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									Contains: []string{platform.FinishedMessagePattern},
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
									CSVFile:  filepath.Join(scd2ByColumnExpectationsDir, "scd2_by_col_expected_updated_02.csv"),
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
					name: platform.Name + "-scd2-by-time",
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
								Name:    "scd2-by-time: generate pipeline from template",
								Command: "sh",
								Args:    []string{"-c", "true"}, // Placeholder command
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									func(task *e2e.Task) error {
										// Generate pipeline from template
										return generatePipelineFromTemplate(scd2ByTimeTemplateDir, scd2ByTimePipelineDir, platform, platformName, "scd2-by-time-pipeline")
									},
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
									CSVFile:  filepath.Join(scd2ByTimeExpectationsDir, "scd2_by_time_expected_initial.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy products_updated_01.sql (copy from template and customize)
							{
								Name:    "scd2-by-time: copy products_updated_01.sql from template",
								Command: "sh",
								Args:    []string{"-c", "true"}, // Placeholder command
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									func(task *e2e.Task) error {
										return copyResourceFile(filepath.Join(scd2ByTimeResourcesTemplateDir, "products_updated_01.sql"), scd2ByTimeAssetPath, platform)
									},
								},
							},
							// Run products_updated_01.sql
							{
								Name:    "scd2-by-time: run products_updated_01.sql with SCD2 materialization",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--env", "default", scd2ByTimeAssetPath),
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
									CSVFile:  filepath.Join(scd2ByTimeExpectationsDir, "scd2_by_time_expected_update_01.csv"),
								},
								Asserts: []func(*e2e.Task) error{
									e2e.AssertByExitCode,
									e2e.AssertByCSV,
								},
							},
							// Copy products_updated_02.sql (copy from template and customize)
							{
								Name:    "scd2-by-time: copy products_updated_02.sql from template",
								Command: "sh",
								Args:    []string{"-c", "true"}, // Placeholder command
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
								},
								Asserts: []func(*e2e.Task) error{
									func(task *e2e.Task) error {
										return copyResourceFile(filepath.Join(scd2ByTimeResourcesTemplateDir, "products_updated_02.sql"), scd2ByTimeAssetPath, platform)
									},
								},
							},
							// Run products_updated_02.sql
							{
								Name:    "scd2-by-time: run products_updated_02.sql with SCD2 materialization",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--env", "default", scd2ByTimeAssetPath),
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
									CSVFile:  filepath.Join(scd2ByTimeExpectationsDir, "scd2_by_time_expected_update_02.csv"),
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
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
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
	configFlags := []string{"--config-file", filepath.Join(projectRoot, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

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
