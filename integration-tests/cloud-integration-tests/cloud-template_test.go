package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/stretchr/testify/require"
)

func TestTemplatedSCD2ByColumn(t *testing.T) {
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
			// Generate pipeline from templates
			templateDir := filepath.Join(currentFolder, "templates/scd2-by-column-pipeline")
			pipelineDir := filepath.Join(tempDir, "test-scd2-by-column/scd2-by-column-pipeline")
			assetPath := filepath.Join(pipelineDir, "assets/menu.sql")
			expectationsDir := filepath.Join(pipelineDir, "expectations")
			resourcesTemplateDir := filepath.Join(templateDir, "resources")

			// Build query command - all platforms use --connection flag
			buildQueryArgs := func(query string) []string {
				args := append([]string{"query"}, configFlags...)
				args = append(args, "--connection", platform.Connection, "--query", query, "--output", "csv")
				return args
			}

			// Build drop table query command - uses config file with connection (like Postgres)
			buildDropTableArgs := func() []string {
				query := "DROP TABLE IF EXISTS " + platform.SchemaPrefix + ".menu;"
				return append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", query)
			}

			// Build select table query command - uses config file with connection (like Postgres)
			buildSelectTableArgs := func() []string {
				query := "SELECT * FROM " + platform.SchemaPrefix + ".menu;"
				return append(append([]string{"query"}, configFlags...), "--connection", platform.Connection, "--query", query)
			}

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
								Args:    buildDropTableArgs(),
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
										return generatePipelineFromTemplate(templateDir, pipelineDir, platform, platformName)
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
								Args:    buildQueryArgs("SELECT ID, Name, Price, _is_current FROM " + platform.SchemaPrefix + ".menu ORDER BY ID, _valid_from;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(expectationsDir, "scd2_by_col_expected_initial.csv"),
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
										return copyResourceFile(filepath.Join(resourcesTemplateDir, "menu_updated_01.sql"), assetPath, platform)
									},
								},
							},
							// Run menu_updated_01.sql
							{
								Name:    "scd2-by-column: run menu_updated_01.sql with SCD2 materialization",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--env", "default", assetPath),
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
								Args:    buildQueryArgs("SELECT ID, Name, Price, _is_current FROM " + platform.SchemaPrefix + ".menu ORDER BY ID, _valid_from;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(expectationsDir, "scd2_by_col_expected_updated_01.csv"),
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
										return copyResourceFile(filepath.Join(resourcesTemplateDir, "menu_updated_02.sql"), assetPath, platform)
									},
								},
							},
							// Run menu_updated_02.sql
							{
								Name:    "scd2-by-column: run menu_updated_02.sql with SCD2 materialization",
								Command: binary,
								Args:    append(append([]string{"run"}, configFlags...), "--env", "default", assetPath),
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
								Args:    buildQueryArgs("SELECT ID, Name, Price, _is_current FROM " + platform.SchemaPrefix + ".menu ORDER BY ID, _valid_from;"),
								Env:     []string{},
								Expected: e2e.Output{
									ExitCode: 0,
									CSVFile:  filepath.Join(expectationsDir, "scd2_by_col_expected_updated_02.csv"),
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
								Args:    buildDropTableArgs(),
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
								Args:    buildSelectTableArgs(),
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
