package main_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

// loadPipelineState loads a pipeline state from a JSON expectation file.
// It replaces the __RUNTIME_OS__ placeholder with the actual runtime OS.
func loadPipelineState(currentFolder, filename string) *scheduler.PipelineState {
	filePath := filepath.Join(currentFolder, "test-pipelines/continue-pipeline/expectations", filename)
	content, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	// Replace the runtime OS placeholder with the actual OS
	contentStr := strings.ReplaceAll(string(content), "__RUNTIME_OS__", runtime.GOOS)

	var state scheduler.PipelineState
	if err := json.Unmarshal([]byte(contentStr), &state); err != nil {
		panic(err)
	}

	return &state
}

func cleanupDuckDBFiles(t *testing.T) {
	duckdbFilesDir := "duckdb-files"

	if err := os.RemoveAll(duckdbFilesDir); err != nil {
		t.Fatalf("Failed to remove duckdb-files directory: %v", err)
	}

	if err := os.MkdirAll(duckdbFilesDir, 0755); err != nil {
		t.Fatalf("Failed to create duckdb-files directory: %v", err)
	}
}

//nolint:maintidx,paralleltest
func TestIndividualTasks(t *testing.T) {
	cleanupDuckDBFiles(t)

	// Check if parallel execution is enabled via environment variable
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tests := []struct {
		name string
		task e2e.Task
	}{
		{
			name: "builtin-policies",
			task: e2e.Task{
				Name:    "builtin-policies",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-builtin")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated 2 assets across 1 pipeline"},
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "custom-policies",
			task: e2e.Task{
				Name:    "custom-policies",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-custom")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated 1 assets across 1 pipeline"},
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "policy-selector",
			task: e2e.Task{
				Name:    "policy-selector",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-selector")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated 1 assets across 1 pipeline"},
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "policy-non-compliance",
			task: e2e.Task{
				Name:    "policy-non-compliance",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-non-compliant")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Output:   "Checked 1 pipeline and found 3 issues",
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "policy-validate-single-asset",
			task: e2e.Task{
				Name:    "policy-validate-single-asset",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-validate-single-asset/assets/target.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "policy-variables",
			task: e2e.Task{
				Name:    "policy-variables",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-variables/assets/target.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "policy-variables-fail",
			task: e2e.Task{
				Name:    "policy-variables-fail",
				Command: binary,
				Args:    []string{"validate", "--var", `message="This should fail"`, filepath.Join(currentFolder, "test-pipelines/policies-variables/assets/target.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "parse-whole-pipeline",
			task: e2e.Task{
				Name:          "parse-whole-pipeline",
				Command:       binary,
				Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline/expectations/pipeline.yml.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "validate-happy-path",
			task: e2e.Task{
				Name:    "validate-happy-path",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/happy-path")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "policy-variables",
			task: e2e.Task{
				Name:    "policy-variables",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/policies-variables/assets/target.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				WorkingDir: currentFolder,
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "parse-whole-pipeline",
			task: e2e.Task{
				Name:          "parse-whole-pipeline",
				Command:       binary,
				Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline/expectations/pipeline.yml.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "render-variables",
			task: e2e.Task{
				Name:          "render-variables",
				Command:       binary,
				Args:          []string{"render", filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"CREATE TABLE public.users", "SELECT 'jhon' as name", "SELECT 'erik' as name"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-variables-override-json",
			task: e2e.Task{
				Name:    "render-variables-override-json",
				Command: binary,
				Args: []string{
					"render",
					"--var", `{"users": ["mark", "nicholas"]}`,
					filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"CREATE TABLE public.users", "SELECT 'mark' as name", "SELECT 'nicholas' as name"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-variables-override-key-val",
			task: e2e.Task{
				Name:    "render-variables-override-key-val",
				Command: binary,
				Args: []string{
					"render",
					"--var", `users=["mark", "nicholas"]`,
					filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"CREATE TABLE public.users", "SELECT 'mark' as name", "SELECT 'nicholas' as name"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-start-date-flag",
			task: e2e.Task{
				Name:    "render-start-date-flag",
				Command: binary,
				Args: []string{
					"render",
					"--start-date", "2024-01-15",
					"--end-date", "2024-01-31",
					"--output", "json",
					filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/assets/date_capture.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{`'2024-01-15'`, `'2024-01-31'`},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-full-refresh-uses-pipeline-start-date",
			task: e2e.Task{
				Name:    "render-full-refresh-uses-pipeline-start-date",
				Command: binary,
				Args: []string{
					"render",
					"--full-refresh",
					"--start-date", "2024-01-15",
					"--end-date", "2024-01-31",
					"--output", "json",
					filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/assets/date_capture.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{`'2023-06-15'`, `'2024-01-31'`},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-full-refresh-no-start-flag-uses-pipeline-start-date",
			task: e2e.Task{
				Name:    "render-full-refresh-no-start-flag-uses-pipeline-start-date",
				Command: binary,
				Args: []string{
					"render",
					"--full-refresh",
					"--end-date", "2024-12-31",
					"--output", "json",
					filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/assets/date_capture.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{`'2023-06-15'`, `'2024-12-31'`},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-is-full-refresh-false",
			task: e2e.Task{
				Name:    "render-is-full-refresh-false",
				Command: binary,
				Args: []string{
					"render",
					"--start-date", "2024-01-15",
					"--end-date", "2024-01-31",
					filepath.Join(currentFolder, "test-pipelines/render-template-this-pipeline/assets/test_full_refresh.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"'render_this.test_full_refresh' AS asset_name",
						"'INCREMENTAL_MODE' AS refresh_mode",
						"'2024-01-15' AS start_date",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-is-full-refresh-true",
			task: e2e.Task{
				Name:    "render-is-full-refresh-true",
				Command: binary,
				Args: []string{
					"render",
					"--full-refresh",
					"--start-date", "2024-01-15",
					"--end-date", "2024-01-31",
					filepath.Join(currentFolder, "test-pipelines/render-template-this-pipeline/assets/test_full_refresh.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"'render_this.test_full_refresh' AS asset_name",
						"'FULL_REFRESH_MODE' AS refresh_mode",
						"'2020-01-01' AS start_date",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-asset-level-start-date-no-start-date",
			task: e2e.Task{
				Name:    "render-asset-level-start-date-no-start-date",
				Command: binary,
				Args: []string{
					"render",
					"--full-refresh",
					"--end-date", "2024-12-31",
					filepath.Join(currentFolder, "test-pipelines/asset-level-start-date-test/assets/asset_no_start_date.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"'2023-01-01' as captured_start_date", "'2024-12-31' as captured_end_date"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-asset-level-start-date-with-start-date",
			task: e2e.Task{
				Name:    "render-asset-level-start-date-with-start-date",
				Command: binary,
				Args: []string{
					"render",
					"--full-refresh",
					"--end-date", "2024-12-31",
					filepath.Join(currentFolder, "test-pipelines/asset-level-start-date-test/assets/asset_with_start_date.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"'2024-06-01' as captured_start_date", "'2024-12-31' as captured_end_date"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-ddl-basic-duckdb",
			task: e2e.Task{
				Name:    "render-ddl-basic-duckdb",
				Command: binary,
				Args: []string{
					"render-ddl",
					filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-ddl/assets/schema.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"CREATE TABLE IF NOT EXISTS test.customers",
						"customer_id INTEGER",
						"name VARCHAR",
						"email VARCHAR",
						"created_at TIMESTAMP",
						"PRIMARY KEY (customer_id)",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-ddl-with-dates",
			task: e2e.Task{
				Name:    "render-ddl-with-dates",
				Command: binary,
				Args: []string{
					"render-ddl",
					"--start-date", "2024-01-15",
					"--end-date", "2024-01-31",
					filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/assets/date_capture.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"CREATE TABLE IF NOT EXISTS date_capture",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "render-ddl-json-output",
			task: e2e.Task{
				Name:    "render-ddl-json-output",
				Command: binary,
				Args: []string{
					"render-ddl",
					"--output", "json",
					filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-ddl/assets/schema.sql")},
				Env: []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"CREATE TABLE IF NOT EXISTS test.customers",
						"customer_id INTEGER",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "python-happy-path-run",
			task: e2e.Task{
				Name:    "python-happy-path-run",
				Command: binary,
				Args: []string{
					"run",
					filepath.Join(currentFolder, "test-pipelines/happy-path/assets/happy.py"),
				},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "python-variable-injection",
			task: e2e.Task{
				Name:    "python-variable-injection",
				Command: binary,
				Args: []string{
					"run",
					filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/load.py"),
				},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"env: dev", "users: jhon,erik"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "python-variable-injection-with-override",
			task: e2e.Task{
				Name:    "python-variable-injection-with-override",
				Command: binary,
				Args: []string{
					"run",
					"--var", `env="prod"`, `--var`, `{"users": ["sakamoto", "shin"]}`,
					filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/load.py"),
				},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"env: prod", "users: sakamoto,shin"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-with-tags",
			task: e2e.Task{
				Name:    "run-with-tags",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-with-tags", "--tag", "include", "--exclude-tag", "exclude", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-with-tags-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "query-asset",
			task: e2e.Task{
				Name:    "query-asset",
				Command: binary,
				Args:    []string{"query", "--env", "env-query-asset", "--output", "json", "--asset", filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/assets/products.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/asset-query-pipeline/expected.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "query-export",
			task: e2e.Task{
				Name:    "query-export",
				Command: binary,
				Args:    []string{"query", "--env", "env-query-export", "--output", "json", "--asset", filepath.Join(currentFolder, "test-pipelines/query-export-pipeline/assets/products.sql"), "--export"},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/query-export-pipeline/expected.csv")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByQueryResultCSV,
				},
			},
		},
		{
			name: "run-with-filters",
			task: e2e.Task{
				Name:    "run-with-filters",
				Command: binary,
				Args:    []string{"run", "-env", "env-run-with-filters", "--tag", "include", "--exclude-tag", "exclude", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-with-filters-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"bruin run completed", "Finished: shipping_provider", "Finished: products", "Finished: products:price:positive"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "format-if-fail",
			task: e2e.Task{
				Name:    "format-if-fail",
				Command: binary,
				Args:    []string{"format", "--fail-if-changed", filepath.Join(currentFolder, "test-pipelines/format-if-changed-pipeline/assets/correctly-formatted.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "run-main-with-filters",
			task: e2e.Task{
				Name:    "run-main-with-filters",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-main-with-filters", "--tag", "include", "--exclude-tag", "exclude", "--only", "main", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-main-with-filters-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"bruin run completed", "Finished: shipping_provider", "Finished: products"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-with-downstream",
			task: e2e.Task{
				Name:    "run-with-downstream",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-with-downstream", "--downstream", filepath.Join(currentFolder, "test-pipelines/run-with-downstream-pipeline/assets/products.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"bruin run completed", "Finished: products", "Finished: products:price:positive", "Finished: product_price_summary", "Finished: product_price_summary:product_count:non_negative", "Finished: product_price_summary:total_stock:non_negative"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-main-with-downstream",
			task: e2e.Task{
				Name:    "run-main-with-downstream",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-main-with-downstream", "--downstream", "--only", "main", filepath.Join(currentFolder, "test-pipelines/run-main-with-downstream-pipeline/assets/products.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"bruin run completed", "Finished: products", "Finished: product_price_summary"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "push-metadata",
			task: e2e.Task{
				Name:    "push-metadata",
				Command: binary,
				Args:    []string{"run", "--env", "env-push-metadata", "--push-metadata", "--only", "push-metadata", filepath.Join(currentFolder, "test-pipelines/push-metadata-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Running:  shopify_raw.products:metadata-push", "Running:  shopify_raw.inventory_items:metadata-push"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-happy-path",
			task: e2e.Task{
				Name:    "validate-happy-path",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/happy-path")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "validate-with-exclude-tags",
			task: e2e.Task{
				Name:    "validate-with-exclude-tags",
				Command: binary,
				Args:    []string{"validate", "--exclude-tag", "exclude", filepath.Join(currentFolder, "test-pipelines/validate-with-exclude-tag")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{" Successfully validated 4 assets across 1 pipeline, all good."},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-custom-check-count-false",
			task: e2e.Task{
				Name:    "run-custom-check-count-false",
				Command: binary,
				Args:    []string{"run", "--env", "env-custom-check-count-false", filepath.Join(currentFolder, "test-pipelines/custom-check-count-false")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{"custom check 'row_count' has returned 4 instead of the expected 7"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-custom-check-count-true",
			task: e2e.Task{
				Name:    "run-custom-check-count-true",
				Command: binary,
				Args:    []string{"run", "--env", "env-custom-check-count-true", filepath.Join(currentFolder, "test-pipelines/custom-check-count-true")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "parse-asset-happy-path-asset-py",
			task: e2e.Task{
				Name:          "parse-asset-happy-path-asset-py",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/asset.py")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/asset.py.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-happy-path-chess-games",
			task: e2e.Task{
				Name:          "parse-asset-happy-path-chess-games",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/chess_games.asset.yml")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/chess_games.asset.yml.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-happy-path-chess-profiles",
			task: e2e.Task{
				Name:          "parse-asset-happy-path-chess-profiles",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/chess_profiles.asset.yml")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/chess_profiles.asset.yml.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-happy-path-player-summary",
			task: e2e.Task{
				Name:          "parse-asset-happy-path-player-summary",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/player_summary.sql")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/player_summary.sql.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-faulty-pipeline-error-sql",
			task: e2e.Task{
				Name:    "parse-asset-faulty-pipeline-error-sql",
				Command: binary,
				Args:    []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/faulty-pipeline/assets/error.sql")},
				Env:     []string{},

				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{"error creating asset from file", "unmarshal errors"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-missing-upstream",
			task: e2e.Task{
				Name:          "validate-missing-upstream",
				Command:       binary,
				Args:          []string{"validate", "-o", "json", filepath.Join(currentFolder, "test-pipelines/missing-upstream-pipeline/assets/nonexistent.sql")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/missing-upstream-pipeline/expectations/missing_upstream.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "run-malformed-sql",
			task: e2e.Task{
				Name:    "run-malformed-sql",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-malformed-sql", filepath.Join(currentFolder, "test-pipelines/run-malformed-pipeline/assets/malformed.sql")},
				Env:     []string{},

				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{"Parser Error: syntax error at or near \"S_ELECT_\"", "1 failed"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "internal-connections",
			task: e2e.Task{
				Name:          "internal-connections",
				Command:       binary,
				Args:          []string{"internal", "connections"},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "expectations/expected_connections_schema.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "connections-list",
			task: e2e.Task{
				Name:          "connections-list",
				Command:       binary,
				Args:          []string{"connections", "list", "-o", "json", currentFolder},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "expectations/expected_connections.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-lineage",
			task: e2e.Task{
				Name:          "parse-lineage",
				Command:       binary,
				Args:          []string{"internal", "parse-pipeline", "-c", filepath.Join(currentFolder, "test-pipelines/parse-lineage-pipeline")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-lineage-pipeline/expectations/lineage.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-lineage",
			task: e2e.Task{
				Name:          "parse-asset-lineage",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", "-c", filepath.Join(currentFolder, "test-pipelines/parse-asset-lineage-pipeline/assets/example.sql")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-asset-lineage-pipeline/expectations/lineage-asset.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-seed-data",
			task: e2e.Task{
				Name:          "parse-asset-seed-data",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/run-seed-data/expectations/seed.asset.yml.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-default-option-pipeline",
			task: e2e.Task{
				Name:          "parse-asset-default-option-pipeline",
				Command:       binary,
				Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-default-option")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/pipeline.yml.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-default-option-asset-py",
			task: e2e.Task{
				Name:          "parse-asset-default-option-asset-py",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-default-option/assets/asset.py")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/asset.py.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-default-option-chess-games",
			task: e2e.Task{
				Name:          "parse-asset-default-option-chess-games",
				Command:       binary,
				Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-default-option/assets/chess_games.asset.yml")},
				Env:           []string{},
				SkipJSONNodes: []string{"\"path\"", "\"extends\""},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/chess_games.asset.yml.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "parse-asset-extends",
			task: e2e.Task{
				Name:          "parse-asset-extends",
				Command:       binary,
				Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-asset-extends")},
				Env:           []string{},
				SkipJSONNodes: []string{`"path"`, `"extends"`, `"commit"`, `"snapshot"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-asset-extends/expectations/pipeline.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "run-non-wait-symbolic",
			task: e2e.Task{
				Name:    "run-non-wait-symbolic",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-non-wait-symbolic", filepath.Join(currentFolder, "test-pipelines/run-non-wait-symbolic")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{"Running:  example", "Finished: example", "Catalog Error: Table with name my does not exist!", "Failed: my-other-asset"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "test-render-template-this",
			task: e2e.Task{
				Name:    "test-render-template-this",
				Command: binary,
				Args:    []string{"run", "--env", "env-render-template-this", filepath.Join(currentFolder, "test-pipelines/render-template-this-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated 3 assets", "bruin run completed", "Finished: render_this.my_asset_2"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "test-ddl-duckdb",
			task: e2e.Task{
				Name:    "test-ddl-duckdb",
				Command: binary,
				Args:    []string{"run", "--env", "env-duckdb-ddl", filepath.Join(currentFolder, "test-pipelines/duckdb-ddl-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated 2 assets", "bruin run completed", "Finished: my_schema.table_check"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "test-truncate-insert-validate",
			task: e2e.Task{
				Name:       "test-truncate-insert-validate",
				Command:    binary,
				Args:       []string{"validate", filepath.Join(currentFolder, "test-pipelines/duckdb-truncate-insert-validate")},
				WorkingDir: currentFolder,
				Env:        []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"Successfully validated 1 asset",
						"all good",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "skip-python-assets-without-bruin-header",
			task: e2e.Task{
				Name:    "skip-python-assets-without-bruin-header",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/empty-py-asset")},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "query-duckdb-decimal-return-types",
			task: e2e.Task{
				Name:          "query-duckdb-decimal-return-types",
				Command:       binary,
				Args:          []string{"query", "--env", "env-duckdb-decimal", "--asset", filepath.Join(currentFolder, "test-pipelines/duckdb-decimal-pipeline/assets/simple_decimal_test.sql"), "--output", "json"},
				Env:           []string{},
				SkipJSONNodes: []string{`"connectionName"`, `"query"`},
				Expected: e2e.Output{
					ExitCode: 0,
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/duckdb-decimal-pipeline/expectations/expected.json")),
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByOutputJSON,
				},
			},
		},
		{
			name: "validate-asset-time-interval",
			task: e2e.Task{
				Name:    "validate-asset-time-interval",
				Command: binary,
				Args:    []string{"validate", "--env", "env-validate-asset-time-interval", filepath.Join(currentFolder, "test-pipelines/validate-asset-time-interval")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{
						"start date", "is after end date", "for asset invalid_jinja.example",
						"start date", "is after end date", "for asset invalid_modifiers.example",
						"Checked 1 pipeline and found", "2 issues",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-asset-start-date-invalid",
			task: e2e.Task{
				Name:    "validate-asset-start-date-invalid",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/validate-asset-start-date")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{
						"start_date must be in the format of YYYY-MM-DD in the asset definition, 'notvalid' given",
						"valid-asset-start-date",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-asset-start-date-valid-asset",
			task: e2e.Task{
				Name:    "validate-asset-start-date-valid-asset",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/validate-asset-start-date/assets/valid_date.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated", "valid_date.sql", "all good"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-asset-start-date-invalid-single-asset",
			task: e2e.Task{
				Name:    "validate-asset-start-date-invalid-single-asset",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/validate-asset-start-date/assets/invalid_date.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{
						"start_date must be in the format of YYYY-MM-DD in the asset definition, 'notvalid' given",
						"valid-asset-start-date",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-asset-start-date-invalid-pipeline",
			task: e2e.Task{
				Name:    "run-asset-start-date-invalid-pipeline",
				Command: binary,
				Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/validate-asset-start-date")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{
						"start_date must be in the format of YYYY-MM-DD in the asset definition, 'notvalid' given",
						"found",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-asset-start-date-invalid-single-asset",
			task: e2e.Task{
				Name:    "run-asset-start-date-invalid-single-asset",
				Command: binary,
				Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/validate-asset-start-date/assets/invalid_date.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 1,
					Contains: []string{
						"start_date must be in the format of YYYY-MM-DD in the asset definition, 'notvalid' given",
						"found",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-single-non-bq-asset-no-adc-check",
			task: e2e.Task{
				Name:    "run-single-non-bq-asset-no-adc-check",
				Command: binary,
				Args:    []string{"run", "--env", "env-adc-filter", filepath.Join(currentFolder, "test-pipelines/adc-filter-pipeline/assets/duckdb_asset.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"Finished: duckdb_products",
						"bruin run completed",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-with-tag-excludes-bq-no-adc-check",
			task: e2e.Task{
				Name:    "run-with-tag-excludes-bq-no-adc-check",
				Command: binary,
				Args:    []string{"run", "--env", "env-adc-filter", "--tag", "duckdb", filepath.Join(currentFolder, "test-pipelines/adc-filter-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"Finished: duckdb_products",
						"Finished: duckdb_categories",
						"bruin run completed",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-with-exclude-tag-bq-no-adc-check",
			task: e2e.Task{
				Name:    "run-with-exclude-tag-bq-no-adc-check",
				Command: binary,
				Args:    []string{"run", "--env", "env-adc-filter", "--exclude-tag", "bigquery", filepath.Join(currentFolder, "test-pipelines/adc-filter-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						"Finished: duckdb_products",
						"Finished: duckdb_categories",
						"bruin run completed",
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check if parallel execution is enabled via environment variable
			if os.Getenv("ENABLE_PARALLEL") == "1" {
				t.Parallel()
			}
			err := tt.task.Run()
			require.NoError(t, err, "Task %s failed: %v", tt.task.Name, err)
		})
	}
}

//nolint:paralleltest
func TestWorkflowTasks(t *testing.T) {
	cleanupDuckDBFiles(t)

	// Check if parallel execution is enabled via environment variable
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tempdir := t.TempDir()

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "continue_after_failure",
			workflow: e2e.Workflow{
				Name: "continue_after_failure",
				Steps: []e2e.Task{
					{
						Name:    "continue: copy shipping_providers_broken.sql to assets folder",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/continue-pipeline/resources/shipping_providers_broken.sql"), filepath.Join(currentFolder, "test-pipelines/continue-pipeline/assets/shipping_providers.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "continue: run first time",
						Command: binary,
						Args:    []string{"run", "--start-date", "2024-01-01", "--end-date", "2024-12-31", "--env", "env-continue", filepath.Join(currentFolder, "test-pipelines/continue-pipeline")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 1,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), loadPipelineState(currentFolder, "continue_pipeline_state_first_run.json")),
						},
					},
					{
						Name:    "continue: copy shipping_providers_corrected.sql to shipping_providers.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/continue-pipeline/resources/shipping_providers_corrected.sql"), filepath.Join(currentFolder, "test-pipelines/continue-pipeline/assets/shipping_providers.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "continue: run continue",
						Command: binary,
						Args:    []string{"run", "--start-date", "2024-01-01", "--end-date", "2024-12-31", "--env", "env-continue", "--continue", filepath.Join(currentFolder, "test-pipelines/continue-pipeline")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), loadPipelineState(currentFolder, "continue_pipeline_state_continue_run.json")),
						},
					},
					{
						Name:    "continue: delete shipping_providers.sql",
						Command: "rm",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/continue-pipeline/assets/shipping_providers.sql")},
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
			name: "bruin_init",
			workflow: e2e.Workflow{
				Name: "bruin_init",
				Steps: []e2e.Task{
					{
						Name:    "bruin_init: create a test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempdir, "test-bruin-init")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "bruin_init: run git init",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "bruin_init: run bruin init",
						Command:    binary,
						Args:       []string{"init", "clickhouse"},
						WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "expectations/expected_bruin.yaml")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByYAML,
						},
					},
					{
						Name:       "bruin_init: run bruin init 2",
						Command:    binary,
						Args:       []string{"init", "clickhouse", "clickhouse2"},
						WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "expectations/expected_bruin.yaml")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByYAML,
						},
					},
					{
						Name:       "bruin_init: run bruin init chess",
						Command:    binary,
						Args:       []string{"init", "chess"},
						WorkingDir: filepath.Join(tempdir, "test-bruin-init"),
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "expectations/expected_bruin_chess.yaml")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByYAML,
						},
					},
				},
			},
		},
		{
			name: "time_materialization",
			workflow: e2e.Workflow{
				Name: "time_materialization",
				Steps: []e2e.Task{
					{
						Name:    "time_materialization: create the table",
						Command: binary,
						Args:    []string{"run", "--full-refresh", "--env", "env-time-materialization", filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time_materialization: query the initial table",
						Command: binary,
						Args:    []string{"query", "--env", "env-time-materialization", "--asset", filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql"), "--query", "SELECT * FROM PRODUCTS;", "--output", "json"},
						Env:     []string{},

						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/expectations/initial_expected.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
					{
						Name:    "time_materialization: update the table with time materialization",
						Command: binary,
						Args:    []string{"run", "--env", "env-time-materialization", "--start-date", "2025-03-01", "--end-date", "2025-03-31", filepath.Join(currentFolder, "test-pipelines/time-materialization-update-pipeline/assets/products_updated.sql")},
						Env:     []string{},

						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "time_materialization: query the updated table with time materialization",
						Command: binary,
						Args:    []string{"query", "--env", "env-time-materialization", "--asset", filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql"), "--query", "SELECT * FROM PRODUCTS;", "--output", "json"},
						Env:     []string{},

						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/expectations/final_expected.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
				},
			},
		},
		{
			name: "run_pipeline_with_nameless_asset",
			workflow: e2e.Workflow{
				Name: "run_pipeline_with_nameless_asset",
				Steps: []e2e.Task{
					{
						Name:    "nameless_asset: create the table",
						Command: binary,
						Args:    []string{"run", "--env", "env-run-nameless-asset", filepath.Join(currentFolder, "test-pipelines/run-nameless-asset-pipeline")},
						Env:     []string{},

						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "nameless_asset: query the table",
						Command: binary,
						Args:    []string{"query", "--env", "env-run-nameless-asset", "--asset", filepath.Join(currentFolder, "test-pipelines/run-nameless-asset-pipeline/assets/test2/shipping_providers.sql"), "--query", "SELECT * FROM test2.shipping_providers", "--output", "json"},
						Env:     []string{},

						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/run-nameless-asset-pipeline/expected.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
				},
			},
		},
		{
			name: "interval_modifiers",
			workflow: e2e.Workflow{
				Name: "interval_modifiers",
				Steps: []e2e.Task{
					{
						Name:    "interval_modifiers: run interval_modifiers",
						Command: binary,
						Args:    []string{"run", "--apply-interval-modifiers", "-env", "env-interval-modifiers", "--start-date", "2025-04-02T09:30:00.000Z", "--end-date", "2025-04-02T11:30:00.000Z", filepath.Join(currentFolder, "test-pipelines/interval-modifiers-pipeline/assets/products.sql")},
						Env:     []string{},

						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "interval_modifiers: query the table",
						Command: binary,
						Args:    []string{"query", "--env", "env-interval-modifiers", "--asset", filepath.Join(currentFolder, "test-pipelines/interval-modifiers-pipeline/assets/products.sql"), "--query", "SELECT * FROM products", "--output", "json"},
						Env:     []string{},

						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/interval-modifiers-pipeline/expectations/final_expected.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
				},
			},
		},
		{
			name: "run_pipeline_with_variables",
			workflow: e2e.Workflow{
				Name: "run_pipeline_with_variables",
				Steps: []e2e.Task{
					{
						Name:    "variables: run pipeline",
						Command: binary,
						Args: []string{
							"run",
							filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql"),
						},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "variables: validate output (run pipeline)",
						Command: binary,
						Args: []string{
							"query",
							"--connection", "duckdb-variables",
							"--query", `SELECT name FROM public.users`,
						},
						WorkingDir: currentFolder,
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   "┌──────┐\n│ NAME │\n├──────┤\n│ jhon │\n│ erik │\n└──────┘\n",
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputString,
						},
					},
					{
						Name:    "variables: run pipeline with json override",
						Command: binary,
						Args: []string{
							"run",
							"--var", `{"users": ["mark", "nicholas"]}`,
							filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql"),
						},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "variables: validate output (run pipeline with json override)",
						Command: binary,
						Args: []string{
							"query",
							"--connection", "duckdb-variables",
							"--query", `SELECT name FROM public.users`,
						},
						WorkingDir: currentFolder,
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   "┌──────────┐\n│ NAME     │\n├──────────┤\n│ mark     │\n│ nicholas │\n└──────────┘\n",
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputString,
						},
					},
					{
						Name:    "variables: run pipeline with key=value override",
						Command: binary,
						Args: []string{
							"run",
							"--var", `users=["tanaka", "yamaguchi"]`,
							filepath.Join(currentFolder, "test-pipelines/variables-interpolation/assets/users.sql"),
						},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "variables: validate output (run pipeline with key=value override)",
						Command: binary,
						Args: []string{
							"query",
							"--connection", "duckdb-variables",
							"--query", `SELECT name FROM public.users`,
						},
						WorkingDir: currentFolder,
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   "┌───────────┐\n│ NAME      │\n├───────────┤\n│ tanaka    │\n│ yamaguchi │\n└───────────┘\n",
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputString,
						},
					},
					{
						Name:    "variables: get databases from duckdb",
						Command: binary,
						Args: []string{
							"internal",
							"fetch-databases",
							"--connection", "duckdb-variables",
						},
						WorkingDir: currentFolder,
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"public"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
				},
			},
		},
		//nolint:dupl
		{
			name: "run_pipeline_with_scd2_by_column",
			workflow: e2e.Workflow{
				Name: "run_pipeline_with_scd2_by_column",
				Steps: []e2e.Task{
					{
						Name:    "scd2-col-01a: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempdir, "test-scd2-by-column")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-col-01b: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempdir, "test-scd2-by-column"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-col-01c: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-column-pipeline"), "."},
						WorkingDir: filepath.Join(tempdir, "test-scd2-by-column"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-col-02: run pipeline with full refresh",
						Command: binary,
						Args:    []string{"run", "--full-refresh", "--env", "env-scd2-by-column", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-column-pipeline")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-col-03: query the initial table",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-scd2-by-column", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-column-pipeline/expectations/scd2_by_col_expected_initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-col-04a: copy menu_updated_01.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/menu_updated_01.sql"), filepath.Join(tempdir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-col-04b: run pipeline with updated menu",
						Command: binary,
						Args:    []string{"run", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--env", "env-scd2-by-column", filepath.Join(tempdir, "test-scd2-by-column/scd2-by-column-pipeline")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-col-05: query the updated table 01",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-scd2-by-column", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_01.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-col-06a: copy menu_updated_02.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/menu_updated_02.sql"), filepath.Join(tempdir, "test-scd2-by-column/scd2-by-column-pipeline/assets/menu.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-col-06b: run pipeline with updated menu 02",
						Command: binary,
						Args:    []string{"run", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--env", "env-scd2-by-column", filepath.Join(tempdir, "test-scd2-by-column/scd2-by-column-pipeline")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-col-07: query the updated table 02",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-scd2-by-column", "--query", "SELECT ID, Name, Price, _is_current FROM test.menu ORDER BY ID, _valid_from;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-column-pipeline/expectations/scd2_by_col_expected_updated_02.csv"),
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
			name: "run_pipeline_with_scd2_by_time",
			workflow: e2e.Workflow{
				Name: "run_pipeline_with_scd2_by_time",
				Steps: []e2e.Task{
					{
						Name:    "scd2-time-01a: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempdir, "test-scd2-by-time")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-time-01b: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempdir, "test-scd2-by-time"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "scd2-time-01c: copy pipeline files",
						Command:    "cp",
						Args:       []string{"-a", filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-time-pipeline"), "."},
						WorkingDir: filepath.Join(tempdir, "test-scd2-by-time"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-time-02: run pipeline with full refresh",
						Command: binary,
						Args:    []string{"run", "--full-refresh", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--env", "env-scd2-by-time", filepath.Join(tempdir, "test-scd2-by-time/scd2-by-time-pipeline")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-time-03: query the initial table",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-scd2-by-time", "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-time-pipeline/expectations/scd2_by_time_expected_initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-time-04a: copy products_updated_01.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/products_updated_01.sql"), filepath.Join(tempdir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-time-04b: run pipeline with updated products",
						Command: binary,
						Args:    []string{"run", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--env", "env-scd2-by-time", filepath.Join(tempdir, "test-scd2-by-time/scd2-by-time-pipeline")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-time-05: query the updated table 01",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-scd2-by-time", "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_01.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "scd2-time-06a: copy products_updated_02.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/resources/products_updated_02.sql"), filepath.Join(tempdir, "test-scd2-by-time/scd2-by-time-pipeline/assets/products.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-time-06b: run pipeline with updated products 02",
						Command: binary,
						Args:    []string{"run", "--config-file", filepath.Join(currentFolder, ".bruin.yml"), "--env", "env-scd2-by-time", filepath.Join(tempdir, "test-scd2-by-time/scd2-by-time-pipeline")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "scd2-time-07: query the updated table 02",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-scd2-by-time", "--query", "SELECT product_id,product_name,stock,_is_current,_valid_from FROM test.products ORDER BY product_id, _valid_from;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-scd2-tests/scd2-by-time-pipeline/expectations/scd2_by_time_expected_update_02.csv"),
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
			name: "start_date_flags_workflow",
			workflow: e2e.Workflow{
				Name: "start_date_flags_workflow",
				Steps: []e2e.Task{
					{
						Name:    "start-date-flags: run with start-date and end-date",
						Command: binary,
						Args:    []string{"run", "--env", "env-start-date-flags", "--start-date", "2024-01-15", "--end-date", "2024-01-31", filepath.Join(currentFolder, "test-pipelines/start-date-flags-test")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"bruin run completed", "Finished: date_capture", "Finished: date_range_analysis"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:          "start-date-flags: validate flag was used",
						Command:       binary,
						Args:          []string{"query", "--env", "env-start-date-flags", "--asset", filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/assets/date_range_analysis.sql"), "--output", "json"},
						Env:           []string{},
						SkipJSONNodes: []string{`"connectionName"`, `"query"`},
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/expectations/flag_start_date_used.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
					{
						Name:    "start-date-flags: run with full-refresh override",
						Command: binary,
						Args:    []string{"run", "--env", "env-start-date-flags", "--full-refresh", "--start-date", "2024-01-15", "--end-date", "2024-01-31", filepath.Join(currentFolder, "test-pipelines/start-date-flags-test")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"bruin run completed", "Finished: date_capture", "Finished: date_range_analysis"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:          "start-date-flags: validate pipeline was used",
						Command:       binary,
						Args:          []string{"query", "--env", "env-start-date-flags", "--asset", filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/assets/date_range_analysis.sql"), "--output", "json"},
						Env:           []string{},
						SkipJSONNodes: []string{`"connectionName"`, `"query"`},
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/expectations/pipeline_start_date_used.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
					{
						Name:    "start-date-flags: run with full-refresh no start-date flag",
						Command: binary,
						Args:    []string{"run", "--env", "env-start-date-flags", "--full-refresh", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/start-date-flags-test")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"bruin run completed", "Finished: date_capture", "Finished: date_range_analysis"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:          "start-date-flags: validate pipeline start-date used when no flag",
						Command:       binary,
						Args:          []string{"query", "--env", "env-start-date-flags", "--asset", filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/assets/date_range_analysis.sql"), "--output", "json"},
						Env:           []string{},
						SkipJSONNodes: []string{`"connectionName"`, `"query"`},
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/start-date-flags-test/expectations/pipeline_start_date_no_flag.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
				},
			},
		},
		{
			name: "patch_pipeline_workflow",
			workflow: e2e.Workflow{
				Name: "patch_pipeline_workflow",
				Steps: []e2e.Task{
					{
						Name:    "patch: create test directory",
						Command: "mkdir",
						Args:    []string{"-p", filepath.Join(tempdir, "test-patch-pipeline")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "patch: initialize git repository",
						Command:    "git",
						Args:       []string{"init"},
						WorkingDir: filepath.Join(tempdir, "test-patch-pipeline"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "patch: copy simple pipeline",
						Command:    "cp",
						Args:       []string{filepath.Join(currentFolder, "../pkg/pipeline/testdata/persist/simple-pipeline.yml"), "pipeline.yml"},
						WorkingDir: filepath.Join(tempdir, "test-patch-pipeline"),
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "patch: patch pipeline name and add retries",
						Command: binary,
						Args:    []string{"internal", "patch-pipeline", "--body", `{"name": "patched-pipeline", "retries": 5}`, filepath.Join(tempdir, "test-patch-pipeline/pipeline.yml")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "patch: patch pipeline concurrency and schedule",
						Command: binary,
						Args:    []string{"internal", "patch-pipeline", "--body", `{"concurrency": 10, "schedule": "daily"}`, filepath.Join(tempdir, "test-patch-pipeline/pipeline.yml")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "patch: patch pipeline with assets",
						Command: binary,
						Args:    []string{"internal", "patch-pipeline", "--body", `{"name": "final-pipeline", "assets": [{"name": "test-asset", "type": "python"}]}`, filepath.Join(tempdir, "test-patch-pipeline/pipeline.yml")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "patch: verify final pipeline with parse-pipeline",
						Command: binary,
						Args:    []string{"internal", "parse-pipeline", filepath.Join(tempdir, "test-patch-pipeline")},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"final-pipeline", "daily", "2023-01-01", "my-connection", "retries\":5", "concurrency\":10"},
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
			name: "asset_level_start_date_workflow",
			workflow: e2e.Workflow{
				Name: "asset_level_start_date_workflow",
				Steps: []e2e.Task{
					{
						Name:    "asset-level-start-date: run asset_no_start_date",
						Command: binary,
						Args:    []string{"run", "--full-refresh", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/asset-level-start-date-test/assets/asset_no_start_date.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"bruin run completed", "Finished: asset_no_start_date"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "asset-level-start-date: run asset_with_start_date",
						Command: binary,
						Args:    []string{"run", "--full-refresh", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/asset-level-start-date-test/assets/asset_with_start_date.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"bruin run completed", "Finished: asset_with_start_date"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:          "asset-level-start-date: validate asset without start_date uses pipeline start_date",
						Command:       binary,
						Args:          []string{"query", "--connection", "duckdb-variables", "--query", "SELECT * FROM asset_no_start_date", "--output", "json"},
						Env:           []string{},
						SkipJSONNodes: []string{`"connectionName"`, `"query"`},
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/asset-level-start-date-test/expectations/asset_no_start_date_full_refresh.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
					{
						Name:          "asset-level-start-date: validate asset with start_date uses its own start_date",
						Command:       binary,
						Args:          []string{"query", "--connection", "duckdb-variables", "--query", "SELECT * FROM asset_with_start_date", "--output", "json"},
						Env:           []string{},
						SkipJSONNodes: []string{`"connectionName"`, `"query"`},
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/asset-level-start-date-test/expectations/asset_with_start_date_full_refresh.json")),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputJSON,
						},
					},
				},
			},
		},
		{
			name: "duckdb_hooks_workflow",
			workflow: e2e.Workflow{
				Name: "duckdb_hooks_workflow",
				Steps: []e2e.Task{
					{
						Name:    "hooks: run pipeline",
						Command: binary,
						Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/duckdb-hooks-pipeline")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"bruin run completed", "Finished: hooks_test.main_table"},
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByContains,
						},
					},
					{
						Name:    "hooks: query hook log",
						Command: binary,
						Args: []string{
							"query",
							"--connection",
							"duckdb-default",
							"--query",
							"SELECT step FROM hooks_test.hook_log ORDER BY step;",
							"--output",
							"csv",
						},
						Env: []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-hooks-pipeline/expectations/hook_log.csv"),
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
			name: "duckdb_create_replace_materialization",
			workflow: e2e.Workflow{
				Name: "duckdb_create_replace_materialization",
				Steps: []e2e.Task{
					{
						Name:    "mat-cr-00: ensure initial orders.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/resources/orders_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/assets/orders.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-cr-01: run pipeline with initial data",
						Command: binary,
						Args:    []string{"run", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-cr-02: query the initial data",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.orders ORDER BY order_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/expectations/initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-cr-03: replace orders.sql with orders_v2.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/resources/orders_v2.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/assets/orders.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-cr-04: run pipeline after replacing asset",
						Command: binary,
						Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-cr-05: query the replaced data",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.orders ORDER BY order_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/expectations/replaced.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-cr-06: restore original orders.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/resources/orders_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-create-replace/assets/orders.sql")},
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
			name: "duckdb_delete_insert_materialization",
			workflow: e2e.Workflow{
				Name: "duckdb_delete_insert_materialization",
				Steps: []e2e.Task{
					{
						Name:    "mat-di-00: ensure initial products.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/resources/products_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/assets/products.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-di-01: run pipeline with initial data",
						Command: binary,
						Args:    []string{"run", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-di-02: query the initial data",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.products ORDER BY product_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/expectations/initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-di-03: replace products.sql with products_v2.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/resources/products_v2.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/assets/products.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-di-04: run pipeline after replacing asset",
						Command: binary,
						Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-di-05: query data after delete+insert",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.products ORDER BY product_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/expectations/after_delete_insert.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-di-06: restore original products.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/resources/products_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-delete-insert/assets/products.sql")},
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
			name: "duckdb_merge_materialization",
			workflow: e2e.Workflow{
				Name: "duckdb_merge_materialization",
				Steps: []e2e.Task{
					{
						Name:    "mat-merge-00: ensure initial inventory.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/resources/inventory_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/assets/inventory.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-merge-01: run pipeline with initial data",
						Command: binary,
						Args:    []string{"run", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-merge-02: query the initial data",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.inventory ORDER BY item_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/expectations/initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-merge-03: replace inventory.sql with inventory_v2.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/resources/inventory_v2.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/assets/inventory.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-merge-04: run pipeline after replacing asset",
						Command: binary,
						Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-merge-05: query data after merge",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.inventory ORDER BY item_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/expectations/after_merge.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-merge-06: restore original inventory.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/resources/inventory_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-merge/assets/inventory.sql")},
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
			name: "duckdb_truncate_insert_materialization",
			workflow: e2e.Workflow{
				Name: "duckdb_truncate_insert_materialization",
				Steps: []e2e.Task{
					{
						Name:    "mat-trunc-00: ensure initial books.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/resources/books_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/assets/books.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-trunc-01: run pipeline with initial data",
						Command: binary,
						Args:    []string{"run", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-trunc-02: query the initial data",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.books ORDER BY book_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/expectations/initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-trunc-03: get initial table structure",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "PRAGMA table_info('test.books');", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-trunc-04: replace books.sql with books_v2.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/resources/books_v2.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/assets/books.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-trunc-05: run pipeline after replacing asset",
						Command: binary,
						Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-trunc-06: query data after truncate+insert",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.books ORDER BY book_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/expectations/after_truncate.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-trunc-07: verify table structure unchanged",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "PRAGMA table_info('test.books');", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-trunc-08: restore original books.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/resources/books_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-truncate-insert/assets/books.sql")},
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
			name: "duckdb_append_materialization",
			workflow: e2e.Workflow{
				Name: "duckdb_append_materialization",
				Steps: []e2e.Task{
					{
						Name:    "mat-app-00: ensure initial logs.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/resources/logs_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/assets/logs.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-app-01: run pipeline with initial data",
						Command: binary,
						Args:    []string{"run", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-app-02: query the initial data",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.logs ORDER BY log_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/expectations/initial.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-app-03: replace logs.sql with logs_v2.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/resources/logs_v2.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/assets/logs.sql")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-app-04: run pipeline to append new data",
						Command: binary,
						Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-app-05: query data after append",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT * FROM test.logs ORDER BY log_id;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							CSVFile:  filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/expectations/after_append.csv"),
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByCSV,
						},
					},
					{
						Name:    "mat-app-06: restore original logs.sql",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/resources/logs_v1.sql"), filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-append/assets/logs.sql")},
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
			name: "duckdb_ddl_materialization",
			workflow: e2e.Workflow{
				Name: "duckdb_ddl_materialization",
				Steps: []e2e.Task{
					{
						Name:    "mat-ddl-01: run pipeline with DDL strategy",
						Command: binary,
						Args:    []string{"run", "--full-refresh", filepath.Join(currentFolder, "test-pipelines/duckdb-materialization-ddl")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "mat-ddl-02: verify table exists but is empty",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "SELECT COUNT(*) as row_count FROM test.customers;", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Output:   "row_count\n0\n",
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
							e2e.AssertByOutputString,
						},
					},
					{
						Name:    "mat-ddl-03: verify table structure",
						Command: binary,
						Args:    []string{"query", "--connection", "duckdb-mat-test", "--query", "PRAGMA table_info('test.customers');", "--output", "csv"},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
							Contains: []string{"customer_id", "name", "email", "created_at"},
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
			// Check if parallel execution is enabled via environment variable
			if os.Getenv("ENABLE_PARALLEL") == "1" {
				t.Parallel()
			}
			err := tt.workflow.Run()

			require.NoError(t, err, "Workflow %s failed: %v", tt.workflow.Name, err)
		})
	}
}

//nolint:paralleltest
func TestIngestrTasks(t *testing.T) {
	cleanupDuckDBFiles(t)

	// Check if parallel execution is enabled via environment variable
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tests := []struct {
		name string
		task e2e.Task
	}{
		{
			name: "ingestr-pipeline",
			task: e2e.Task{
				Name:    "ingestr-pipeline",
				Command: binary,
				Args:    []string{"run", "-env", "env-ingestr", filepath.Join(currentFolder, "test-pipelines/ingestr-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"bruin run completed", "Finished: chess_playground.profiles", "Finished: chess_playground.games", "Finished: chess_playground.player_summary", "Finished: chess_playground.player_summary:total_games:positive"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-seed-data",
			task: e2e.Task{
				Name:    "run-seed-data",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-seed-data", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"bruin run completed"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-asset-default-option-pipeline",
			task: e2e.Task{
				Name:    "run-asset-default-option-pipeline",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-default-option", filepath.Join(currentFolder, "test-pipelines/parse-default-option")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated 4 assets", "bruin run completed", "Finished: chess_playground.player_summary", "Finished: chess_playground.games", "Finished: python_asset"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "run-python-materialization",
			task: e2e.Task{
				Name:    "run-python-materialization",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-python-materialization", filepath.Join(currentFolder, "test-pipelines/run-python-materialization")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated 1 assets", "bruin run completed", "Finished: materialize.country"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-r-basic-execution",
			task: e2e.Task{
				Name:    "validate-r-basic-execution",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/r-basic-execution")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "run-r-basic-execution",
			task: e2e.Task{
				Name:    "run-r-basic-execution",
				Command: binary,
				Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/r-basic-execution")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Hello from R!", "2 + 2 = 4"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-r-with-connections",
			task: e2e.Task{
				Name:    "validate-r-with-connections",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/r-with-connections")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
				},
			},
		},
		{
			name: "run-r-with-connections",
			task: e2e.Task{
				Name:    "run-r-with-connections",
				Command: binary,
				Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/r-with-connections")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"All environment variable tests passed!"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check if parallel execution is enabled via environment variable
			if os.Getenv("ENABLE_PARALLEL") == "1" {
				t.Parallel()
			}
			err := tt.task.Run()
			require.NoError(t, err, "Task %s failed: %v", tt.task.Name, err)
		})
	}
}

//nolint:paralleltest
func TestMacros(t *testing.T) {
	cleanupDuckDBFiles(t)

	// Check if parallel execution is enabled via environment variable
	if os.Getenv("ENABLE_PARALLEL") == "1" {
		t.Parallel()
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tests := []struct {
		name string
		task e2e.Task
	}{
		{
			name: "macros-pipeline",
			task: e2e.Task{
				Name:    "macros-pipeline",
				Command: binary,
				Args:    []string{"run", filepath.Join(currentFolder, "test-pipelines/macros-pipeline")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"bruin run completed", "3 succeeded"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-single-asset",
			task: e2e.Task{
				Name:    "validate-single-asset",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/happy-path/assets/products.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated", "products.sql", "all good"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "validate-pipeline",
			task: e2e.Task{
				Name:    "validate-pipeline",
				Command: binary,
				Args:    []string{"validate", filepath.Join(currentFolder, "test-pipelines/happy-path")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{"Successfully validated", "assets across 1 pipeline", "all good"},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "rerun-cooldown-translation",
			task: e2e.Task{
				Name:    "rerun-cooldown-translation",
				Command: binary,
				Args:    []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "../test-rerun-cooldown")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						// Pipeline default rerun_cooldown
						`"default":{"type":"","parameters":null,"secrets":null,"interval_modifiers":null,"rerun_cooldown":300}`, `"retries_delay":300`,
						// Asset with explicit rerun_cooldown
						`"name":"test_asset"`, `"rerun_cooldown":600`, `"retries_delay":600`,
						// Asset that inherits from pipeline
						`"name":"inherits_pipeline"`, `"retries_delay":300`,
						// Asset with disabled retries
						`"name":"no_delay"`, `"rerun_cooldown":-1`, `"retries_delay":0`,
						// Python asset with rerun_cooldown
						`"name":"python_test"`, `"rerun_cooldown":900`, `"retries_delay":900`,
						// Ingestr asset with rerun_cooldown
						`"name":"ingestr_test"`, `"rerun_cooldown":450`, `"retries_delay":450`,
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
		{
			name: "rerun-cooldown-asset-parsing",
			task: e2e.Task{
				Name:    "rerun-cooldown-asset-parsing",
				Command: binary,
				Args:    []string{"internal", "parse-asset", filepath.Join(currentFolder, "../test-rerun-cooldown/assets/test_asset.sql")},
				Env:     []string{},
				Expected: e2e.Output{
					ExitCode: 0,
					Contains: []string{
						// Asset with explicit rerun_cooldown should translate correctly
						`"name":"test_asset"`, `"rerun_cooldown":600`, `"retries_delay":600`,
					},
				},
				Asserts: []func(*e2e.Task) error{
					e2e.AssertByExitCode,
					e2e.AssertByContains,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check if parallel execution is enabled via environment variable
			if os.Getenv("ENABLE_PARALLEL") == "1" {
				t.Parallel()
			}
			err := tt.task.Run()
			require.NoError(t, err, "Task %s failed: %v", tt.task.Name, err)
		})
	}
}
