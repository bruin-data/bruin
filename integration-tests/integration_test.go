package main_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

var (
	stateForFirstRun = &scheduler.PipelineState{
		Parameters: scheduler.RunConfig{
			Downstream:   false,
			Workers:      16,
			Environment:  "env-continue",
			Force:        false,
			PushMetadata: false,
			NoLogFile:    false,
			FullRefresh:  false,
			UsePip:       false,
			Tag:          "",
			ExcludeTag:   "",
			Only:         nil,
		},
		Metadata: scheduler.Metadata{
			Version: "dev",
			OS:      runtime.GOOS,
		},
		State: []*scheduler.PipelineAssetState{
			{
				Name:   "product_categories",
				Status: "succeeded",
			},
			{
				Name:   "product_price_summary",
				Status: "succeeded",
			},
			{
				Name:   "products",
				Status: "succeeded",
			},
			{
				Name:   "shipping_providers",
				Status: "failed",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "e62a4c57b82d5452bc57cab24f45eb4abda2a737b0269492de0030fba452ed7e",
	}
	stateForContinueRun = &scheduler.PipelineState{
		Parameters: scheduler.RunConfig{
			Downstream:   false,
			StartDate:    "2024-12-22 00:00:00.000000",
			EndDate:      "2024-12-22 23:59:59.999999",
			Workers:      16,
			Environment:  "env-continue",
			Force:        false,
			PushMetadata: false,
			NoLogFile:    false,
			FullRefresh:  false,
			UsePip:       false,
			Tag:          "",
			ExcludeTag:   "",
			Only:         nil,
		},
		Metadata: scheduler.Metadata{
			Version: "dev",
			OS:      runtime.GOOS,
		},
		State: []*scheduler.PipelineAssetState{
			{
				Name:   "product_categories",
				Status: "skipped",
			},
			{
				Name:   "product_price_summary",
				Status: "skipped",
			},
			{
				Name:   "products",
				Status: "skipped",
			},
			{
				Name:   "shipping_providers",
				Status: "succeeded",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "e62a4c57b82d5452bc57cab24f45eb4abda2a737b0269492de0030fba452ed7e",
	}
)

// Global variables for managing shared temporary directories.
var (
	sharedTempDir      string
	sharedTempDirOnce  sync.Once
	sharedTempDirMutex sync.Mutex
)

// getSharedTempDir returns a shared temporary directory for all integration tests.
func getSharedTempDir(t *testing.T) string {
	sharedTempDirOnce.Do(func() {
		var err error
		// We use os.MkdirTemp instead of t.TempDir to maintain a shared directory structure
		// across all tests for easier cleanup and organization.
		sharedTempDir, err = os.MkdirTemp("", "bruin-integration-tests-*") //nolint:usetesting
		if err != nil {
			t.Fatalf("Failed to create shared temporary directory: %v", err)
		}
		t.Logf("Created shared temporary directory: %s", sharedTempDir)
	})
	return sharedTempDir
}

// getTestTempDir returns a unique temporary directory for a specific test.
func getTestTempDir(t *testing.T) string {
	sharedTempDirMutex.Lock()
	defer sharedTempDirMutex.Unlock()

	sharedDir := getSharedTempDir(t)
	// We use os.MkdirTemp instead of t.TempDir to create subdirectories within the shared directory.
	testDir, err := os.MkdirTemp(sharedDir, "test-*") //nolint:usetesting
	if err != nil {
		t.Fatalf("Failed to create test temporary directory: %v", err)
	}
	return testDir
}

// setupTestEnvironment sets up the test environment with temporary directories.
func setupTestEnvironment() {
	// Set environment variable to indicate we're running integration tests.
	os.Setenv("BRUIN_INTEGRATION_TEST", "1")
}

// setupTaskEnvironment sets up the environment for an individual task.
func setupTaskEnvironment(t *testing.T) {
	// Create a unique temporary directory for this specific task.
	taskTempDir := getTestTempDir(t)
	os.Setenv("BRUIN_TEST_TEMP_DIR", taskTempDir) //nolint:tenv
}

// cleanupTestEnvironment cleans up the test environment.
func cleanupTestEnvironment() {
	// Clean up test-specific environment variables.
	os.Unsetenv("BRUIN_TEST_TEMP_DIR")
	os.Unsetenv("BRUIN_INTEGRATION_TEST")
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

	// Setup test environment
	setupTestEnvironment()
	defer cleanupTestEnvironment()

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
			name: "run-use-uv",
			task: e2e.Task{
				Name:    "run-use-uv",
				Command: binary,
				Args:    []string{"run", "--env", "env-run-use-uv", "--use-uv", filepath.Join(currentFolder, "test-pipelines/run-use-uv-pipeline")},
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
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections_schema.json")),
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
					Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections.json")),
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
					Contains: []string{"Successfully validated 2 assets", "bruin run completed", "Finished: render_this.my_asset_2"},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check if parallel execution is enabled via environment variable
			if os.Getenv("ENABLE_PARALLEL") == "1" {
				t.Parallel()
			}

			// Setup task-specific environment (each task gets its own UV installation directory)
			setupTaskEnvironment(t)

			err := tt.task.Run()
			require.NoError(t, err, "Task %s failed: %v", tt.task.Name, err)
			t.Logf("Task '%s' completed successfully", tt.task.Name)
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

	// Setup test environment
	setupTestEnvironment()
	defer cleanupTestEnvironment()

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
							e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), stateForFirstRun),
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
							e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), stateForContinueRun),
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
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_bruin.yaml")),
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
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_bruin.yaml")),
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
							Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_bruin_chess.yaml")),
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
							Contains: []string{"PUBLIC"},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check if parallel execution is enabled via environment variable
			if os.Getenv("ENABLE_PARALLEL") == "1" {
				t.Parallel()
			}
			err := tt.workflow.Run()

			require.NoError(t, err, "Workflow %s failed: %v", tt.workflow.Name, err)
			t.Logf("Workflow '%s' completed successfully", tt.workflow.Name)
		})
	}
}

//nolint:paralleltest
// func TestIngestrTasks(t *testing.T) {
// 	cleanupDuckDBFiles(t)

// 	// Check if parallel execution is enabled via environment variable
// 	if os.Getenv("ENABLE_PARALLEL") == "1" {
// 		t.Parallel()
// 	}

// 	// Setup test environment
// 	setupTestEnvironment(t)
// 	defer cleanupTestEnvironment(t)

// 	currentFolder, err := os.Getwd()
// 	if err != nil {
// 		t.Fatalf("Failed to get current working directory: %v", err)
// 	}

// 	executable := "bruin"
// 	if runtime.GOOS == "windows" {
// 		executable = "bruin.exe"
// 	}
// 	binary := filepath.Join(currentFolder, "../bin", executable)

// 	tests := []struct {
// 		name string
// 		task e2e.Task
// 	}{
// 		{
// 			name: "ingestr-pipeline",
// 			task: e2e.Task{
// 				Name:    "ingestr-pipeline",
// 				Command: binary,
// 				Args:    []string{"run", "-env", "env-ingestr", filepath.Join(currentFolder, "test-pipelines/ingestr-pipeline")},
// 				Env:     []string{},
// 				Expected: e2e.Output{
// 					ExitCode: 0,
// 					Contains: []string{"bruin run completed", "Finished: chess_playground.profiles", "Finished: chess_playground.games", "Finished: chess_playground.player_summary", "Finished: chess_playground.player_summary:total_games:positive"},
// 				},
// 				Asserts: []func(*e2e.Task) error{
// 					e2e.AssertByExitCode,
// 					e2e.AssertByContains,
// 				},
// 			},
// 		},
// 		{
// 			name: "run-seed-data",
// 			task: e2e.Task{
// 				Name:    "run-seed-data",
// 				Command: binary,
// 				Args:    []string{"run", "--env", "env-run-seed-data", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
// 				Env:     []string{},
// 				Expected: e2e.Output{
// 					ExitCode: 0,
// 					Contains: []string{"bruin run completed"},
// 				},
// 				Asserts: []func(*e2e.Task) error{
// 					e2e.AssertByExitCode,
// 					e2e.AssertByContains,
// 				},
// 			},
// 		},
// 		{
// 			name: "run-asset-default-option-pipeline",
// 			task: e2e.Task{
// 				Name:    "run-asset-default-option-pipeline",
// 				Command: binary,
// 				Args:    []string{"run", "--env", "env-run-default-option", filepath.Join(currentFolder, "test-pipelines/parse-default-option")},
// 				Env:     []string{},
// 				Expected: e2e.Output{
// 					ExitCode: 0,
// 					Contains: []string{"Successfully validated 4 assets", "bruin run completed", "Finished: chess_playground.player_summary", "Finished: chess_playground.games", "Finished: python_asset"},
// 				},
// 				Asserts: []func(*e2e.Task) error{
// 					e2e.AssertByExitCode,
// 					e2e.AssertByContains,
// 				},
// 			},
// 		},
// 		{
// 			name: "run-python-materialization",
// 			task: e2e.Task{
// 				Name:    "run-python-materialization",
// 				Command: binary,
// 				Args:    []string{"run", "--env", "env-run-python-materialization", filepath.Join(currentFolder, "test-pipelines/run-python-materialization")},
// 				Env:     []string{},
// 				Expected: e2e.Output{
// 					ExitCode: 0,
// 					Contains: []string{"Successfully validated 1 assets", "bruin run completed", "Finished: materialize.country"},
// 				},
// 				Asserts: []func(*e2e.Task) error{
// 					e2e.AssertByExitCode,
// 					e2e.AssertByContains,
// 				},
// 			},
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			// Check if parallel execution is enabled via environment variable
// 			if os.Getenv("ENABLE_PARALLEL") == "1" {
// 				t.Parallel()
// 			}

// 			// Setup task-specific environment (each task gets its own UV installation directory)
// 			setupTaskEnvironment(t)

// 			err := tt.task.Run()
// 			require.NoError(t, err, "Task %s failed: %v", tt.task.Name, err)
// 			t.Logf("Task '%s' completed successfully", tt.task.Name)
// 		})
// 	}
// }

// cleanupSharedTempDir cleans up the shared temporary directory.
func cleanupSharedTempDir() {
	if sharedTempDir != "" {
		if err := os.RemoveAll(sharedTempDir); err != nil {
			// Log the error but don't fail the tests.
			fmt.Printf("Warning: failed to clean up shared temporary directory %s: %v\n", sharedTempDir, err)
		} else {
			fmt.Printf("Cleaned up shared temporary directory: %s\n", sharedTempDir)
		}
	}
}

// TestMain runs before and after all tests.
func TestMain(m *testing.M) {
	// Run the tests.
	code := m.Run()

	// Clean up shared temporary directory.
	cleanupSharedTempDir()

	// Exit with the test result code.
	os.Exit(code)
}
