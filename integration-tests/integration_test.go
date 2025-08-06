package main_test

import (
	"os"
	"path/filepath"
	"runtime"
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
			Environment:  "",
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
			Environment:  "",
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

func TestIndividualTasks(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	// currentFolder is already in integration-tests, so no need to join again

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.task.Run()
			require.NoError(t, err, "Task %s failed: %v", tt.task.Name, err)
			t.Logf("Task '%s' completed successfully", tt.task.Name)
		})
	}
}

func TestWorkflowTasks(t *testing.T) {
	t.Parallel()

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	// currentFolder is already in integration-tests, so no need to join again

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	binary := filepath.Join(currentFolder, "../bin", executable)

	tempdir, err := os.MkdirTemp(os.TempDir(), "bruin-test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempdir)

	// Create the shipping providers temp file in advance
	tempfile, err := os.CreateTemp(tempdir, "shipping_providers*.sql")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempfilePath := tempfile.Name()
	tempfile.Close() // Close the file but keep the path

	tests := []struct {
		name     string
		workflow e2e.Workflow
	}{
		{
			name: "continue after failure",
			workflow: e2e.Workflow{
				Name: "continue after failure",
				Steps: []e2e.Task{
					{
						Name:    "run first time",
						Command: binary,
						Args:    []string{"run", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "continue")},
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
						Name:    "copy shipping_providers.sql to tempfile",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "continue/assets/shipping_providers.sql"), tempfilePath},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "copy shipping_providers.sql to continue",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "shipping_providers.sql"), filepath.Join(currentFolder, "continue/assets/shipping_providers.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "run continue",
						Command: binary,
						Args:    []string{"run", "--start-date", "2024-01-01", "--end-date", "2024-12-31", "--continue", filepath.Join(currentFolder, "continue")},
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
						Name:    "copy broken shipping_providers.sql back to continue",
						Command: "cp",
						Args:    []string{tempfilePath, filepath.Join(currentFolder, "continue/assets/shipping_providers.sql")},
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
			name: "Bruin init",
			workflow: e2e.Workflow{
				Name: "Bruin init",
				Steps: []e2e.Task{
					{
						Name:       "create a test directory",
						Command:    "mkdir",
						Args:       []string{"-p", filepath.Join(tempdir, "test-bruin-init")},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:       "run git init",
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
						Name:       "run bruin init",
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
				},
			},
		},
		{
			name: "Time materialization",
			workflow: e2e.Workflow{
				Name: "Time materialization",
				Steps: []e2e.Task{
					{
						Name:    "restore asset to initial state",
						Command: "cp",
						Args:    []string{filepath.Join(currentFolder, "resources/products.sql"), filepath.Join(currentFolder, "test-pipelines/time-materialization-pipeline/assets/products.sql")},
						Env:     []string{},
						Expected: e2e.Output{
							ExitCode: 0,
						},
						Asserts: []func(*e2e.Task) error{
							e2e.AssertByExitCode,
						},
					},
					{
						Name:    "create the table",
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

func TestIngestrTasks(t *testing.T) {
	t.Parallel()

	includeIngestr := os.Getenv("INCLUDE_INGESTR") == "1"
	if !includeIngestr {
		t.Skip("Skipping ingestr tests - set INCLUDE_INGESTR=1 to run")
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	// currentFolder is already in integration-tests, so no need to join again

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.task.Run()
			require.NoError(t, err, "Task %s failed: %v", tt.task.Name, err)
			t.Logf("Task '%s' completed successfully", tt.task.Name)
		})
	}
}


