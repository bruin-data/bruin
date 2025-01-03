package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/bruin-data/bruin/pkg/e2e"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

var currentFolder string

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
				Name:   "chess_playground.games",
				Status: "succeeded",
			},
			{
				Name:   "chess_playground.profiles",
				Status: "succeeded",
			},
			{
				Name:   "chess_playground.game_outcome_summary",
				Status: "succeeded",
			},
			{
				Name:   "chess_playground.player_profile_summary",
				Status: "succeeded",
			},
			{
				Name:   "chess_playground.player_summary",
				Status: "failed",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "6a4a1598e729fea65eeaa889aa0602be3133a465bcdde84843ff02954497ff65",
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
				Name:   "chess_playground.games",
				Status: "skipped",
			},
			{
				Name:   "chess_playground.profiles",
				Status: "skipped",
			},
			{
				Name:   "chess_playground.game_outcome_summary",
				Status: "skipped",
			},
			{
				Name:   "chess_playground.player_profile_summary",
				Status: "skipped",
			},
			{
				Name:   "chess_playground.player_summary",
				Status: "succeeded",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "6a4a1598e729fea65eeaa889aa0602be3133a465bcdde84843ff02954497ff65",
	}
)

func main() {
	path, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	currentFolder = filepath.Join(path, "integration-tests")

	if runtime.GOOS == "windows" {
		out, err := exec.Command("mv", "bin/bruin", "bin/bruin.exe").Output()
		if err != nil {
			fmt.Printf("failed to rename binary for execution on windows: %s\n", out)
			panic(err)
		}
	}

	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	wd, _ := os.Getwd()
	binary := filepath.Join(wd, "bin", executable)

	// Run workflows in parallel
	runIntegrationWorkflow(binary, currentFolder)

	// Run tasks in parallel
	runIntegrationTasks(binary, currentFolder)
}

func runIntegrationWorkflow(binary string, currentFolder string) {
	tempfile, err := os.CreateTemp("", "bruin-test-continue")
	if err != nil {
		fmt.Println("Failed to create temporary file:", err)
		os.Exit(1)
	}

	workflows := getWorkflow(binary, currentFolder, tempfile.Name())

	for _, workflow := range workflows {
		err := workflow.Run()
		if err != nil {
			fmt.Printf("Assert error: %v\n", err)
			os.Exit(1)
		}
	}
}

// runIntegrationTasks runs tasks concurrently.
func runIntegrationTasks(binary string, currentFolder string) {
	tests := getTasks(binary, currentFolder)
	var wg sync.WaitGroup
	errCh := make(chan error, len(tests)) // Buffered channel to collect errors.

	for _, test := range tests {
		wg.Add(1)
		go func(t e2e.Task) {
			defer wg.Done()
			if err := t.Run(); err != nil {
				errCh <- fmt.Errorf("task %s: %w", t.Name, err)
			}
		}(test)
	}

	wg.Wait()
	close(errCh)

	// Collect and handle errors
	for err := range errCh {
		fmt.Println(err)
		os.Exit(1)
	}
}

// getWorkflow defines workflows and ensures sequential execution within steps.
func getWorkflow(binary string, currentFolder string, tempfile string) []e2e.Workflow {
	return []e2e.Workflow{
		{
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
					Name:    "copy player_summary.sql to tempfile",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "continue/assets/player_summary.sql"), tempfile},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "copy player_summary.sql to continue",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "player_summary.sql"), filepath.Join(currentFolder, "continue/assets/player_summary.sql")},
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
					Name:    "copy player_summary.sql back to continue",
					Command: "cp",
					Args:    []string{tempfile, filepath.Join(currentFolder, "continue/assets/player_summary.sql")},
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
	}
}
func getTasks(binary string, currentFolder string) []e2e.Task {
	return []e2e.Task{
		{
			Name:          "parse-whole-pipeline",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-whole-pipeline/expectations/pipeline.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
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
		{
			Name:    "run-with-filters",
			Command: binary,
			Args:    []string{"run", "-env", "env-run-with-filters", "--tag", "include", "--exclude-tag", "exclude", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-with-filters-pipeline")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 4 tasks", "Finished: chess_playground.games", " Finished: chess_playground.game_outcome_summary", "Finished: chess_playground.game_outcome_summary:total_games:positive", "Finished: chess_playground.profiles"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
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
		{
			Name:    "run-main-with-filters",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-main-with-filters", "--tag", "include", "--exclude-tag", "exclude", "--only", "main", "--start-date", "2024-01-01", "--end-date", "2024-12-31", filepath.Join(currentFolder, "test-pipelines/run-main-with-filters-pipeline")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 3 tasks", " Finished: chess_playground.games", "Finished: chess_playground.profiles", "Finished: chess_playground.game_outcome_summary"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-with-downstream",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-with-downstream", "--downstream", filepath.Join(currentFolder, "test-pipelines/run-with-downstream-pipeline/assets/products.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 5 tasks", "Finished: products", "Finished: products:price:positive", "Finished: product_price_summary", "Finished: product_price_summary:product_count:non_negative", "Finished: product_price_summary:total_stock:non_negative"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "run-main-with-downstream",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-main-with-downstream", "--downstream", "--only", "main", filepath.Join(currentFolder, "test-pipelines/run-main-with-downstream-pipeline/assets/products.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 2 tasks", "Finished: products", "Finished: product_price_summary"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "push-metadata",
			Command: binary,
			Args:    []string{"run", "--env", "env-push-metadata", "--push-metadata", "--only", "push-metadata", filepath.Join(currentFolder, "test-pipelines/push-metadata-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 1,
				Contains: []string{" Starting: shopify_raw.products:metadata-push", "Starting: shopify_raw.inventory_items:metadata-push"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
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
		{
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
		{
			Name:          "parse-asset-happy-path-asset-py",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/happy-path/assets/asset.py")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/happy-path/expectations/asset.py.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-games",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/happy-path/assets/chess_games.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/happy-path/expectations/chess_games.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-profiles",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/happy-path/assets/chess_profiles.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/happy-path/expectations/chess_profiles.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-player-summary",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/happy-path/assets/player_summary.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/happy-path/expectations/player_summary.sql.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
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
		{
			Name:          "validate-missing-upstream",
			Command:       binary,
			Args:          []string{"validate", "-o", "json", filepath.Join(currentFolder, "test-pipelines/missing-upstream-pipeline/assets/nonexistent.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/missing-upstream-pipeline/expectations/missing_upstream.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-malformed-sql",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-malformed-sql", filepath.Join(currentFolder, "test-pipelines/run-malformed-pipeline/assets/malformed.sql")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 1,
				Contains: []string{"Parser Error: syntax error at or near \"S_ELECT_\"", "Failed assets 1"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:          "internal-connections",
			Command:       binary,
			Args:          []string{"internal", "connections"},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections_schema.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "connections-list",
			Command:       binary,
			Args:          []string{"connections", "list", "-o", "json", currentFolder},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-lineage",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", "-c", filepath.Join(currentFolder, "test-pipelines/parse-lineage-pipeline")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-lineage-pipeline/expectations/lineage.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-lineage",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", "-c", filepath.Join(currentFolder, "test-pipelines/parse-asset-lineage-pipeline/assets/example.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-asset-lineage-pipeline/expectations/lineage-asset.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-seed-data",
			Command: binary,
			Args:    []string{"run --env env-run-seed-data", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 5 tasks"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
	}
}
