package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

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

	includeIngestr := os.Getenv("INCLUDE_INGESTR") == "1"
	runIntegrationTests(binary, currentFolder, includeIngestr)
	runIntegrationWorkflow(binary, currentFolder)
}

func runIntegrationWorkflow(binary string, currentFolder string) {
	tempdir, err := os.MkdirTemp(os.TempDir(), "bruin-test")
	if err != nil {
		fmt.Println("Failed to create temporary directory:", err)
		os.Exit(1)
	}

	workflows := getWorkflow(binary, currentFolder, tempdir)

	for _, workflow := range workflows {
		err := workflow.Run()
		if err != nil {
			fmt.Printf("Assert error: %v\n", err)
			os.Exit(1)
		}
	}
}

func runIntegrationTests(binary string, currentFolder string, includeIngestr bool) {
	tests := getTasks(binary, currentFolder)
	if includeIngestr {
		ingestrTasks := getIngestrTasks(binary, currentFolder)
		tests = append(tests, ingestrTasks...)
	}
	for _, test := range tests {
		if err := test.Run(); err != nil {
			fmt.Printf("%s Assert error: %v\n", test.Name, err)
			os.Exit(1)
		}
	}
}

func getWorkflow(binary string, currentFolder string, tempdir string) []e2e.Workflow {
	tempfile := GetTempFile(tempdir, "shipping_providers.sql")
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
					Name:    "copy shipping_providers.sql to tempfile",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "continue/assets/shipping_providers.sql"), tempfile},
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
					Args:    []string{tempfile, filepath.Join(currentFolder, "continue/assets/shipping_providers.sql")},
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
		{
			Name: "Bruin init",
			Steps: []e2e.Task{
				{
					Name:    "create a test directory",
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
					Name:    "change directory to test-bruin-init",
					Command: "cd",
					Args:    []string{filepath.Join(tempdir, "test-bruin-init")},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "run git init",
					Command: "git",
					Args:    []string{"init"},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "run bruin init",
					Command: binary,
					Args:    []string{"init", "clickhouse"},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Name:    "run bruin init 2",
					Command: binary,
					Args:    []string{"init", "clickhouse", "clickhouse2"},
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
				Contains: []string{"Executed 3 tasks", "Finished: shipping_provider", "Finished: products", "Finished: products:price:positive"},
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
				Contains: []string{"Executed 2 tasks", "Finished: shipping_provider", "Finished: products"},
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
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/asset.py")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/asset.py.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-games",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/chess_games.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/chess_games.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-profiles",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/chess_profiles.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/chess_profiles.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-happy-path-player-summary",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-happy-path/assets/player_summary.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-happy-path/expectations/player_summary.sql.json")),
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
			Args:    []string{"run", "--env", "env-run-seed-data", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
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
		{
			Name:          "parse-asset-seed-data",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/run-seed-data/assets/seed.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/run-seed-data/expectations/seed.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-asset-default-option-pipeline",
			Command: binary,
			Args:    []string{"run", "--env", "env-run-default-option", filepath.Join(currentFolder, "test-pipelines/parse-default-option")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Successfully validated 4 assets", "Executed 5 tasks", "Finished: chess_playground.player_summary", "Finished: chess_playground.games", "Finished: python_asset"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:          "parse-asset-default-option-pipeline",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "test-pipelines/parse-default-option")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/pipeline.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-default-option-asset-py",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-default-option/assets/asset.py")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/asset.py.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-default-option-chess-games",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "test-pipelines/parse-default-option/assets/chess_games.asset.yml")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "test-pipelines/parse-default-option/expectations/chess_games.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
	}
}

func getIngestrTasks(binary string, currentFolder string) []e2e.Task {
	return []e2e.Task{
		{
			Name:    "ingestr-pipeline",
			Command: binary,
			Args:    []string{"run", "-env", "env-ingestr", filepath.Join(currentFolder, "test-pipelines/ingestr-pipeline")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 4 tasks", "Finished: chess_playground.profiles", "Finished: chess_playground.games", "Finished: chess_playground.player_summary", "Finished: chess_playground.player_summary:total_games:positive"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
	}
}

func GetTempFile(tempdir string, filename string) string {
	tempfile, err := os.CreateTemp(tempdir, filename)
	if err != nil {
		fmt.Println("Failed to create temporary file:", err)
		os.Exit(1)
	}

	return tempfile.Name()
}
