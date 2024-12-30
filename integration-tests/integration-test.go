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
				Name:   "users",
				Status: "succeeded",
			},
			{
				Name:   "country",
				Status: "failed",
			},
			{
				Name:   "example",
				Status: "failed",
			},
			{
				Name:   "people",
				Status: "failed",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "39c095c875c072225e2927db0a6488153524215636cb2e6672a1056b80bc64c3",
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
				Name:   "users",
				Status: "succeeded",
			},
			{
				Name:   "country",
				Status: "failed",
			},
			{
				Name:   "example",
				Status: "failed",
			},
			{
				Name:   "people",
				Status: "failed",
			},
		},
		Version:           "1.0.0",
		CompatibilityHash: "39c095c875c072225e2927db0a6488153524215636cb2e6672a1056b80bc64c3",
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

	runIntegrationTests(binary, currentFolder)
	runIntegrationWorkflow(binary, currentFolder)
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

func runIntegrationTests(binary string, currentFolder string) {
	tests := getTasks(binary, currentFolder)
	for _, test := range tests {
		if err := test.Run(); err != nil {
			fmt.Printf("%s Assert error: %v\n", test.Name, err)
			os.Exit(1)
		}
	}
}

func getWorkflow(binary string, currentFolder string, tempfile string) []e2e.Workflow {
	return []e2e.Workflow{
		{
			Name: "example after failure",
			Steps: []e2e.Task{
				{
					Name:    "run first time",
					Command: binary,
					Args:    []string{"run", filepath.Join(currentFolder, "example-continue")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 1,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/example_continue"), stateForFirstRun),
					},
				},
				{
					Name:    "copy player_summary.sql to tempfile",
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "example/assets/example.sql"), tempfile},
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
					Args:    []string{filepath.Join(currentFolder, "example.sql"), filepath.Join(currentFolder, "example/assets/example.sql")},
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
					Args:    []string{"run", "--continue", filepath.Join(currentFolder, "example-continue")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/example_continue"), stateForContinueRun),
					},
				},
				{
					Name:    "copy player_summary.sql back to continue",
					Command: "cp",
					Args:    []string{tempfile, filepath.Join(currentFolder, "example/assets/example.sql")},
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
			Name:          "example",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "example")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "example/expectations/pipeline.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "chess-extended",
			Command: binary,
			Args:    []string{"run", "--tag", "include", "--exclude-tag", "exclude", filepath.Join(currentFolder, "chess-extended")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "chess-extended-only-checks",
			Command: binary,
			Args:    []string{"run", "--tag", "include", "--exclude-tag", "exclude", "--only", "checks", filepath.Join(currentFolder, "chess-extended")},
			Env:     []string{},

			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 1 tasks", "total_games:positive"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "format-if-fail",
			Command: binary,
			Args:    []string{"format", "--fail-if-changed", filepath.Join(currentFolder, "chess-extended/assets/chess_games.asset.yml")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "chess-extended-only-main",
			Command: binary,
			Args:    []string{"run", "--tag", "include", "--exclude-tag", "exclude", "--only", "main", filepath.Join(currentFolder, "chess-extended")},
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
			Name:    "downstream-chess-extended",
			Command: binary,
			Args:    []string{"run", "--downstream", filepath.Join(currentFolder, "chess-extended/assets/game_outcome_summary.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 4 tasks", " Finished: chess_playground.game_outcome_summary", "Finished: chess_playground.game_outcome_summary:total_games:positive", "Finished: chess_playground.player_summary", " Finished: chess_playground.player_summary:total_games:non_negative"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "downstream-only-main-chess-extended",
			Command: binary,
			Args:    []string{"run", "--downstream", "--only", "main", filepath.Join(currentFolder, "chess-extended/assets/game_outcome_summary.sql")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Contains: []string{"Executed 2 tasks", " Finished: chess_playground.game_outcome_summary", "Finished: chess_playground.player_summary"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "push-metadata",
			Command: binary,
			Args:    []string{"run", "--push-metadata", "--only", "push-metadata", filepath.Join(currentFolder, "bigquery-metadata")},
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
			Args:    []string{"validate", filepath.Join(currentFolder, "example")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
			},
		},
		{
			Name:    "run-use-uv-happy-path",
			Command: binary,
			Args:    []string{"run", "--use-uv", filepath.Join(currentFolder, "chess-extended")},
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
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "example/assets/country.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "example/expectations/country.sql.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "parse-asset-faulty-pipeline-error-sql",
			Command: binary,
			Args:    []string{"internal", "parse-asset", filepath.Join(currentFolder, "faulty-pipeline/assets/error.sql")},
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
			Args:          []string{"validate", "-o", "json", filepath.Join(currentFolder, "missing-upstream/assets/nonexistent.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "missing-upstream/expectations/missing_upstream.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:    "run-malformed-sql",
			Command: binary,
			Args:    []string{"run", filepath.Join(currentFolder, "malformed/assets/malformed.sql")},
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
			Name:          "parse-pipeline-lineage",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", "-c", filepath.Join(currentFolder, "example")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "example/expectations/lineage.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
		{
			Name:          "parse-asset-lineage-example",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", "-c", filepath.Join(currentFolder, "example/assets/example.sql")},
			Env:           []string{},
			SkipJSONNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "example/expectations/lineage-asset.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
	}
}
