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
	STATE_FOR_FIRST_RUN = &scheduler.PipelineState{
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
	STATE_FOR_CONTINUE_RUN = &scheduler.PipelineState{
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

	runIntegrationTests(binary, currentFolder)
	runIntegrationWorkflow(binary, currentFolder)

}

func runIntegrationWorkflow(binary string, currentFolder string) {
	workflows := []e2e.Workflow{
		{
			Name: "continue after failure",
			Steps: []e2e.Task{
				{
					Command: binary,
					Args:    []string{"run", filepath.Join(currentFolder, "continue")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 1,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), STATE_FOR_CONTINUE_RUN),
					},
				},
				{
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "continue/assets/player_summary.sql"), filepath.Join(currentFolder, "./player_summary.sql.bak")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "continue/player_summary.sql"), filepath.Join(currentFolder, "continue/assets/player_summary.sql")},
					Env:     []string{},

					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
					},
				},
				{
					Command: binary,
					Args:    []string{"run", "--continue", filepath.Join(currentFolder, "continue")},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertCustomState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), STATE_FOR_CONTINUE_RUN),
					},
				},
				{
					Command: "cp",
					Args:    []string{filepath.Join(currentFolder, "continue/player_summary.sql.bak"), filepath.Join(currentFolder, "continue/assets/player_summary.sql")},
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

	for _, workflow := range workflows {
		err := workflow.Run()
		if err != nil {
			fmt.Printf("Assert error: %v\n", err)
			os.Exit(1)
		}
	}
}

func runIntegrationTests(binary string, currentFolder string) {

	tests := []e2e.Task{
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

		// {
		// 	Name:    "chess-extended-exclude-tag",
		// 	Command: binary,
		// 	Args:    []string{"run", "--tag", "include", "--exclude-tag", "exclude", filepath.Join(currentFolder, "chess-extended/expectations/chess_games.asset.yml")},
		// 	Env:     []string{},

		// 	Expected: e2e.Output{
		// 		ExitCode: 1,
		// 	},
		// 	Asserts: []func(*e2e.Task) error{
		// 		e2e.AssertByExitCode,
		// 	},
		// },
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
			Name:          "happy-path",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", filepath.Join(currentFolder, "happy-path")},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "happy-path/expectations/pipeline.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
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
			Name:    "push-metadata",
			Command: binary,
			Args:    []string{"run", "--push-metadata", "--only", "push-metadata", filepath.Join(currentFolder, "bigquery-metadata")},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 1,
				Contains: []string{"Starting: shopify_raw.products:metadata-push", "Starting: shopify_raw.inventory_items:metadata-push"},
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByContains,
			},
		},
		{
			Name:    "validate-happy-path",
			Command: binary,
			Args:    []string{"validate", filepath.Join(currentFolder, "happy-path")},
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
			Args:    []string{"run", "--use-uv", filepath.Join(currentFolder, "happy-path")},
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
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "happy-path/assets/asset.py")},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "happy-path/expectations/asset.py.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-games",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "happy-path/assets/chess_games.asset.yml")},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "happy-path/expectations/chess_games.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
			},
		},
		{
			Name:          "parse-asset-happy-path-chess-profiles",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "happy-path/assets/chess_profiles.asset.yml")},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "happy-path/expectations/chess_profiles.asset.yml.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
			},
		},
		{
			Name:          "parse-asset-happy-path-player-summary",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", filepath.Join(currentFolder, "happy-path/assets/player_summary.sql")},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "happy-path/expectations/player_summary.sql.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
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
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "missing-upstream/expectations/missing_upstream.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
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
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections_schema.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
			},
		},
		{
			Name:          "connections-list",
			Command:       binary,
			Args:          []string{"connections", "list", "-o", "json", currentFolder},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "expected_connections.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
			},
		},
		{
			Name:          "parse-pipeline-lineage",
			Command:       binary,
			Args:          []string{"internal", "parse-pipeline", "-c", filepath.Join(currentFolder, "lineage")},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "lineage/expectations/lineage.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
			},
		},
		{
			Name:          "parse-asset-lineage-example",
			Command:       binary,
			Args:          []string{"internal", "parse-asset", "-c", filepath.Join(currentFolder, "lineage/assets/example.sql")},
			Env:           []string{},
			SkipJsonNodes: []string{"\"path\""},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   helpers.ReadFile(filepath.Join(currentFolder, "lineage/expectations/lineage-asset.json")),
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJson,
			},
		},
	}
	for _, test := range tests {
		if err := test.Run(); err != nil {
			fmt.Printf("%s Assert error: %v\n", test.Name, err)
			os.Exit(1)
		}
	}
}
