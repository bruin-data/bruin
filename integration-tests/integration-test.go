package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/bruin-data/bruin/pkg/scheduler"
	jd "github.com/josephburnett/jd/lib"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

var currentFolder string

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
	// utput("internal parse-pipeline happy-path", "happy-path/expectations/pipeline.yml.json")
	// 	expectExitCode("run --tag include --exclude-tag exclude chess-extended", 0)
	// 	expectExitCode("run --tag include --exclude-tag exclude chess-extended/expectations/chess_games.asset.yml", 1)
	// 	expectOutputIncludes(
	// 		"run --tag include --exclude-tag exclude --only checks chess-extended",
	// 		0,
	// 		[]string{"Executed 1 tasks", "total_games:positive"},
	// 	)
	// 	expectExitCode("format --fail-if-changed ./chess-extended/assets/chess_games.asset.yml", 0)
	// 	expectOutputIncludes(
	// 		"run --tag include --exclude-tag exclude --only main chess-extended",
	// 		0,
	// 		[]string{"Executed 3 tasks", " Finished: chess_playground.games", "Finished: chess_playground.profiles", "Finished: chess_playground.game_outcome_summary"},
	// 	)
	// 	expectOutputIncludes(
	// 		"run --push-metadata --only push-metadata bigquery-metadata",
	// 		0,
	// 		[]string{" Starting: shopify_raw.products:metadata-push", "Starting: shopify_raw.inventory_items:metadata-push"},
	// 	)
	// 	expectExitCode("validate happy-path", 0)
	// 	expectExitCode("run --use-uv happy-path", 0)
	// 	// expectExitCode("run happy-path", 0)
	// 	expectJSONOutput(
	// 		"internal parse-asset happy-path/assets/asset.py",
	// 		"happy-path/expectations/asset.py.json",
	// 	)
	// 	expectJSONOutput(
	// 		"internal parse-asset happy-path/assets/chess_games.asset.yml",
	// 		"happy-path/expectations/chess_games.asset.yml.json",
	// 	)
	// 	expectJSONOutput(
	// 		"internal parse-asset happy-path/assets/chess_profiles.asset.yml",
	// 		"happy-path/expectations/chess_profiles.asset.yml.json",
	// 	)
	// 	expectJSONOutput(
	// 		"internal parse-asset happy-path/assets/player_summary.sql",
	// 		"happy-path/expectations/player_summary.sql.json",
	// 	)

	// 	expectOutputIncludes(
	// 		"internal parse-asset faulty-pipeline/assets/error.sql",
	// 		1,
	// 		[]string{"error creating asset from file", "unmarshal errors"},
	// 	)

	// 	expectJSONOutput(
	// 		"validate -o json missing-upstream/assets/nonexistent.sql",
	// 		"missing-upstream/expectations/missing_upstream.json",
	// 	)

	// 	expectOutputIncludes(
	// 		"run malformed/assets/malformed.sql",
	// 		1,
	// 		[]string{"Parser Error: syntax error at or near \"S_ELECT_\"", "Failed assets 1"},
	// 	)

	// 	expectJSONOutput(
	// 		"internal connections",
	// 		"expected_connections_schema.json",
	// 	)
	// 	expectJSONOutput(
	// 		"connections list -o json",
	// 		"expected_connections.json",
	// 	)

	// 	expectJSONOutput(
	// 		"internal parse-pipeline -c lineage",
	// 		"lineage/expectations/lineage.json",
	// 	)

	// 	expectJSONOutput(
	// 		"internal parse-asset -c lineage/assets/example.sql",
	// 		"lineage/expectations/lineage-asset.json",
	// 	)

	// expectJSONO
	expectExitCode("run ./continue", 1)

	expectedState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), &scheduler.PipelineState{
		Parameters: scheduler.RunConfig{
			Downstream:   false,
			Workers:      16,
			Environment:  "",
			Force:        false,
			PushMetadata: false,
			NoLogFile:    false,
			FullRefresh:  false,
			UseUV:        false,
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
	})
	if err := copyFile(filepath.Join(currentFolder, "./player_summary.sql"), filepath.Join(currentFolder, "continue/assets/player_summary.sql")); err != nil {
		fmt.Println("Failed to copy file:", err)
		os.Exit(1)
	}
	expectExitCode("run --continue ./continue", 0)
	expectedState(filepath.Join(currentFolder, "/logs/runs/continue_duckdb"), &scheduler.PipelineState{
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
			UseUV:        false,
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
	})
}

func expectOutputIncludes(command string, code int, contains []string) {
	output, exitCode, err := runCommandWithExitCode(command)
	if err != nil {
		if exitCode != code {
			fmt.Println("Failed:", err)
			os.Exit(1)
		}
	}

	for _, c := range contains {
		if !strings.Contains(output, c) {
			fmt.Printf("Failed:, output of '%s does not contain '%s'", command, c)
			os.Exit(1)
		}
	}

	fmt.Println("Passed")
}

func expectJSONOutput(command string, jsonFilePath string) {
	output, err := runCommand(command)
	if err != nil {
		fmt.Println("Failed:", err)
		os.Exit(1)
	}

	expectation, err := jd.ReadJsonFile(filepath.Join(currentFolder, jsonFilePath))
	if err != nil {
		fmt.Println("Failed to read expectation:", err)
		os.Exit(1)
	}

	// normalize linebreaks
	parsedOutput, err := jd.ReadJsonString(strings.ReplaceAll(output, "\\r\\n", "\\n"))
	if err != nil {
		fmt.Println("Failed to parse output:", err)
		os.Exit(1)
	}

	diff := expectation.Diff(parsedOutput)
	if len(diff) != 0 {
		var path jd.JsonNode
		for _, d := range diff {
			// Paths are hell to normalize, we skip
			path = d.Path[len(d.Path)-1]
			if path.Json() == "\"path\"" {
				continue
			}
			fmt.Println("Mismatch at: ", d.Path)
			fmt.Print("Expected json: ")
			fmt.Println(d.NewValues)
			fmt.Print("Not Matching found json: ")
			fmt.Println(d.OldValues)

			os.Exit(1)
		}
	}
	fmt.Println("Passed")
}

func expectExitCode(command string, code int) {
	output, exitCode, err := runCommandWithExitCode(command)
	if err != nil {
		if exitCode != code {
			fmt.Println(strconv.Itoa(code) + " Was expected but got:" + strconv.Itoa(exitCode))
			fmt.Printf("Error: %v\n", err)
			fmt.Println(output)
			os.Exit(1)
		}
		// Handle other errors
		fmt.Printf("Error running command: %v\n", err)
		fmt.Println(output)
		return
	}

	fmt.Println("Passed")
}

func runCommand(command string) (string, error) {
	fmt.Println("Running command: bruin", command)
	args := strings.Split(command, " ")
	executable := "bruin"
	if runtime.GOOS == "windows" {
		executable = "bruin.exe"
	}
	wd, _ := os.Getwd()
	binary := filepath.Join(wd, "bin", executable)
	cmd := exec.Command(binary, args...)

	cmd.Dir = currentFolder
	output, err := cmd.Output()

	return string(output), err
}

func runCommandWithExitCode(command string) (string, int, error) {
	output, err := runCommand(command)
	if err != nil {
		// Try to get the exit code
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			// Get the status code
			if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
				return output, status.ExitStatus(), nil
			}
		}
		return output, 1, err
	}
	return output, 0, nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	return nil
}

func readState(dir string) *scheduler.PipelineState {
	state, err := scheduler.ReadState(afero.NewOsFs(), dir)
	if err != nil {
		return nil
	}
	return state
}

func expectedState(dir string, expected *scheduler.PipelineState) {
	state := readState(dir)
	if state.Parameters.Workers != expected.Parameters.Workers {
		fmt.Println("Mismatch in Workers")
		os.Exit(1)
	}
	if state.Parameters.Environment != expected.Parameters.Environment {
		fmt.Println("Mismatch in Environment")
		os.Exit(1)
	}

	if state.Metadata.Version != expected.Metadata.Version {
		fmt.Println("Mismatch in Version")
		os.Exit(1)
	}
	if state.Metadata.OS != expected.Metadata.OS {
		fmt.Println("Mismatch in OS")
		os.Exit(1)
	}

	if len(state.State) != len(expected.State) {
		fmt.Println("Mismatch in State length")
		os.Exit(1)
	}

	var dict = make(map[string]string)
	for _, assetState := range state.State {
		dict[assetState.Name] = assetState.Status
	}
	for _, assetState := range expected.State {
		if dict[assetState.Name] != assetState.Status {
			fmt.Println("Mismatch in State")
			os.Exit(1)
		}
	}

	if state.Version != expected.Version {
		fmt.Println("Mismatch in Version")
		os.Exit(1)
	}
	if state.CompatibilityHash != expected.CompatibilityHash {
		fmt.Println("Mismatch in CompatibilityHash")
		os.Exit(1)
	}

	fmt.Println("Passed")
}
