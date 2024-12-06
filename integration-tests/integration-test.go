package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	jd "github.com/josephburnett/jd/lib"
	"github.com/pkg/errors"
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

	expectExitCode("validate happy-path", 0)
	expectExitCode("run --use-uv happy-path", 0)
	// expectExitCode("run happy-path", 0)
	expectJSONOutput("internal parse-pipeline happy-path", "happy-path/expectations/pipeline.yml.json")
	expectJSONOutput(
		"internal parse-asset happy-path/assets/asset.py",
		"happy-path/expectations/asset.py.json",
	)
	expectJSONOutput(
		"internal parse-asset happy-path/assets/chess_games.asset.yml",
		"happy-path/expectations/chess_games.asset.yml.json",
	)
	expectJSONOutput(
		"internal parse-asset happy-path/assets/chess_profiles.asset.yml",
		"happy-path/expectations/chess_profiles.asset.yml.json",
	)
	expectJSONOutput(
		"internal parse-asset happy-path/assets/player_summary.sql",
		"happy-path/expectations/player_summary.sql.json",
	)

	expectOutputIncludes(
		"internal parse-asset faulty-pipeline/assets/error.sql",
		1,
		[]string{"error creating asset from file", "unmarshal errors"},
	)

	expectJSONOutput(
		"validate -o json missing-upstream/assets/nonexistent.sql",
		"missing-upstream/expectations/missing_upstream.json",
	)

	expectOutputIncludes(
		"run malformed/assets/malformed.sql",
		1,
		[]string{"Parser Error: syntax error at or near \"S_ELECT_\"", "Failed assets 1"},
	)

	expectJSONOutput(
		"internal connections",
		"expected_connections_schema.json",
	)
	expectJSONOutput(
		"connections list -o json",
		"expected_connections.json",
	)

	expectJSONOutput(
		"internal parse-pipeline -c lineage",
		"lineage/expectations/lineage.json",
	)

	expectJSONOutput(
		"internal parse-asset -c lineage/assets/example.sql",
		"lineage/expectations/lineage-asset.json",
	)
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
