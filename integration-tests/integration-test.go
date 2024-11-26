package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	jd "github.com/josephburnett/jd/lib"
	"github.com/pkg/errors"
)

var currentFolder string

func main() {
	var err error
	currentFolder = filepath.Join(currentFolder, "integration-tests")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	expectExitCode("validate happy-path", 0)
	expectExitCode("run --use-uv happy-path", 0)
	expectExitCode("run --tag include --exclude exclude chess-extended", 0)
	expectFetchQuery(
		"duckdb-default",
		"SELECT * FROM chess_playground.player_summary;",
		"json",
		"chess-extended/expectations/player_summary.json",
	)

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
	output, err := runCommand(command)
	if err != nil {
		// Try to get the exit code
		if errors.As(err, exec.ExitError{}) {
			exitError := err.(*exec.ExitError) //nolint: errorlint
			// Get the status code
			if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
				if status.ExitStatus() != code {
					fmt.Println(strconv.Itoa(code) + " Was expected but got:" + strconv.Itoa(status.ExitStatus()))
					fmt.Println(output)
					os.Exit(1)
				}
			}
		}
		// Handle other errors
		fmt.Printf("Error running command: %v\n", err)
		fmt.Println(output)
		return
	}

	fmt.Println("Passed")
}

func runCommand(command string) (string, error) {
	fmt.Println("Running command:", command)
	args := []string{"run", "../main.go"}
	args = append(args, strings.Split(command, " ")...)
	cmd := exec.Command("go", args...)
	cmd.Dir = currentFolder
	output, err := cmd.Output()

	return string(output), err
}

func expectFetchQuery(connection, query, outputFile, expectationPath string) {
	// Construct the fetch query arguments
	args := []string{"run", "../main.go", "fetch", "query",
		"--connection", connection,
		"--query", query,
		"--output", outputFile,
	}

	// Create the command
	cmd := exec.Command("go", args...)
	cmd.Dir = currentFolder // Set the working directory

	// Print the fetch query being executed
	fmt.Printf("fetch query with arguments: --connection %s --query \"%s\" --output %s\n", connection, query, outputFile)

	// Run the command and capture the output
	outputBytes, err := cmd.CombinedOutput()
	output := string(outputBytes) // Convert output to a string for processing

	if err != nil {
		fmt.Printf("Fetch query failed with error: %v\n", err)
		fmt.Println("Command output:", output)
		os.Exit(1)
	}

	// Verify the output against the expected JSON file
	expectation, err := jd.ReadJsonFile(filepath.Join(currentFolder, expectationPath))
	if err != nil {
		fmt.Println("Failed to read expectation file:", err)
		os.Exit(1)
	}

	// Normalize and parse the output for comparison
	parsedOutput, err := jd.ReadJsonString(strings.ReplaceAll(output, "\\r\\n", "\\n"))
	if err != nil {
		fmt.Println("Failed to parse output JSON:", err)
		os.Exit(1)
	}

	diff := expectation.Diff(parsedOutput)
	if len(diff) != 0 {
		os.Exit(1) // Output mismatch, exit with failure
	}

	// If everything matches, print "Passed"
	fmt.Println("Passed")
}
