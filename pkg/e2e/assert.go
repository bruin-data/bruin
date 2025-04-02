package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/scheduler"
	jd "github.com/josephburnett/jd/lib"
	"github.com/spf13/afero"
)

func AssertByExitCode(i *Task) error {
	if i.Actual.ExitCode != i.Expected.ExitCode {
		return fmt.Errorf("exit code mismatch: expected %d, got %d", i.Expected.ExitCode, i.Actual.ExitCode)
	}
	return nil
}

func AssertByOutputString(i *Task) error {
	if i.Actual.Output != i.Expected.Output {
		return fmt.Errorf("output mismatch: expected %q, got %q", i.Expected.Output, i.Actual.Output)
	}
	return nil
}

func AssertByOutputJSON(i *Task) error {
	expectation, err := jd.ReadJsonString(i.Expected.Output)
	if err != nil {
		return errors.New("Failed to read expectation: " + err.Error())
	}

	// normalize linebreaks
	parsedOutput, err := jd.ReadJsonString(strings.ReplaceAll(i.Actual.Output, "\\r\\n", "\\n"))
	if err != nil {
		return errors.New("Failed to parse output: " + err.Error())
	}

	diff := expectation.Diff(parsedOutput)

	if len(diff) != 0 {
		var path jd.JsonNode
		for _, d := range diff {
			path = d.Path[len(d.Path)-1]
			if len(i.SkipJSONNodes) > 0 {
				exists := false
				for _, skip := range i.SkipJSONNodes {
					if path.Json() == skip {
						exists = true
					}
				}
				if !exists {
					return fmt.Errorf(
						"mismatch at path %v. Expected json: %v, but found: %v",
						d.Path, d.NewValues, d.OldValues,
					)
				}
			} else {
				return fmt.Errorf(
					"mismatch at path %v. Expected json: %v, but found: %v",
					d.Path, d.NewValues, d.OldValues,
				)
			}
		}
	}
	return nil
}

func AssertByContains(i *Task) error {
	for _, expected := range i.Expected.Contains {
		if !strings.Contains(i.Actual.Output, expected) {
			return fmt.Errorf("output does not contain expected substring: %q", expected)
		}
	}
	return nil
}

func AssertByError(i *Task) error {
	if i.Actual.Error != i.Expected.Error {
		return fmt.Errorf("error mismatch: expected %q, got %q", i.Expected.Error, i.Actual.Error)
	}
	return nil
}

func AssertCustomState(dir string, expected *scheduler.PipelineState) func(*Task) error {
	return func(i *Task) error {
		state, err := scheduler.ReadState(afero.NewOsFs(), dir)
		if err != nil {
			return fmt.Errorf("failed to read state from directory %s: %w", dir, err)
		}
		if state.Parameters.Workers != expected.Parameters.Workers {
			return fmt.Errorf("mismatch in Workers: expected %d, got %d", expected.Parameters.Workers, state.Parameters.Workers)
		}
		if state.Parameters.Environment != expected.Parameters.Environment {
			return fmt.Errorf("mismatch in Environment: expected %s, got %s", expected.Parameters.Environment, state.Parameters.Environment)
		}

		if state.Metadata.Version != expected.Metadata.Version {
			return fmt.Errorf("mismatch in Version: expected %s, got %s", expected.Metadata.Version, state.Metadata.Version)
		}
		if state.Metadata.OS != expected.Metadata.OS {
			return fmt.Errorf("mismatch in OS: expected %s, got %s", expected.Metadata.OS, state.Metadata.OS)
		}

		if len(state.State) != len(expected.State) {
			return fmt.Errorf("mismatch in State length: expected %d, got %d", len(expected.State), len(state.State))
		}

		dict := make(map[string]string)
		for _, assetState := range state.State {
			dict[assetState.Name] = assetState.Status
		}
		for _, assetState := range expected.State {
			if dict[assetState.Name] != assetState.Status {
				return fmt.Errorf("mismatch in State for asset %s: expected %s, got %s", assetState.Name, assetState.Status, dict[assetState.Name])
			}
		}

		if state.Version != expected.Version {
			return fmt.Errorf("mismatch in Version: expected %s, got %s", expected.Version, state.Version)
		}
		if state.CompatibilityHash != expected.CompatibilityHash {
			return fmt.Errorf("mismatch in CompatibilityHash: expected %s, got %s", expected.CompatibilityHash, state.CompatibilityHash)
		}

		return nil
	}
}

func AssertByYAML(i *Task) error {
	fs := afero.NewOsFs()

	actualBytes, err := afero.ReadFile(fs, i.WorkingDir+"/.bruin.yml")
	if err != nil {
		return fmt.Errorf("failed to read .bruin.yaml file: %w", err)
	}

	actualContent := strings.TrimSpace(strings.ReplaceAll(string(actualBytes), "\r\n", "\n"))
	expectedContent := strings.TrimSpace(strings.ReplaceAll(i.Expected.Output, "\r\n", "\n"))

	if actualContent != expectedContent {
		return fmt.Errorf("YAML content mismatch:\nexpected:\n%s\ngot:\n%s", expectedContent, actualContent)
	}

	return nil
}

func AssertByQueryResultCSV(i *Task) error {
	fs := afero.NewOsFs()

	var result map[string]string
	if err := json.Unmarshal([]byte(i.Actual.Output), &result); err != nil {
		return fmt.Errorf("failed to parse JSON output: %w", err)
	}

	actualPath, ok := result["Results Successfully exported to"]
	if !ok {
		return fmt.Errorf("could not find file path in output: %s", i.Actual.Output)
	}

	// Read the actual file
	actualBytes, err := afero.ReadFile(fs, actualPath)
	if err != nil {
		return fmt.Errorf("failed to read actual CSV file at %s: %w", actualPath, err)
	}

	// Use expected content directly since it's already the file content
	expectedContent := strings.TrimSpace(strings.ReplaceAll(i.Expected.Output, "\r\n", "\n"))
	actualContent := strings.TrimSpace(strings.ReplaceAll(string(actualBytes), "\r\n", "\n"))

	if expectedContent != actualContent {
		expectedLines := strings.Split(expectedContent, "\n")
		actualLines := strings.Split(actualContent, "\n")

		if len(expectedLines) != len(actualLines) {
			return fmt.Errorf("CSV line count mismatch: expected %d lines, got %d lines",
				len(expectedLines), len(actualLines))
		}
		for idx, expectedLine := range expectedLines {
			if idx >= len(actualLines) {
				break
			}
			if expectedLine != actualLines[idx] {
				return fmt.Errorf("CSV mismatch at line %d:\nexpected: %s\ngot: %s",
					idx+1, expectedLine, actualLines[idx])
			}
		}
	}

	return nil
}
