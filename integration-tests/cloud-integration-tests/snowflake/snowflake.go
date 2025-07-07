package snowflake

import (
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/e2e"
)

// GetTasks returns Snowflake-specific integration test tasks
func GetTasks(binary string, currentFolder string) []e2e.Task {
	configFlags := []string{"--config-file", filepath.Join(currentFolder, "integration-tests/cloud-integration-tests/.bruin.cloud.yml")}

	// Example tasks for Snowflake
	tasks := []e2e.Task{
		{
			Name:    "[snowflake] query 'select 1'",
			Command: binary,
			Args:    []string{"query", "--env", "default", "--connection", "snowflake-default", "--query", "SELECT 1", "--output", "json"},
			Env:     []string{},
			Expected: e2e.Output{
				ExitCode: 0,
				Output:   `{"columns":[{"name":"1","type":"FIXED"}],"rows":[["1"]],"connectionName":"snowflake-default","query":"SELECT 1"}`,
			},
			Asserts: []func(*e2e.Task) error{
				e2e.AssertByExitCode,
				e2e.AssertByOutputJSON,
			},
		},
	}

	for i := range tasks {
		tasks[i].Args = append(tasks[i].Args, configFlags...)
	}

	return tasks
}

// GetWorkflows returns Snowflake-specific integration test workflows
func GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
	// Example workflows for Snowflake
	workflows := []e2e.Workflow{
		{
			Name: "[snowflake] example workflow",
			Steps: []e2e.Task{
				{
					Name:    "placeholder snowflake workflow step",
					Command: "echo",
					Args:    []string{"Snowflake workflow step would go here"},
					Env:     []string{},
					Expected: e2e.Output{
						ExitCode: 0,
						Output:   "Snowflake workflow step would go here\n",
					},
					Asserts: []func(*e2e.Task) error{
						e2e.AssertByExitCode,
						e2e.AssertByOutputString,
					},
				},
			},
		},
	}

	return workflows
} 