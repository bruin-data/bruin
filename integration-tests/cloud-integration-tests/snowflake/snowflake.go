package snowflake

import (
	"github.com/bruin-data/bruin/pkg/e2e"
)

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
