package e2e

import (
	"testing"
)

func TestWorkflowRun(t *testing.T) {
	tests := []struct {
		name     string
		workflow Workflow
		wantErr  bool
	}{
		{
			name: "All Steps Pass",
			workflow: Workflow{
				Steps: []Task{
					{
						Command:  "echo",
						Args:     []string{"hello"},
						Expected: Output{ExitCode: 0, Output: "hello\n"},
						Asserts:  []func(*Task) error{AssertByExitCode},
					},
				},
			},
			wantErr: false,
		},

		{
			name: "Step Fails on Output",
			workflow: Workflow{
				Steps: []Task{
					{
						Command:  "echo",
						Args:     []string{"hello"},
						Expected: Output{Output: "world\n"},
						Asserts:  []func(*Task) error{AssertByOutputString},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.workflow.Run()
			if (err != nil) != tt.wantErr {
				t.Errorf("Workflow.Run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
