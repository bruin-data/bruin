package e2e

import (
	"testing"
)

func TestIntegrationTest_Run(t *testing.T) {
	test := &Task{
		Command: "echo",
		Args:    []string{"Hello, World!"},
	}

	err := test.Run()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if test.Actual.Output != "Hello, World!\n" {
		t.Errorf("expected output 'Hello, World!', got %v", test.Actual.Output)
	}
}

func TestIntegrationTest_RunCommandError(t *testing.T) {
	test := &Task{
		Command: "invalid_command",

		Expected: Output{
			ExitCode: 1,
		},
		Asserts: []func(*Task) error{
			AssertByExitCode,
		},
	}

	err := test.Run()
	if err == nil {
		t.Fatalf("expected error for invalid command, got nil")
	}
}
