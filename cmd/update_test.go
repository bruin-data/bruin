package cmd

import (
	"testing"
)

func TestUpdateCommand(t *testing.T) {
	t.Parallel()
	cmd := Update()

	if cmd.Name != "update" {
		t.Errorf("Expected command name to be 'update', got %s", cmd.Name)
	}

	if cmd.Usage != "Update Bruin CLI to the latest version" {
		t.Errorf("Expected usage to be 'Update Bruin CLI to the latest version', got %s", cmd.Usage)
	}

	if cmd.Action == nil {
		t.Error("Expected command action to be defined")
	}
}

func TestIsCommandAvailable(t *testing.T) {
	t.Parallel()
	// Test with a command that should exist on most systems
	if !isCommandAvailable("sh") {
		t.Error("Expected 'sh' command to be available")
	}

	// Test with a command that should not exist
	if isCommandAvailable("nonexistentcommand12345") {
		t.Error("Expected 'nonexistentcommand12345' to not be available")
	}
}
