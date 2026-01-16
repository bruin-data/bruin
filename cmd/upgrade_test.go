package cmd

import (
	"testing"
)

func TestUpgradeCommand(t *testing.T) {
	t.Parallel()
	cmd := Upgrade()

	if cmd.Name != "upgrade" {
		t.Errorf("Expected command name to be 'upgrade', got %s", cmd.Name)
	}

	// Check that 'update' is an alias
	found := false
	for _, alias := range cmd.Aliases {
		if alias == "update" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected 'update' to be an alias for 'upgrade'")
	}

	if cmd.Usage != "Upgrade Bruin CLI to the latest version" {
		t.Errorf("Expected usage to be 'Upgrade Bruin CLI to the latest version', got %s", cmd.Usage)
	}

	if cmd.Action == nil {
		t.Error("Expected command action to be defined")
	}
}

func TestGetOSName(t *testing.T) {
	t.Parallel()
	osName := getOSName()
	// Just ensure it returns something non-empty
	if osName == "" {
		t.Error("Expected getOSName to return a non-empty string")
	}
}

func TestGetArchName(t *testing.T) {
	t.Parallel()
	archName := getArchName()
	// Just ensure it returns something non-empty
	if archName == "" {
		t.Error("Expected getArchName to return a non-empty string")
	}
}

func TestBuildDownloadURL(t *testing.T) {
	t.Parallel()
	url := buildDownloadURL("v1.0.0")
	if url == "" {
		t.Error("Expected buildDownloadURL to return a non-empty string")
	}
	// Check it contains expected parts
	if !contains(url, "github.com/bruin-data/bruin/releases/download") {
		t.Error("Expected URL to contain github.com/bruin-data/bruin/releases/download")
	}
	if !contains(url, "v1.0.0") {
		t.Error("Expected URL to contain version")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
