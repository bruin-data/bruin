package connection

import (
	"os"
	"testing"
)

// Test for validateServiceAccountFile function.
func TestValidateServiceAccountFile(t *testing.T) {
	t.Parallel() // Allow this test to run in parallel

	// Create a temporary file for testing.
	tempFile, err := os.CreateTemp("", "service_account.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name()) // Clean up

	// Write valid JSON to the temp file.
	validJSON := []byte(`{"type": "service_account"}`)
	if _, err := tempFile.Write(validJSON); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tempFile.Close()

	// Test valid file.
	if err := validateServiceAccountFile(tempFile.Name()); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	// Test invalid file.
	if err := validateServiceAccountFile("invalid_path.json"); err == nil {
		t.Error("expected error for invalid file path, got none")
	}
}

// Test for validateServiceAccountJSON function.
func TestValidateServiceAccountJSON(t *testing.T) {
	t.Parallel() // Allow this test to run in parallel

	// Test valid JSON string.
	validJSON := `{"type": "service_account"}`
	if err := validateServiceAccountJSON(validJSON); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	// Test invalid JSON string.
	invalidJSON := `{"type": "service_account",}`
	if err := validateServiceAccountJSON(invalidJSON); err == nil {
		t.Error("expected error for invalid JSON format, got none")
	}

	// Test using a file path.
	if err := validateServiceAccountJSON("some_file_path.json"); err == nil {
		t.Error("expected error for file path, got none")
	}
}
