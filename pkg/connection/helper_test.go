package connection

import (
	"os"
	"testing"
)

// Test for validateServiceAccountFile function.
func TestValidateServiceAccountFile(t *testing.T) {
	t.Parallel()

	// Create a temporary file for testing.
	tempFile, err := os.CreateTemp(t.TempDir(), "service_account.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write valid JSON to the temp file.
	validJSON := []byte(`{
        "type": "service_account",
        "project_id": "bruin-common-health-check",
        "private_key_id": "TEST",
        "private_key": "-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n",
        "client_email": "bruin-health-check@bruin-common-health-check.iam.gserviceaccount.com",
        "client_id": "TEST",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bruin-health-check%40bruin-common-health-check.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    }`)
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

	// Test empty file.
	emptyFile, err := os.CreateTemp(t.TempDir(), "empty_file.json")
	if err != nil {
		t.Fatalf("failed to create empty temp file: %v", err)
	}
	defer os.Remove(emptyFile.Name())
	emptyFile.Close()

	if err := validateServiceAccountFile(emptyFile.Name()); err == nil {
		t.Error("expected error for empty file, got none")
	}

	// Test valid JSON string as filePath.
	jsonString := `{"type": "service_account"}`
	if err := validateServiceAccountFile(jsonString); err == nil {
		t.Error("expected error for valid JSON string as filePath, got none")
	} else if err.Error() != "please use service_account_json instead of service_account_file to define json" {
		t.Errorf("expected specific error message, got: %v", err)
	}
}

// Test for validateServiceAccountJSON function.
func TestValidateServiceAccountJSON(t *testing.T) {
	t.Parallel()

	// Test valid JSON string.
	validJSON := `{
        "type": "service_account",
        "project_id": "bruin-common-health-check",
        "private_key_id": "TEST",
        "private_key": "-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n",
        "client_email": "bruin-health-check@bruin-common-health-check.iam.gserviceaccount.com",
        "client_id": "TEST",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bruin-health-check%40bruin-common-health-check.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    }`
	if err := validateServiceAccountJSON(validJSON); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	// Test invalid JSON string.
	invalidJSON := `{"type": "service_account",}`
	if err := validateServiceAccountJSON(invalidJSON); err == nil {
		t.Error("expected error for invalid JSON format, got none")
	} else if err.Error() != "invalid JSON format in service account JSON" {
		t.Errorf("expected specific error message, got: %v", err)
	}

	// Test using a file path that doesn't exist.
	if err := validateServiceAccountJSON("some_file_path.json"); err == nil {
		t.Error("expected error for file path, got none")
	} else if err.Error() != "invalid JSON format in service account JSON" {
		t.Errorf("expected specific error message, got: %v", err)
	}

	// Test using a file path that exist
	tempFile, err := os.CreateTemp(t.TempDir(), "service_account.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	if _, err := tempFile.WriteString(validJSON); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tempFile.Close()

	if err := validateServiceAccountJSON(tempFile.Name()); err == nil {
		t.Error("expected error for file path, got none")
	} else if err.Error() != "please use service_account_file instead of service_account_json to define path" {
		t.Errorf("expected specific error message, got: %v", err)
	}

	// Test empty JSON string.
	emptyJSON := ``
	if err := validateServiceAccountJSON(emptyJSON); err == nil {
		t.Error("expected error for empty JSON string, got none")
	} else if err.Error() != "invalid JSON format in service account JSON" {
		t.Errorf("expected specific error message, got: %v", err)
	}

	// Test malformed JSON string.
	malformedJSON := `{"type": "service_account", "project_id": "bruin-common-health-check", "private_key_id": "TEST", "private_key": "-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n", "client_email": "bruin-health-check@bruin-common-health-check.iam.gserviceaccount.com", "client_id": "TEST", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bruin-health-check%40bruin-common-health-check.iam.gserviceaccount.com", "universe_domain": "googleapis.com"`
	if err := validateServiceAccountJSON(malformedJSON); err == nil {
		t.Error("expected error for malformed JSON string, got none")
	} else if err.Error() != "invalid JSON format in service account JSON" {
		t.Errorf("expected specific error message, got: %v", err)
	}
}
