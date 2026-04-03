package connection

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// ServiceAccountFileError provides detailed error information for service account file issues.
type ServiceAccountFileError struct {
	FilePath    string
	Issue       string
	Hint        string
	OriginalErr error
}

func (e *ServiceAccountFileError) Error() string {
	var sb strings.Builder
	sb.WriteString(e.Issue)

	if e.Hint != "" {
		sb.WriteString("\n\n")
		sb.WriteString("Hint: ")
		sb.WriteString(e.Hint)
	}

	return sb.String()
}

func (e *ServiceAccountFileError) Unwrap() error {
	return e.OriginalErr
}

// validateServiceAccountFile validates the service account file path and contents.
func validateServiceAccountFile(filePath string) error {
	// Check if the path looks like JSON content instead of a file path
	var jsonStr json.RawMessage
	if err := json.Unmarshal([]byte(filePath), &jsonStr); err == nil {
		return &ServiceAccountFileError{
			FilePath: filePath,
			Issue:    "the 'service_account_file' field contains JSON content instead of a file path",
			Hint:     "Use 'service_account_json' to provide JSON credentials directly, or use 'service_account_file' with a path to a credentials file.",
		}
	}

	// Check if the file exists
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		absPath, _ := filepath.Abs(filePath)
		return &ServiceAccountFileError{
			FilePath:    filePath,
			Issue:       "service account file not found at '" + filePath + "'",
			Hint:        formatServiceAccountFileHint(filePath, absPath),
			OriginalErr: err,
		}
	}
	if err != nil {
		return &ServiceAccountFileError{
			FilePath:    filePath,
			Issue:       "cannot access service account file at '" + filePath + "': " + err.Error(),
			Hint:        "Check that the file exists and you have read permissions.",
			OriginalErr: err,
		}
	}

	// Check if it's actually a file (not a directory)
	if fileInfo.IsDir() {
		return &ServiceAccountFileError{
			FilePath: filePath,
			Issue:    "'" + filePath + "' is a directory, not a file",
			Hint:     "Provide the path to the service account JSON file, e.g., 'credentials/service-account.json'.",
		}
	}

	// Read and validate the file contents
	file, err := os.ReadFile(filePath)
	if err != nil {
		return &ServiceAccountFileError{
			FilePath:    filePath,
			Issue:       "failed to read service account file at '" + filePath + "': " + err.Error(),
			Hint:        "Check that the file has proper read permissions.",
			OriginalErr: err,
		}
	}

	// Validate that the file contains valid JSON
	var js json.RawMessage
	if err := json.Unmarshal(file, &js); err != nil {
		return &ServiceAccountFileError{
			FilePath:    filePath,
			Issue:       "invalid JSON format in service account file at '" + filePath + "'",
			Hint:        "Ensure the file contains valid JSON. You can download a new service account key from Google Cloud Console: IAM & Admin > Service Accounts > Keys > Add Key > Create new key > JSON.",
			OriginalErr: err,
		}
	}

	// Validate that it looks like a service account key
	var keyData map[string]interface{}
	if err := json.Unmarshal(file, &keyData); err == nil {
		if _, hasType := keyData["type"]; !hasType {
			return &ServiceAccountFileError{
				FilePath: filePath,
				Issue:    "the file at '" + filePath + "' does not appear to be a valid service account key (missing 'type' field)",
				Hint:     "Download a new service account key from Google Cloud Console: IAM & Admin > Service Accounts > Keys > Add Key > Create new key > JSON.",
			}
		}
		if keyType, ok := keyData["type"].(string); ok && keyType != "service_account" {
			return &ServiceAccountFileError{
				FilePath: filePath,
				Issue:    "the file at '" + filePath + "' has type '" + keyType + "' instead of 'service_account'",
				Hint:     "Ensure you're using a service account key file, not an OAuth client or other credential type.",
			}
		}
	}

	return nil
}

// formatServiceAccountFileHint provides helpful guidance for file not found errors.
func formatServiceAccountFileHint(originalPath, absPath string) string {
	var hints []string

	// Check if it's a relative path
	if !filepath.IsAbs(originalPath) {
		hints = append(hints, "The path '"+originalPath+"' is relative to your .bruin.yml file location.")
		hints = append(hints, "Resolved absolute path: "+absPath)
		hints = append(hints, "You can use either:")
		hints = append(hints, "  - A relative path from the .bruin.yml file (e.g., 'credentials/service-account.json')")
		hints = append(hints, "  - An absolute path (e.g., '/home/user/credentials/service-account.json')")
	}

	hints = append(hints, "To create a new service account key:")
	hints = append(hints, "  1. Go to Google Cloud Console > IAM & Admin > Service Accounts")
	hints = append(hints, "  2. Select your service account (or create one)")
	hints = append(hints, "  3. Go to Keys > Add Key > Create new key > JSON")
	hints = append(hints, "  4. Save the downloaded file and update service_account_file in .bruin.yml")

	return strings.Join(hints, "\n")
}

// validateServiceAccountJSON validates the service account JSON content.
func validateServiceAccountJSON(jsonStr string) error {
	// Check if the path exists and is a file (user probably meant to use service_account_file)
	if _, err := os.Stat(jsonStr); err == nil {
		return &ServiceAccountFileError{
			FilePath: jsonStr,
			Issue:    "the 'service_account_json' field contains a file path instead of JSON content",
			Hint:     "Use 'service_account_file' to provide a path to a credentials file, or paste the JSON content directly into 'service_account_json'.",
		}
	}

	var js json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &js); err != nil {
		return &ServiceAccountFileError{
			Issue:       "invalid JSON format in service_account_json",
			Hint:        "Ensure the value is valid JSON. If you're providing a file path, use 'service_account_file' instead.",
			OriginalErr: err,
		}
	}

	// Validate that it looks like a service account key
	var keyData map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &keyData); err == nil {
		if _, hasType := keyData["type"]; !hasType {
			return &ServiceAccountFileError{
				Issue: "service_account_json does not appear to be a valid service account key (missing 'type' field)",
				Hint:  "Ensure you're using JSON from a service account key file. You can generate one from Google Cloud Console: IAM & Admin > Service Accounts > Keys.",
			}
		}
		if keyType, ok := keyData["type"].(string); ok && keyType != "service_account" {
			return &ServiceAccountFileError{
				Issue: "service_account_json has type '" + keyType + "' instead of 'service_account'",
				Hint:  "Ensure you're using a service account key, not an OAuth client or other credential type.",
			}
		}
	}

	return nil
}
