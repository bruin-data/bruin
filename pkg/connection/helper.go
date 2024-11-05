package connection

import (
	"encoding/json"
	"os"

	"github.com/pkg/errors"
)

// New helper function to validate ServiceAccountFile.
func validateServiceAccountFile(filePath string) error {
	var jsonStr json.RawMessage
	if err := json.Unmarshal([]byte(filePath), &jsonStr); err == nil {
		return errors.New("please use service_account_json instead of service_account_file to define json")
	}

	file, err := os.ReadFile(filePath)
	if err != nil {
		return errors.Errorf("failed to read service account file at '%s': %v", filePath, err)
	}
	var js json.RawMessage
	if err := json.Unmarshal(file, &js); err != nil {
		return errors.Errorf("invalid JSON format in service account file at '%s'", filePath)
	}
	return nil
}

// New helper function to validate ServiceAccountJSON.
func validateServiceAccountJSON(jsonStr string) error {
	// Check if the path exists and is a file
	if _, err := os.Stat(jsonStr); err == nil {
		return errors.New("please use service_account_file instead of service_account_json to define path")
	}

	var js json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &js); err != nil {
		return errors.Errorf("invalid JSON format in service account JSON")
	}
	return nil
}
