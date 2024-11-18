package connection

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

func getProjectRoot() (string, error) {
	// Get the absolute path of the executable
	executablePath, err := os.Executable()
	if err != nil {
		return "", errors.New("failed to determine executable path: " + err.Error())
	}
	// The project root is the parent directory of the "bin" directory
	return filepath.Dir(filepath.Dir(executablePath)), nil
}

// Resolve file paths relative to the project root directory
func resolveFilePath(relativePath string) (string, error) {
	projectRoot, err := getProjectRoot()
	if err != nil {
		return "", errors.New("failed to determine project root: " + err.Error())
	}
	absPath := filepath.Join(projectRoot, relativePath)
	return absPath, nil
}

// Function to validate the service account file
func validateServiceAccountFile(filePath string) error {
	var jsonStr json.RawMessage
	if err := json.Unmarshal([]byte(filePath), &jsonStr); err == nil {
		return errors.New("please use service_account_json instead of service_account_file to define JSON")
	}

	file, err := os.ReadFile(filePath)
	if err != nil {
		return errors.New("failed to read service account file at '" + filePath + "': " + err.Error())
	}

	var js json.RawMessage
	if err := json.Unmarshal(file, &js); err != nil {
		return errors.New("invalid JSON format in service account file at '" + filePath + "': " + err.Error())
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
