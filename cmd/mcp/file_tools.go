package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
)

// FileToolsConfig holds configuration for file tools.
type FileToolsConfig struct {
	RepoRoot string
}

// formatToolResult represents the result of a format or validate tool call.
type formatToolResult struct {
	Success bool   `json:"success"`
	Path    string `json:"path"`
	Output  string `json:"output"`
	Error   string `json:"error,omitempty"`
}

// Global file tools config.
var fileToolsConfig *FileToolsConfig

// initFileToolsConfig initializes the file tools configuration.
func initFileToolsConfig() {
	if fileToolsConfig != nil {
		return
	}

	fileToolsConfig = &FileToolsConfig{
		RepoRoot: os.Getenv("BRUIN_REPO_ROOT"),
	}

	// If repo root not set, try to find it from current directory
	if fileToolsConfig.RepoRoot == "" {
		cwd, err := os.Getwd()
		if err == nil {
			repo, err := git.FindRepoFromPath(cwd)
			if err == nil {
				fileToolsConfig.RepoRoot = repo.Path
			}
		}
	}
}

// validatePath ensures the path is within the allowed repo root.
func validatePath(path string) (string, error) {
	initFileToolsConfig()

	if fileToolsConfig.RepoRoot == "" {
		return "", errors.New("no Bruin repository found. Set BRUIN_REPO_ROOT environment variable")
	}

	// Resolve absolute path
	absPath := path
	if !filepath.IsAbs(path) {
		absPath = filepath.Join(fileToolsConfig.RepoRoot, path)
	}

	// Clean the path to remove .. and .
	absPath = filepath.Clean(absPath)

	// Ensure path is within repo root
	repoRoot := filepath.Clean(fileToolsConfig.RepoRoot)
	if !strings.HasPrefix(absPath, repoRoot) {
		return "", fmt.Errorf("path '%s' is outside the repository root", path)
	}

	return absPath, nil
}

// readFile reads the contents of a file.
func readFile(path string) (string, error) {
	absPath, err := validatePath(path)
	if err != nil {
		return "", err
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %w", err)
	}

	return string(content), nil
}

// writeFile writes content to a file.
func writeFile(path, content string) error {
	absPath, err := validatePath(path)
	if err != nil {
		return err
	}

	// Ensure parent directory exists
	dir := filepath.Dir(absPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := os.WriteFile(absPath, []byte(content), 0600); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// runBruinFormat runs bruin format on a file.
func runBruinFormat(ctx context.Context, path string) (string, error) {
	absPath, err := validatePath(path)
	if err != nil {
		return "", err
	}

	bruinPath, err := exec.LookPath("bruin")
	if err != nil {
		return "", errors.New("bruin CLI not found in PATH")
	}

	cmd := exec.CommandContext(ctx, bruinPath, "format", absPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("bruin format failed: %s", string(output))
	}

	return string(output), nil
}

// runBruinValidate runs bruin validate (lint) on a file or pipeline.
func runBruinValidate(ctx context.Context, path string) (string, error) {
	absPath, err := validatePath(path)
	if err != nil {
		return "", err
	}

	bruinPath, err := exec.LookPath("bruin")
	if err != nil {
		return "", errors.New("bruin CLI not found in PATH")
	}

	cmd := exec.CommandContext(ctx, bruinPath, "validate", absPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Return output even on error so Claude can see the validation errors
		return string(output), fmt.Errorf("validation failed: %s", string(output))
	}

	return string(output), nil
}

// HandleFileToolCall handles file-related tool calls.
func HandleFileToolCall(toolName string, args map[string]interface{}, debug bool) (string, error) {
	ctx := context.Background()

	switch toolName {
	case "bruin_read_file":
		path, _ := args["path"].(string)
		if path == "" {
			return formatFileError("read_file", errors.New("path parameter is required")), nil
		}
		content, err := readFile(path)
		if err != nil {
			return formatFileError("read_file", err), nil
		}
		return content, nil

	case "bruin_write_file":
		path, _ := args["path"].(string)
		content, _ := args["content"].(string)
		if path == "" {
			return formatFileError("write_file", errors.New("path parameter is required")), nil
		}
		if err := writeFile(path, content); err != nil {
			return formatFileError("write_file", err), nil
		}
		result := map[string]string{
			"success": "true",
			"path":    path,
			"message": "File written successfully",
		}
		jsonBytes, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal result: %w", err)
		}
		return string(jsonBytes), nil

	case "bruin_format":
		path, _ := args["path"].(string)
		if path == "" {
			return formatFileError("format", errors.New("path parameter is required")), nil
		}
		output, fmtErr := runBruinFormat(ctx, path)
		result := formatToolResult{
			Success: fmtErr == nil,
			Path:    path,
			Output:  output,
		}
		if fmtErr != nil {
			result.Error = fmtErr.Error()
		}
		jsonBytes, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal result: %w", err)
		}
		return string(jsonBytes), nil

	case "bruin_validate":
		path, _ := args["path"].(string)
		if path == "" {
			return formatFileError("validate", errors.New("path parameter is required")), nil
		}
		output, valErr := runBruinValidate(ctx, path)
		result := formatToolResult{
			Success: valErr == nil,
			Path:    path,
			Output:  output,
		}
		if valErr != nil {
			result.Error = valErr.Error()
		}
		jsonBytes, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal result: %w", err)
		}
		return string(jsonBytes), nil

	default:
		return "", fmt.Errorf("unknown file tool: %s", toolName)
	}
}

// fileErrorResult represents an error result from a file tool.
type fileErrorResult struct {
	Error     string `json:"error"`
	Operation string `json:"operation"`
}

func formatFileError(operation string, err error) string {
	result := fileErrorResult{
		Error:     err.Error(),
		Operation: operation,
	}
	jsonBytes, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		return fmt.Sprintf(`{"error": "%s", "operation": "%s"}`, err.Error(), operation)
	}
	return string(jsonBytes)
}

// GetFileToolDefinitions returns the MCP tool definitions for file tools.
func GetFileToolDefinitions() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name":        "bruin_read_file",
			"description": "Read the contents of a file. Use this to read asset YAML files before making modifications.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path": map[string]interface{}{
						"type":        "string",
						"description": "Path to the file (relative to repo root or absolute)",
					},
				},
				"required": []string{"path"},
			},
		},
		{
			"name":        "bruin_write_file",
			"description": "Write content to a file. Use this to save modified asset YAML files. Always run bruin_format after writing.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path": map[string]interface{}{
						"type":        "string",
						"description": "Path to the file (relative to repo root or absolute)",
					},
					"content": map[string]interface{}{
						"type":        "string",
						"description": "The full content to write to the file",
					},
				},
				"required": []string{"path", "content"},
			},
		},
		{
			"name":        "bruin_format",
			"description": "Format a Bruin asset file to ensure proper YAML structure. Always run this after writing or modifying an asset file.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path": map[string]interface{}{
						"type":        "string",
						"description": "Path to the asset file to format",
					},
				},
				"required": []string{"path"},
			},
		},
		{
			"name":        "bruin_validate",
			"description": "Validate a Bruin asset or pipeline. Use this to check for errors after making changes. If validation fails, fix the issues and try again.",
			"inputSchema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"path": map[string]interface{}{
						"type":        "string",
						"description": "Path to the asset file or pipeline directory to validate",
					},
				},
				"required": []string{"path"},
			},
		},
	}
}

// IsFileTool checks if a tool name is a file tool.
func IsFileTool(toolName string) bool {
	fileTools := map[string]bool{
		"bruin_read_file":  true,
		"bruin_write_file": true,
		"bruin_format":     true,
		"bruin_validate":   true,
	}
	return fileTools[toolName]
}
