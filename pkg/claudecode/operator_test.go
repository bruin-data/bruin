package claudecode

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
)

func TestExtractParameters(t *testing.T) {
	tests := []struct {
		name        string
		asset       *pipeline.Asset
		expected    *ClaudeParameters
		expectError bool
	}{
		{
			name: "minimal valid parameters",
			asset: &pipeline.Asset{
				Name: "test_asset",
				Parameters: map[string]string{
					"prompt": "Test prompt",
				},
			},
			expected: &ClaudeParameters{
				Prompt:         "Test prompt",
				OutputFormat:   "text",
				PermissionMode: "default",
			},
			expectError: false,
		},
		{
			name: "full parameters",
			asset: &pipeline.Asset{
				Name: "test_asset",
				Parameters: map[string]string{
					"prompt":              "Test prompt",
					"model":               "sonnet",
					"fallback_model":      "haiku",
					"output_format":       "json",
					"system_prompt":       "Be concise",
					"allowed_directories": "/data,/reports",
					"allowed_tools":       "Read,Grep",
					"disallowed_tools":    "Edit,Write",
					"skip_permissions":    "true",
					"permission_mode":     "plan",
					"session_id":          "123e4567-e89b-12d3-a456-426614174000",
					"continue_session":    "false",
					"debug":               "true",
					"verbose":             "false",
				},
			},
			expected: &ClaudeParameters{
				Prompt:          "Test prompt",
				Model:           "sonnet",
				FallbackModel:   "haiku",
				OutputFormat:    "json",
				SystemPrompt:    "Be concise",
				AllowedDirs:     []string{"/data", "/reports"},
				AllowedTools:    "Read,Grep",
				DisallowedTools: "Edit,Write",
				SkipPermissions: true,
				PermissionMode:  "plan",
				SessionID:       "123e4567-e89b-12d3-a456-426614174000",
				ContinueSession: false,
				Debug:           true,
				Verbose:         false,
			},
			expectError: false,
		},
		{
			name: "missing prompt",
			asset: &pipeline.Asset{
				Name: "test_asset",
				Parameters: map[string]string{
					"model": "sonnet",
				},
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "empty prompt",
			asset: &pipeline.Asset{
				Name: "test_asset",
				Parameters: map[string]string{
					"prompt": "  ",
				},
			},
			expected:    nil,
			expectError: true,
		},
		{
			name: "nil parameters",
			asset: &pipeline.Asset{
				Name:       "test_asset",
				Parameters: nil,
			},
			expected:    nil,
			expectError: true,
		},
	}

	operator := &ClaudeCodeOperator{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := operator.extractParameters(tt.asset)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestValidateParameters(t *testing.T) {
	tests := []struct {
		name        string
		params      *ClaudeParameters
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid parameters",
			params: &ClaudeParameters{
				Prompt:         "Test",
				Model:          "sonnet",
				OutputFormat:   "json",
				PermissionMode: "plan",
				SessionID:      "123e4567-e89b-12d3-a456-426614174000",
			},
			expectError: false,
		},
		{
			name: "invalid model",
			params: &ClaudeParameters{
				Prompt:       "Test",
				Model:        "invalid",
				OutputFormat: "text",
				PermissionMode: "default",
			},
			expectError: true,
			errorMsg:    "invalid model",
		},
		{
			name: "invalid output format",
			params: &ClaudeParameters{
				Prompt:       "Test",
				OutputFormat: "xml",
				PermissionMode: "default",
			},
			expectError: true,
			errorMsg:    "invalid output_format",
		},
		{
			name: "invalid permission mode",
			params: &ClaudeParameters{
				Prompt:         "Test",
				OutputFormat:   "text",
				PermissionMode: "invalid",
			},
			expectError: true,
			errorMsg:    "invalid permission_mode",
		},
		{
			name: "invalid session ID format",
			params: &ClaudeParameters{
				Prompt:         "Test",
				OutputFormat:   "text",
				PermissionMode: "default",
				SessionID:      "not-a-uuid",
			},
			expectError: true,
			errorMsg:    "valid UUID",
		},
		{
			name: "allow claude model names",
			params: &ClaudeParameters{
				Prompt:         "Test",
				Model:          "claude-3-5-sonnet-20241022",
				OutputFormat:   "text",
				PermissionMode: "default",
			},
			expectError: false,
		},
	}

	operator := &ClaudeCodeOperator{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := operator.validateParameters(tt.params)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBuildCommand(t *testing.T) {
	tests := []struct {
		name     string
		params   *ClaudeParameters
		expected []string
	}{
		{
			name: "minimal command",
			params: &ClaudeParameters{
				Prompt:         "Test prompt",
				OutputFormat:   "text",
				PermissionMode: "default",
			},
			expected: []string{"-p", "Test prompt"},
		},
		{
			name: "with model and output format",
			params: &ClaudeParameters{
				Prompt:         "Test prompt",
				Model:          "sonnet",
				OutputFormat:   "json",
				PermissionMode: "default",
			},
			expected: []string{"-p", "--output-format", "json", "--model", "sonnet", "Test prompt"},
		},
		{
			name: "full command",
			params: &ClaudeParameters{
				Prompt:          "Test prompt",
				Model:           "sonnet",
				FallbackModel:   "haiku",
				OutputFormat:     "json",
				SystemPrompt:    "Be concise",
				AllowedDirs:     []string{"/data", "/reports"},
				AllowedTools:    "Read,Grep",
				DisallowedTools: "Edit,Write",
				SkipPermissions: true,
				PermissionMode:  "plan",
				SessionID:       "123e4567-e89b-12d3-a456-426614174000",
				Debug:           true,
				Verbose:         true,
			},
			expected: []string{
				"-p",
				"--output-format", "json",
				"--model", "sonnet",
				"--fallback-model", "haiku",
				"--append-system-prompt", "Be concise",
				"--add-dir", "/data",
				"--add-dir", "/reports",
				"--allowed-tools", "Read,Grep",
				"--disallowed-tools", "Edit,Write",
				"--dangerously-skip-permissions",
				"--permission-mode", "plan",
				"--session-id", "123e4567-e89b-12d3-a456-426614174000",
				"--debug",
				"--verbose",
				"Test prompt",
			},
		},
	}

	operator := &ClaudeCodeOperator{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := operator.buildCommand(tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClaudeCodeOperator_Run_InvalidTaskInstance(t *testing.T) {
	renderer := jinja.NewRenderer(make(map[string]interface{}))
	
	operator := NewClaudeCodeOperator(renderer)
	
	// Create a non-AssetInstance type
	invalidInstance := &scheduler.ColumnCheckInstance{}
	
	err := operator.Run(context.Background(), invalidInstance)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "claude_code assets can only be run as a main asset")
}

func TestClaudeCodeOperator_Run_MissingPrompt(t *testing.T) {
	renderer := jinja.NewRenderer(make(map[string]interface{}))
	
	operator := NewClaudeCodeOperator(renderer)
	
	asset := &pipeline.Asset{
		Name:       "test_claude_asset",
		Type:       pipeline.AssetTypeAgentClaudeCode,
		Parameters: map[string]string{},
	}
	
	instance := &scheduler.AssetInstance{
		Asset: asset,
	}
	
	err := operator.Run(context.Background(), instance)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "'prompt' parameter is required")
}

func TestClaudeCodeOperator_Run_InvalidModel(t *testing.T) {
	renderer := jinja.NewRenderer(make(map[string]interface{}))
	
	operator := NewClaudeCodeOperator(renderer)
	
	asset := &pipeline.Asset{
		Name:       "test_claude_asset",
		Type:       pipeline.AssetTypeAgentClaudeCode,
		Parameters: map[string]string{
			"prompt": "Test prompt",
			"model":  "invalid-model",
		},
	}
	
	instance := &scheduler.AssetInstance{
		Asset: asset,
	}
	
	err := operator.Run(context.Background(), instance)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid model")
}