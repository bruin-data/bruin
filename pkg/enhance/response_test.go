package enhance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseClaudeResponse(t *testing.T) {
	t.Run("parses valid JSON response", func(t *testing.T) {
		response := `{
			"asset_description": "Test asset description",
			"column_descriptions": {
				"id": "Unique identifier",
				"name": "User name"
			},
			"column_checks": {
				"id": [
					{"name": "not_null", "reasoning": "ID should not be null"},
					{"name": "unique", "reasoning": "ID should be unique"}
				]
			},
			"suggested_tags": ["users", "core"],
			"suggested_owner": "team@example.com",
			"suggested_domains": ["identity"]
		}`

		suggestions, err := ParseClaudeResponse(response)

		require.NoError(t, err)
		assert.Equal(t, "Test asset description", suggestions.AssetDescription)
		assert.Equal(t, "Unique identifier", suggestions.ColumnDescriptions["id"])
		assert.Equal(t, "User name", suggestions.ColumnDescriptions["name"])
		assert.Len(t, suggestions.ColumnChecks["id"], 2)
		assert.Equal(t, "not_null", suggestions.ColumnChecks["id"][0].Name)
		assert.Equal(t, "unique", suggestions.ColumnChecks["id"][1].Name)
		assert.Equal(t, []string{"users", "core"}, suggestions.SuggestedTags)
		assert.Equal(t, "team@example.com", suggestions.SuggestedOwner)
		assert.Equal(t, []string{"identity"}, suggestions.SuggestedDomains)
	})

	t.Run("parses JSON from markdown code block", func(t *testing.T) {
		response := "Here's my analysis:\n\n```json\n{\"asset_description\": \"Test\"}\n```\n\nHope this helps!"

		suggestions, err := ParseClaudeResponse(response)

		require.NoError(t, err)
		assert.Equal(t, "Test", suggestions.AssetDescription)
	})

	t.Run("parses JSON with surrounding text", func(t *testing.T) {
		response := "Based on my analysis: {\"asset_description\": \"Test\"} that's my suggestion."

		suggestions, err := ParseClaudeResponse(response)

		require.NoError(t, err)
		assert.Equal(t, "Test", suggestions.AssetDescription)
	})

	t.Run("returns error for empty response", func(t *testing.T) {
		_, err := ParseClaudeResponse("")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty response")
	})

	t.Run("returns error for no JSON found", func(t *testing.T) {
		_, err := ParseClaudeResponse("This is just plain text without any JSON")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no valid JSON found")
	})

	t.Run("filters invalid check types", func(t *testing.T) {
		response := `{
			"column_checks": {
				"id": [
					{"name": "not_null"},
					{"name": "invalid_check_type"},
					{"name": "unique"}
				]
			}
		}`

		suggestions, err := ParseClaudeResponse(response)

		require.NoError(t, err)
		assert.Len(t, suggestions.ColumnChecks["id"], 2)
		assert.Equal(t, "not_null", suggestions.ColumnChecks["id"][0].Name)
		assert.Equal(t, "unique", suggestions.ColumnChecks["id"][1].Name)
	})

	t.Run("trims whitespace from descriptions", func(t *testing.T) {
		response := `{
			"asset_description": "  Test description  ",
			"column_descriptions": {
				"id": "  Trimmed  "
			}
		}`

		suggestions, err := ParseClaudeResponse(response)

		require.NoError(t, err)
		assert.Equal(t, "Test description", suggestions.AssetDescription)
		assert.Equal(t, "Trimmed", suggestions.ColumnDescriptions["id"])
	})

	t.Run("removes empty column descriptions", func(t *testing.T) {
		response := `{
			"column_descriptions": {
				"id": "Valid",
				"name": "   "
			}
		}`

		suggestions, err := ParseClaudeResponse(response)

		require.NoError(t, err)
		assert.Len(t, suggestions.ColumnDescriptions, 1)
		assert.Equal(t, "Valid", suggestions.ColumnDescriptions["id"])
	})

	t.Run("filters empty tags", func(t *testing.T) {
		response := `{
			"suggested_tags": ["valid", "", "  ", "also_valid"]
		}`

		suggestions, err := ParseClaudeResponse(response)

		require.NoError(t, err)
		assert.Equal(t, []string{"valid", "also_valid"}, suggestions.SuggestedTags)
	})
}

func TestExtractJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple JSON object",
			input:    `{"key": "value"}`,
			expected: `{"key": "value"}`,
		},
		{
			name:     "JSON with surrounding text",
			input:    `Here is the JSON: {"key": "value"} and more text`,
			expected: `{"key": "value"}`,
		},
		{
			name:     "nested JSON object",
			input:    `{"outer": {"inner": "value"}}`,
			expected: `{"outer": {"inner": "value"}}`,
		},
		{
			name:     "JSON in markdown code block",
			input:    "```json\n{\"key\": \"value\"}\n```",
			expected: `{"key": "value"}`,
		},
		{
			name:     "JSON with strings containing braces",
			input:    `{"pattern": "^{[a-z]+}$"}`,
			expected: `{"pattern": "^{[a-z]+}$"}`,
		},
		{
			name:     "no JSON",
			input:    "plain text without json",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJSON(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIsValidCheckValue(t *testing.T) {
	tests := []struct {
		name      string
		checkName string
		value     interface{}
		valid     bool
	}{
		// Checks that don't need values
		{"not_null without value", "not_null", nil, true},
		{"unique without value", "unique", nil, true},
		{"positive without value", "positive", nil, true},
		{"negative without value", "negative", nil, true},
		{"non_negative without value", "non_negative", nil, true},

		// min/max checks
		{"min with float", "min", float64(10), true},
		{"min with int", "min", 10, true},
		{"min without value", "min", nil, false},
		{"max with float", "max", float64(100), true},

		// accepted_values checks
		{"accepted_values with array", "accepted_values", []interface{}{"a", "b"}, true},
		{"accepted_values with empty array", "accepted_values", []interface{}{}, false},
		{"accepted_values without value", "accepted_values", nil, false},

		// pattern checks
		{"pattern with string", "pattern", "^[a-z]+$", true},
		{"pattern with empty string", "pattern", "", false},
		{"pattern without value", "pattern", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidCheckValue(tt.checkName, tt.value)
			assert.Equal(t, tt.valid, got)
		})
	}
}
