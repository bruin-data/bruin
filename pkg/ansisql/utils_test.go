package ansisql

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddAnnotationComment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		annotations string
		query       string
		expectError bool
		expected    string
	}{
		{
			name:        "valid JSON annotations",
			annotations: `{"project": "test", "pipeline": "test-pipeline"}`,
			query:       "SELECT * FROM table",
			expectError: false,
			expected:    "SELECT * FROM table", // We'll check the JSON separately
		},
		{
			name:        "invalid JSON annotations",
			annotations: `{"project": "test", "pipeline": "test-pipeline"`,
			query:       "SELECT * FROM table",
			expectError: true,
			expected:    "",
		},
		{
			name:        "empty annotations",
			annotations: "",
			query:       "SELECT * FROM table",
			expectError: false,
			expected:    "SELECT * FROM table",
		},
		{
			name:        "no annotations in context",
			annotations: "",
			query:       "SELECT * FROM table",
			expectError: false,
			expected:    "SELECT * FROM table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.annotations != "" {
				ctx = context.WithValue(ctx, pipeline.RunConfigQueryAnnotations, tt.annotations)
			}

			q := &query.Query{Query: tt.query}
			resultQuery, err := AddAnnotationComment(ctx, q, "test-asset", "main", "test-pipeline")

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, resultQuery)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resultQuery)
				// Original query should remain unchanged
				assert.Equal(t, tt.query, q.Query)

				if tt.name == "valid JSON annotations" {
					// Check that the result query starts with the comment and contains the required fields
					assert.Contains(t, resultQuery.Query, "-- @bruin.config:")
					assert.Contains(t, resultQuery.Query, `"asset":"test-asset"`)
					assert.Contains(t, resultQuery.Query, `"type":"main"`)
					assert.Contains(t, resultQuery.Query, `"pipeline":"test-pipeline"`)
					assert.Contains(t, resultQuery.Query, `"project":"test"`)
					assert.Contains(t, resultQuery.Query, "SELECT * FROM table")
				} else {
					assert.Equal(t, tt.expected, resultQuery.Query)
				}
			}
		})
	}
}

func TestAddAgentIDAnnotationComment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		agentID  string
		query    string
		expected string
	}{
		{
			name:     "with agent ID",
			agentID:  "my-agent-123",
			query:    "SELECT * FROM table",
			expected: `-- @bruin.config: {"agent_id":"my-agent-123","type":"adhoc_query"}` + "\n" + "SELECT * FROM table",
		},
		{
			name:     "empty agent ID",
			agentID:  "",
			query:    "SELECT * FROM table",
			expected: "SELECT * FROM table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := &query.Query{Query: tt.query}
			result := AddAgentIDAnnotationComment(q, tt.agentID)

			assert.NotNil(t, result)
			assert.Equal(t, tt.expected, result.Query)
			// Original query should remain unchanged
			assert.Equal(t, tt.query, q.Query)
		})
	}
}

func TestBuildAnnotationJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		annotations string
		fields      map[string]interface{}
		expectError bool
		expectEmpty bool
		contains    []string
	}{
		{
			name:        "default annotations with standard fields",
			annotations: "default",
			fields:      map[string]interface{}{"asset": "test-asset", "type": "main", "pipeline": "test-pipeline"},
			contains:    []string{`"asset":"test-asset"`, `"type":"main"`, `"pipeline":"test-pipeline"`},
		},
		{
			name:        "custom JSON merged with fields",
			annotations: `{"project": "test"}`,
			fields:      map[string]interface{}{"asset": "test-asset", "type": "main"},
			contains:    []string{`"asset":"test-asset"`, `"type":"main"`, `"project":"test"`},
		},
		{
			name:        "no annotations returns empty",
			annotations: "",
			fields:      map[string]interface{}{"asset": "test-asset"},
			expectEmpty: true,
		},
		{
			name:        "invalid JSON returns error",
			annotations: `{"project": "test"`,
			fields:      map[string]interface{}{"asset": "test-asset"},
			expectError: true,
		},
		{
			name:        "single-quoted JSON annotations are trimmed",
			annotations: `'{"project": "test"}'`,
			fields:      map[string]interface{}{"asset": "test-asset", "type": "main"},
			contains:    []string{`"asset":"test-asset"`, `"type":"main"`, `"project":"test"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.annotations != "" {
				ctx = context.WithValue(ctx, pipeline.RunConfigQueryAnnotations, tt.annotations)
			}

			result, err := BuildAnnotationJSON(ctx, tt.fields)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.expectEmpty {
				assert.Empty(t, result)
				return
			}

			for _, s := range tt.contains {
				assert.Contains(t, result, s)
			}
			assert.NotContains(t, result, "-- @bruin.config")
		})
	}
}

func TestBuildAgentIDQueryTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		agentID  string
		expected string
	}{
		{
			name:     "with agent ID",
			agentID:  "my-agent-123",
			expected: `{"agent_id":"my-agent-123","type":"adhoc_query"}`,
		},
		{
			name:     "empty agent ID returns empty JSON",
			agentID:  "",
			expected: `{"agent_id":"","type":"adhoc_query"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := BuildAgentIDQueryTag(tt.agentID)
			assert.Equal(t, tt.expected, result)
		})
	}
}
