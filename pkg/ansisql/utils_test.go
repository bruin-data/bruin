package ansisql

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
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

			ctx := context.Background()
			if tt.annotations != "" {
				ctx = context.WithValue(ctx, pipeline.RunConfigQueryAnnotations, tt.annotations)
			}

			q := &query.Query{Query: tt.query}
			err := AddAnnotationComment(ctx, q, "test-asset", "main", "test-pipeline")

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.name == "valid JSON annotations" {
					// Check that the query starts with the comment and contains the required fields
					assert.Contains(t, q.Query, "-- @bruin.config:")
					assert.Contains(t, q.Query, `"asset":"test-asset"`)
					assert.Contains(t, q.Query, `"type":"main"`)
					assert.Contains(t, q.Query, `"pipeline":"test-pipeline"`)
					assert.Contains(t, q.Query, `"project":"test"`)
					assert.Contains(t, q.Query, "SELECT * FROM table")
				} else {
					assert.Equal(t, tt.expected, q.Query)
				}
			}
		})
	}
}
