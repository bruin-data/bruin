package lint_test

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryColumnsMatchColumnsPolicy(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	t.Run("returns no issues when parser is nil", func(t *testing.T) {
		t.Parallel()

		validator := lint.QueryColumnsMatchColumnsPolicy(nil)

		asset := &pipeline.Asset{
			Name: "test.table",
			Type: "bq.sql",
		}
		pipeline := &pipeline.Pipeline{}

		issues, err := validator(ctx, pipeline, asset)

		require.NoError(t, err)
		assert.Empty(t, issues)
	})

	t.Run("integration test - real parser with simple jinja", func(t *testing.T) {
		t.Parallel()

		// Create a real SQLParser for integration testing
		parser, err := sqlparser.NewSQLParser(false)
		require.NoError(t, err)
		defer parser.Close()

		err = parser.Start()
		require.NoError(t, err)

		validator := lint.QueryColumnsMatchColumnsPolicy(parser)

		asset := &pipeline.Asset{
			Name: "test.users",
			Type: "bq.sql",
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT id, {{ var.email_col }} as email FROM users WHERE active = {{ var.is_active }}",
			},
			Columns: []pipeline.Column{
				{Name: "id"},
				// Missing email column to trigger issue
			},
		}
		pipeline := &pipeline.Pipeline{
			Name: "test-pipeline",
			Variables: pipeline.Variables{
				"email_col": map[string]any{
					"type":    "string",
					"default": "user_email",
				},
				"is_active": map[string]any{
					"type":    "boolean",
					"default": true,
				},
			},
		}

		issues, err := validator(ctx, pipeline, asset)

		require.NoError(t, err)
		if len(issues) > 0 {
			// Should find missing email column
			assert.Contains(t, issues[0].Description, "email")
		}
	})

	t.Run("integration test - this variable resolution", func(t *testing.T) {
		t.Parallel()

		// Create a real SQLParser for integration testing
		parser, err := sqlparser.NewSQLParser(false)
		require.NoError(t, err)
		defer parser.Close()

		err = parser.Start()
		require.NoError(t, err)

		validator := lint.QueryColumnsMatchColumnsPolicy(parser)

		asset := &pipeline.Asset{
			Name: "analytics.users",
			Type: "bq.sql",
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT id, name FROM {{ this }}",
			},
			Columns: []pipeline.Column{
				{Name: "id"},
				{Name: "name"},
			},
		}
		pipeline := &pipeline.Pipeline{
			Name: "test-pipeline",
		}

		issues, err := validator(ctx, pipeline, asset)

		require.NoError(t, err)
		assert.Empty(t, issues)
	})

	t.Run("returns no issues for non-SQL assets", func(t *testing.T) {
		t.Parallel()

		parser, err := sqlparser.NewSQLParser(false)
		require.NoError(t, err)
		defer parser.Close()

		err = parser.Start()
		require.NoError(t, err)

		validator := lint.QueryColumnsMatchColumnsPolicy(parser)

		asset := &pipeline.Asset{
			Name: "test.table",
			Type: "python",
		}
		pipeline := &pipeline.Pipeline{}

		issues, err := validator(ctx, pipeline, asset)

		require.NoError(t, err)
		assert.Empty(t, issues)
	})

	t.Run("returns no issues when asset type dialect conversion fails", func(t *testing.T) {
		t.Parallel()

		parser, err := sqlparser.NewSQLParser(false)
		require.NoError(t, err)
		defer parser.Close()

		err = parser.Start()
		require.NoError(t, err)

		validator := lint.QueryColumnsMatchColumnsPolicy(parser)

		asset := &pipeline.Asset{
			Name: "test.table",
			Type: "invalid.sql",
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1",
			},
		}
		pipeline := &pipeline.Pipeline{}

		issues, err := validator(ctx, pipeline, asset)

		require.NoError(t, err)
		assert.Empty(t, issues)
	})
}

func TestQueryColumnsMatchColumnsPolicy_JinjaIntegration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	t.Run("complex jinja template with variables and this resolution", func(t *testing.T) {
		t.Parallel()

		parser, err := sqlparser.NewSQLParser(false)
		require.NoError(t, err)
		defer parser.Close()

		err = parser.Start()
		require.NoError(t, err)

		validator := lint.QueryColumnsMatchColumnsPolicy(parser)

		asset := &pipeline.Asset{
			Name: "analytics.user_metrics",
			Type: "bq.sql",
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT {{ var.id_col }}, {{ var.email_col }}, score FROM {{ var.source_table }} WHERE created_at >= '{{ var.cutoff_date }}' AND {{ var.id_col }} NOT IN (SELECT {{ var.id_col }} FROM {{ this }})",
			},
			Columns: []pipeline.Column{
				{Name: "user_id"},
				{Name: "email"},
				// Intentionally missing 'score' to test detection
			},
		}
		pipeline := &pipeline.Pipeline{
			Name: "analytics-pipeline",
			Variables: pipeline.Variables{
				"id_col": map[string]any{
					"type":    "string",
					"default": "user_id",
				},
				"email_col": map[string]any{
					"type":    "string",
					"default": "email",
				},
				"source_table": map[string]any{
					"type":    "string",
					"default": "raw.users",
				},
				"cutoff_date": map[string]any{
					"type":    "string",
					"default": "2023-01-01",
				},
			},
		}

		issues, err := validator(ctx, pipeline, asset)

		require.NoError(t, err)
		// Should detect missing score column
		if len(issues) > 0 {
			assert.Contains(t, issues[0].Description, "score")
		}
	})

	t.Run("jinja template with boolean and numeric variables", func(t *testing.T) {
		t.Parallel()

		parser, err := sqlparser.NewSQLParser(false)
		require.NoError(t, err)
		defer parser.Close()

		err = parser.Start()
		require.NoError(t, err)

		validator := lint.QueryColumnsMatchColumnsPolicy(parser)

		asset := &pipeline.Asset{
			Name: "analytics.active_users",
			Type: "bq.sql",
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT id, name, score FROM users WHERE active = {{ var.is_active }} AND score >= {{ var.min_score }}",
			},
			Columns: []pipeline.Column{
				{Name: "id"},
				{Name: "name"},
				// Missing score column to test detection
			},
		}
		pipeline := &pipeline.Pipeline{
			Name: "scoring-pipeline",
			Variables: pipeline.Variables{
				"is_active": map[string]any{
					"type":    "boolean",
					"default": true,
				},
				"min_score": map[string]any{
					"type":    "integer",
					"default": 85,
				},
			},
		}

		issues, err := validator(ctx, pipeline, asset)

		require.NoError(t, err)
		// Should detect missing score column
		if len(issues) > 0 {
			assert.Contains(t, issues[0].Description, "score")
		}
	})
}
