package enhance

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestBuildEnhancePrompt(t *testing.T) {
	t.Run("includes asset name and pipeline", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test_asset",
			Type: "duckdb.sql",
		}

		prompt := BuildEnhancePrompt(asset, "test_pipeline")

		assert.Contains(t, prompt, "test_asset")
		assert.Contains(t, prompt, "test_pipeline")
		assert.Contains(t, prompt, "duckdb.sql")
	})

	t.Run("includes column information", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "users",
			Type: "pg.sql",
			Columns: []pipeline.Column{
				{Name: "id", Type: "integer"},
				{Name: "email", Type: "varchar"},
			},
		}

		prompt := BuildEnhancePrompt(asset, "pipeline")

		assert.Contains(t, prompt, "id")
		assert.Contains(t, prompt, "email")
		assert.Contains(t, prompt, "integer")
		assert.Contains(t, prompt, "varchar")
	})

	t.Run("includes existing checks in summary", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "users",
			Type: "pg.sql",
			Columns: []pipeline.Column{
				{
					Name: "id",
					Checks: []pipeline.ColumnCheck{
						{Name: "not_null"},
					},
				},
			},
		}

		prompt := BuildEnhancePrompt(asset, "pipeline")

		assert.Contains(t, prompt, "not_null")
		assert.Contains(t, prompt, "existing_checks")
	})

	t.Run("includes check type guidelines", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
			Type: "sql",
		}

		prompt := BuildEnhancePrompt(asset, "pipeline")

		// Should include guidelines for different check types
		assert.Contains(t, prompt, "not_null")
		assert.Contains(t, prompt, "unique")
		assert.Contains(t, prompt, "positive")
		assert.Contains(t, prompt, "accepted_values")
		assert.Contains(t, prompt, "pattern")
	})

	t.Run("includes JSON schema format", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
			Type: "sql",
		}

		prompt := BuildEnhancePrompt(asset, "pipeline")

		assert.Contains(t, prompt, "asset_description")
		assert.Contains(t, prompt, "column_descriptions")
		assert.Contains(t, prompt, "column_checks")
		assert.Contains(t, prompt, "suggested_tags")
	})
}

func TestGetSystemPrompt(t *testing.T) {
	t.Run("without MCP", func(t *testing.T) {
		prompt := GetSystemPrompt(false)

		assert.NotEmpty(t, prompt)
		assert.Contains(t, prompt, "data quality expert")
		assert.Contains(t, prompt, "JSON")
		assert.NotContains(t, prompt, "MCP")
	})

	t.Run("with MCP", func(t *testing.T) {
		prompt := GetSystemPrompt(true)

		assert.NotEmpty(t, prompt)
		assert.Contains(t, prompt, "data quality expert")
		assert.Contains(t, prompt, "JSON")
		assert.Contains(t, prompt, "MCP")
		// Documentation tools
		assert.Contains(t, prompt, "bruin_get_overview")
		assert.Contains(t, prompt, "bruin_get_docs_tree")
		assert.Contains(t, prompt, "bruin_get_doc_content")
		// Database tools
		assert.Contains(t, prompt, "bruin_list_connections")
		assert.Contains(t, prompt, "bruin_get_table_schema")
		assert.Contains(t, prompt, "bruin_get_column_stats")
		assert.Contains(t, prompt, "bruin_sample_column_values")
	})
}

func TestBuildAssetSummary(t *testing.T) {
	t.Run("includes basic asset info", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name:        "orders",
			Type:        "bq.sql",
			Description: "Order data",
			Owner:       "data-team@example.com",
			Tags:        []string{"finance", "core"},
		}

		summary := buildAssetSummary(asset)

		assert.Contains(t, summary, "orders")
		assert.Contains(t, summary, "bq.sql")
		assert.Contains(t, summary, "Order data")
		assert.Contains(t, summary, "data-team@example.com")
		assert.Contains(t, summary, "finance")
		assert.Contains(t, summary, "core")
	})

	t.Run("returns valid JSON", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
			Type: "sql",
			Columns: []pipeline.Column{
				{Name: "id", Type: "int"},
			},
		}

		summary := buildAssetSummary(asset)

		// Should be valid JSON (starts with { and ends with })
		assert.True(t, strings.HasPrefix(strings.TrimSpace(summary), "{"))
		assert.True(t, strings.HasSuffix(strings.TrimSpace(summary), "}"))
	})
}
