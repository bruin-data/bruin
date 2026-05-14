package enhance

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildEnhancePrompt(t *testing.T) {
	t.Parallel()
	t.Run("includes asset path and pipeline", func(t *testing.T) {
		t.Parallel()
		prompt := BuildEnhancePrompt("/path/to/asset.sql", "test_asset", "test_pipeline", "")

		assert.Contains(t, prompt, "test_asset")
		assert.Contains(t, prompt, "test_pipeline")
		assert.Contains(t, prompt, "/path/to/asset.sql")
	})

	t.Run("includes check type guidelines", func(t *testing.T) {
		t.Parallel()
		prompt := BuildEnhancePrompt("/path/to/asset.sql", "test", "pipeline", "")

		// Should include guidelines for different check types
		assert.Contains(t, prompt, "not_null")
		assert.Contains(t, prompt, "unique")
		assert.Contains(t, prompt, "enum checks for status fields")
	})

	t.Run("includes pre-fetched stats when provided", func(t *testing.T) {
		t.Parallel()
		statsJSON := `{"table_name": "test", "columns": []}`
		prompt := BuildEnhancePrompt("/path/to/asset.sql", "test", "pipeline", statsJSON)

		assert.Contains(t, prompt, "PRE-FETCHED TABLE STATISTICS")
		assert.Contains(t, prompt, statsJSON)
	})

	t.Run("includes BigQuery cost guard safety instructions", func(t *testing.T) {
		t.Parallel()
		prompt := BuildEnhancePrompt("/path/to/asset.sql", "test", "pipeline", "")

		assert.Contains(t, prompt, "Query Cost Safety")
		assert.Contains(t, prompt, "max_query_cost")
		assert.Contains(t, prompt, "max_query_cost_soft")
		assert.Contains(t, prompt, "max_billable_bytes")
		assert.Contains(t, prompt, "max_billable_bytes_soft")
		assert.Contains(t, prompt, "--dry-run")
		assert.Contains(t, prompt, "--dangerously-bypass-soft-limits")
	})

	t.Run("cost safety section precedes context discovery", func(t *testing.T) {
		t.Parallel()
		prompt := BuildEnhancePrompt("/path/to/asset.sql", "test", "pipeline", "")

		costIdx := strings.Index(prompt, "Query Cost Safety")
		discoveryIdx := strings.Index(prompt, "Context Discovery")

		assert.NotEqual(t, -1, costIdx, "Query Cost Safety section must exist")
		assert.NotEqual(t, -1, discoveryIdx, "Context Discovery section must exist")
		assert.Less(t, costIdx, discoveryIdx, "cost safety must come before context discovery so the agent evaluates it before planning queries")
	})
}
