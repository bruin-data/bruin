package enhance

import (
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
		assert.Contains(t, prompt, "non_negative")
		assert.Contains(t, prompt, "accepted_values")
	})

	t.Run("includes pre-fetched stats when provided", func(t *testing.T) {
		t.Parallel()
		statsJSON := `{"table_name": "test", "columns": []}`
		prompt := BuildEnhancePrompt("/path/to/asset.sql", "test", "pipeline", statsJSON)

		assert.Contains(t, prompt, "PRE-FETCHED TABLE STATISTICS")
		assert.Contains(t, prompt, statsJSON)
	})
}

func TestGetSystemPrompt(t *testing.T) {
	t.Parallel()
	t.Run("without pre-fetched stats", func(t *testing.T) {
		t.Parallel()
		prompt := GetSystemPrompt(false)

		assert.NotEmpty(t, prompt)
		assert.Contains(t, prompt, "data quality expert")
		assert.Contains(t, prompt, "DIRECTLY EDIT")
	})

	t.Run("with pre-fetched stats", func(t *testing.T) {
		t.Parallel()
		prompt := GetSystemPrompt(true)

		assert.NotEmpty(t, prompt)
		assert.Contains(t, prompt, "data quality expert")
		assert.Contains(t, prompt, "DIRECTLY EDIT")
		// Should mention using pre-fetched statistics
		assert.Contains(t, prompt, "PRE-FETCHED")
		// Should mention sample_values in the context of statistics
		assert.Contains(t, prompt, "sample_values")
	})
}
