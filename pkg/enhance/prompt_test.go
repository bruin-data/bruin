package enhance

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func indexOf(s, substr string) int {
	return strings.Index(s, substr)
}

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
		prompt := GetSystemPrompt(false, "")

		assert.NotEmpty(t, prompt)
		assert.Contains(t, prompt, "data quality expert")
		assert.Contains(t, prompt, "DIRECTLY EDIT")
	})

	t.Run("with pre-fetched stats", func(t *testing.T) {
		t.Parallel()
		prompt := GetSystemPrompt(true, "")

		assert.NotEmpty(t, prompt)
		assert.Contains(t, prompt, "data quality expert")
		assert.Contains(t, prompt, "DIRECTLY EDIT")
		// Should mention using pre-fetched statistics
		assert.Contains(t, prompt, "PRE-FETCHED")
		// Should mention sample_values in the context of statistics
		assert.Contains(t, prompt, "sample_values")
	})

	t.Run("with custom system prompt", func(t *testing.T) {
		t.Parallel()
		custom := "Focus on financial data compliance checks"
		prompt := GetSystemPrompt(false, custom)

		assert.NotEmpty(t, prompt)
		// Custom prompt should be appended
		assert.Greater(t, len(prompt), len(custom))
		assert.Contains(t, prompt, custom)
		assert.Contains(t, prompt, "data quality expert")
		// Custom prompt should appear after the default
		assert.Greater(t,
			indexOf(prompt, custom),
			indexOf(prompt, "data quality expert"),
		)
	})

	t.Run("with custom system prompt and pre-fetched stats", func(t *testing.T) {
		t.Parallel()
		custom := "Always add pattern checks for email columns"
		prompt := GetSystemPrompt(true, custom)

		assert.Contains(t, prompt, custom)
		assert.Contains(t, prompt, "sample_values")
		assert.Greater(t,
			indexOf(prompt, custom),
			indexOf(prompt, "data quality expert"),
		)
	})
}
