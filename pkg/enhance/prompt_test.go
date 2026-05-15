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
		assert.Contains(t, prompt, "enum checks for status fields")
	})

	t.Run("includes pre-fetched stats when provided", func(t *testing.T) {
		t.Parallel()
		statsJSON := `{"table_name": "test", "columns": []}`
		prompt := BuildEnhancePrompt("/path/to/asset.sql", "test", "pipeline", statsJSON)

		assert.Contains(t, prompt, "PRE-FETCHED TABLE STATISTICS")
		assert.Contains(t, prompt, statsJSON)
	})
}
