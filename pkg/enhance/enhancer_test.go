package enhance

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestNewEnhancer(t *testing.T) {
	fs := afero.NewMemMapFs()

	t.Run("uses default model when empty", func(t *testing.T) {
		enhancer := NewEnhancer(fs, "")

		assert.Equal(t, defaultModel, enhancer.GetModel())
	})

	t.Run("uses provided model", func(t *testing.T) {
		enhancer := NewEnhancer(fs, "claude-opus-4-20250514")

		assert.Equal(t, "claude-opus-4-20250514", enhancer.GetModel())
	})
}

func TestNewEnhancerWithAPIKey(t *testing.T) {
	fs := afero.NewMemMapFs()

	enhancer := NewEnhancerWithAPIKey(fs, "claude-sonnet-4-20250514", "sk-test-key")

	assert.Equal(t, "claude-sonnet-4-20250514", enhancer.GetModel())
	assert.Equal(t, "sk-test-key", enhancer.apiKey)
}

func TestEnhancer_SetAPIKey(t *testing.T) {
	fs := afero.NewMemMapFs()
	enhancer := NewEnhancer(fs, "")

	enhancer.SetAPIKey("sk-new-key")

	assert.Equal(t, "sk-new-key", enhancer.apiKey)
}

func TestEnhancer_FilterExistingSuggestions(t *testing.T) {
	fs := afero.NewMemMapFs()
	enhancer := NewEnhancer(fs, "")

	t.Run("filters out existing description", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name:        "test",
			Description: "Existing description",
		}
		suggestions := &EnhancementSuggestions{
			AssetDescription: "New description",
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.Empty(t, filtered.AssetDescription)
	})

	t.Run("keeps description when asset has none", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
		}
		suggestions := &EnhancementSuggestions{
			AssetDescription: "New description",
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.Equal(t, "New description", filtered.AssetDescription)
	})

	t.Run("filters out existing column descriptions", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
			Columns: []pipeline.Column{
				{Name: "col1", Description: "Existing"},
				{Name: "col2"},
			},
		}
		suggestions := &EnhancementSuggestions{
			ColumnDescriptions: map[string]string{
				"col1": "New desc for col1",
				"col2": "New desc for col2",
			},
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.NotContains(t, filtered.ColumnDescriptions, "col1")
		assert.Equal(t, "New desc for col2", filtered.ColumnDescriptions["col2"])
	})

	t.Run("filters out existing checks", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
			Columns: []pipeline.Column{
				{
					Name: "id",
					Checks: []pipeline.ColumnCheck{
						{Name: "not_null"},
					},
				},
			},
		}
		suggestions := &EnhancementSuggestions{
			ColumnChecks: map[string][]CheckSuggestion{
				"id": {
					{Name: "not_null"}, // exists
					{Name: "unique"},   // new
				},
			},
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.Len(t, filtered.ColumnChecks["id"], 1)
		assert.Equal(t, "unique", filtered.ColumnChecks["id"][0].Name)
	})

	t.Run("filters out existing tags", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
			Tags: []string{"existing"},
		}
		suggestions := &EnhancementSuggestions{
			SuggestedTags: []string{"existing", "new"},
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.Equal(t, []string{"new"}, filtered.SuggestedTags)
	})

	t.Run("filters out existing owner", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name:  "test",
			Owner: "existing@example.com",
		}
		suggestions := &EnhancementSuggestions{
			SuggestedOwner: "new@example.com",
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.Empty(t, filtered.SuggestedOwner)
	})

	t.Run("filters out existing domains", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name:    "test",
			Domains: []string{"existing"},
		}
		suggestions := &EnhancementSuggestions{
			SuggestedDomains: []string{"existing", "new"},
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.Equal(t, []string{"new"}, filtered.SuggestedDomains)
	})

	t.Run("handles nil suggestions", func(t *testing.T) {
		asset := &pipeline.Asset{Name: "test"}

		filtered := enhancer.filterExistingSuggestions(nil, asset)

		assert.Nil(t, filtered)
	})

	t.Run("filters column checks for non-existent columns", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: "test",
			Columns: []pipeline.Column{
				{Name: "id"},
			},
		}
		suggestions := &EnhancementSuggestions{
			ColumnChecks: map[string][]CheckSuggestion{
				"id":              {{Name: "unique"}},
				"nonexistent_col": {{Name: "not_null"}},
			},
		}

		filtered := enhancer.filterExistingSuggestions(suggestions, asset)

		assert.Contains(t, filtered.ColumnChecks, "id")
		assert.NotContains(t, filtered.ColumnChecks, "nonexistent_col")
	})
}

func TestGetColumnByName(t *testing.T) {
	asset := &pipeline.Asset{
		Columns: []pipeline.Column{
			{Name: "ID"},
			{Name: "email"},
		},
	}

	t.Run("finds column case-insensitively", func(t *testing.T) {
		col := getColumnByName(asset, "id")
		assert.NotNil(t, col)
		assert.Equal(t, "ID", col.Name)

		col = getColumnByName(asset, "EMAIL")
		assert.NotNil(t, col)
		assert.Equal(t, "email", col.Name)
	})

	t.Run("returns nil for non-existent column", func(t *testing.T) {
		col := getColumnByName(asset, "nonexistent")
		assert.Nil(t, col)
	})
}

func TestEnhancer_IsClaudeCLIInstalled(t *testing.T) {
	fs := afero.NewMemMapFs()

	t.Run("returns false when path is empty", func(t *testing.T) {
		enhancer := &Enhancer{fs: fs, model: defaultModel, claudePath: ""}

		assert.False(t, enhancer.IsClaudeCLIInstalled())
	})

	t.Run("returns true when path is set", func(t *testing.T) {
		enhancer := &Enhancer{fs: fs, model: defaultModel, claudePath: "/usr/local/bin/claude"}

		assert.True(t, enhancer.IsClaudeCLIInstalled())
	})
}
