package enhance

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestEnhancementSuggestions_IsEmpty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		suggestions *EnhancementSuggestions
		want        bool
	}{
		{
			name:        "nil suggestions",
			suggestions: nil,
			want:        true,
		},
		{
			name:        "empty suggestions",
			suggestions: &EnhancementSuggestions{},
			want:        true,
		},
		{
			name: "with asset description",
			suggestions: &EnhancementSuggestions{
				AssetDescription: "test description",
			},
			want: false,
		},
		{
			name: "with column descriptions",
			suggestions: &EnhancementSuggestions{
				ColumnDescriptions: map[string]string{"col1": "desc1"},
			},
			want: false,
		},
		{
			name: "with column checks",
			suggestions: &EnhancementSuggestions{
				ColumnChecks: map[string][]CheckSuggestion{
					"col1": {{Name: "not_null"}},
				},
			},
			want: false,
		},
		{
			name: "with tags",
			suggestions: &EnhancementSuggestions{
				SuggestedTags: []string{"tag1"},
			},
			want: false,
		},
		{
			name: "with owner",
			suggestions: &EnhancementSuggestions{
				SuggestedOwner: "owner@example.com",
			},
			want: false,
		},
		{
			name: "with domains",
			suggestions: &EnhancementSuggestions{
				SuggestedDomains: []string{"domain1"},
			},
			want: false,
		},
		{
			name: "with custom checks",
			suggestions: &EnhancementSuggestions{
				CustomChecks: []CustomCheckSuggestion{{Name: "check1"}},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.suggestions.IsEmpty()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestApplySuggestions(t *testing.T) {
	t.Parallel()
	t.Run("applies asset description when empty", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Name: "test_asset"}
		suggestions := &EnhancementSuggestions{
			AssetDescription: "New description",
		}

		ApplySuggestions(asset, suggestions)

		assert.Equal(t, "New description", asset.Description)
	})

	t.Run("does not overwrite existing description", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name:        "test_asset",
			Description: "Existing description",
		}
		suggestions := &EnhancementSuggestions{
			AssetDescription: "New description",
		}

		ApplySuggestions(asset, suggestions)

		assert.Equal(t, "Existing description", asset.Description)
	})

	t.Run("applies column descriptions", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_asset",
			Columns: []pipeline.Column{
				{Name: "col1"},
				{Name: "col2", Description: "existing"},
			},
		}
		suggestions := &EnhancementSuggestions{
			ColumnDescriptions: map[string]string{
				"col1": "description for col1",
				"col2": "should not apply",
			},
		}

		ApplySuggestions(asset, suggestions)

		assert.Equal(t, "description for col1", asset.Columns[0].Description)
		assert.Equal(t, "existing", asset.Columns[1].Description)
	})

	t.Run("applies column checks", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_asset",
			Columns: []pipeline.Column{
				{Name: "id"},
			},
		}
		suggestions := &EnhancementSuggestions{
			ColumnChecks: map[string][]CheckSuggestion{
				"id": {
					{Name: "not_null"},
					{Name: "unique"},
				},
			},
		}

		ApplySuggestions(asset, suggestions)

		assert.Len(t, asset.Columns[0].Checks, 2)
		assert.Equal(t, "not_null", asset.Columns[0].Checks[0].Name)
		assert.Equal(t, "unique", asset.Columns[0].Checks[1].Name)
	})

	t.Run("does not duplicate existing checks", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_asset",
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
					{Name: "not_null"}, // already exists
					{Name: "unique"},   // new
				},
			},
		}

		ApplySuggestions(asset, suggestions)

		assert.Len(t, asset.Columns[0].Checks, 2)
		assert.Equal(t, "not_null", asset.Columns[0].Checks[0].Name)
		assert.Equal(t, "unique", asset.Columns[0].Checks[1].Name)
	})

	t.Run("applies tags without duplicates", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_asset",
			Tags: []string{"existing_tag"},
		}
		suggestions := &EnhancementSuggestions{
			SuggestedTags: []string{"existing_tag", "new_tag"},
		}

		ApplySuggestions(asset, suggestions)

		assert.Len(t, asset.Tags, 2)
		assert.Contains(t, asset.Tags, "existing_tag")
		assert.Contains(t, asset.Tags, "new_tag")
	})

	t.Run("applies owner when empty", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Name: "test_asset"}
		suggestions := &EnhancementSuggestions{
			SuggestedOwner: "owner@example.com",
		}

		ApplySuggestions(asset, suggestions)

		assert.Equal(t, "owner@example.com", asset.Owner)
	})

	t.Run("does not overwrite existing owner", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name:  "test_asset",
			Owner: "existing@example.com",
		}
		suggestions := &EnhancementSuggestions{
			SuggestedOwner: "new@example.com",
		}

		ApplySuggestions(asset, suggestions)

		assert.Equal(t, "existing@example.com", asset.Owner)
	})

	t.Run("handles nil asset", func(t *testing.T) {
		t.Parallel()
		suggestions := &EnhancementSuggestions{
			AssetDescription: "test",
		}

		// Should not panic
		ApplySuggestions(nil, suggestions)
	})

	t.Run("handles nil suggestions", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Name: "test_asset"}

		// Should not panic
		ApplySuggestions(asset, nil)
	})
}

func TestCreateColumnCheck(t *testing.T) {
	t.Parallel()
	t.Run("creates check without value", func(t *testing.T) {
		t.Parallel()
		suggestion := CheckSuggestion{
			Name: "not_null",
		}

		check := createColumnCheck(suggestion)

		assert.Equal(t, "not_null", check.Name)
	})

	t.Run("creates check with int value", func(t *testing.T) {
		t.Parallel()
		suggestion := CheckSuggestion{
			Name:  "min",
			Value: float64(10), // JSON unmarshals numbers as float64
		}

		check := createColumnCheck(suggestion)

		assert.Equal(t, "min", check.Name)
		assert.NotNil(t, check.Value.Int)
		assert.Equal(t, 10, *check.Value.Int)
	})

	t.Run("creates check with string value", func(t *testing.T) {
		t.Parallel()
		suggestion := CheckSuggestion{
			Name:  "pattern",
			Value: "^[a-z]+$",
		}

		check := createColumnCheck(suggestion)

		assert.Equal(t, "pattern", check.Name)
		assert.NotNil(t, check.Value.String)
		assert.Equal(t, "^[a-z]+$", *check.Value.String)
	})

	t.Run("creates check with string array value", func(t *testing.T) {
		t.Parallel()
		suggestion := CheckSuggestion{
			Name:  "accepted_values",
			Value: []interface{}{"active", "inactive", "pending"},
		}

		check := createColumnCheck(suggestion)

		assert.Equal(t, "accepted_values", check.Name)
		assert.NotNil(t, check.Value.StringArray)
		assert.Equal(t, []string{"active", "inactive", "pending"}, *check.Value.StringArray)
	})
}

func TestContainsString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		slice []string
		s     string
		want  bool
	}{
		{
			name:  "contains string",
			slice: []string{"a", "b", "c"},
			s:     "b",
			want:  true,
		},
		{
			name:  "does not contain string",
			slice: []string{"a", "b", "c"},
			s:     "d",
			want:  false,
		},
		{
			name:  "empty slice",
			slice: []string{},
			s:     "a",
			want:  false,
		},
		{
			name:  "nil slice",
			slice: nil,
			s:     "a",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := containsString(tt.slice, tt.s)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHasCheck(t *testing.T) {
	t.Parallel()
	checks := []pipeline.ColumnCheck{
		{Name: "not_null"},
		{Name: "unique"},
	}

	assert.True(t, hasCheck(checks, "not_null"))
	assert.True(t, hasCheck(checks, "unique"))
	assert.False(t, hasCheck(checks, "positive"))
	assert.False(t, hasCheck(nil, "not_null"))
}
