package pipeline

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveSelectorAssets_Methods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector string
		expected []string
	}{
		{
			name:     "default asset name",
			selector: "fct_orders",
			expected: []string{"fct_orders"},
		},
		{
			name:     "tag selector",
			selector: "tag:nightly",
			expected: []string{"stg_orders", "int_orders"},
		},
		{
			name:     "path selector",
			selector: "path:assets/marts",
			expected: []string{"fct_orders", "audit_orders"},
		},
		{
			name:     "recursive wildcard path selector",
			selector: "path:assets/*",
			expected: []string{"stg_orders", "int_orders", "fct_orders", "audit_orders", "external_seed"},
		},
		{
			name:     "file selector",
			selector: "file:external_seed",
			expected: []string{"external_seed"},
		},
		{
			name:     "fqn selector",
			selector: "fqn:selector_pipeline.assets.marts.fct_orders",
			expected: []string{"fct_orders"},
		},
		{
			name:     "default path selector",
			selector: "assets/staging",
			expected: []string{"stg_orders", "int_orders"},
		},
		{
			name:     "wildcard tag selector",
			selector: "tag:fin*",
			expected: []string{"int_orders", "fct_orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newSelectorTestPipeline(t)
			assets, err := ResolveSelectorAssets(tt.selector, p)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, selectorAssetNames(assets))
		})
	}
}

func TestResolveSelectorAssets_GraphOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector string
		expected []string
	}{
		{
			name:     "all upstream",
			selector: "+fct_orders",
			expected: []string{"stg_orders", "int_orders", "fct_orders"},
		},
		{
			name:     "first degree upstream",
			selector: "1+fct_orders",
			expected: []string{"int_orders", "fct_orders"},
		},
		{
			name:     "all downstream",
			selector: "fct_orders+",
			expected: []string{"fct_orders", "audit_orders"},
		},
		{
			name:     "first degree downstream",
			selector: "stg_orders+1",
			expected: []string{"stg_orders", "int_orders"},
		},
		{
			name:     "upstream and downstream",
			selector: "+int_orders+",
			expected: []string{"stg_orders", "int_orders", "fct_orders", "audit_orders"},
		},
		{
			name:     "at operator includes ancestors of descendants",
			selector: "@fct_orders",
			expected: []string{"stg_orders", "int_orders", "fct_orders", "audit_orders", "external_seed"},
		},
		{
			name:     "at operator includes ancestors of leaf nodes",
			selector: "@audit_orders",
			expected: []string{"stg_orders", "int_orders", "fct_orders", "audit_orders", "external_seed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newSelectorTestPipeline(t)
			assets, err := ResolveSelectorAssets(tt.selector, p)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, selectorAssetNames(assets))
		})
	}
}

func TestResolveSelectorAssets_SetOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector string
		expected []string
	}{
		{
			name:     "space union",
			selector: "tag:nightly tag:qa",
			expected: []string{"stg_orders", "int_orders", "audit_orders"},
		},
		{
			name:     "comma intersection",
			selector: "path:assets/marts,tag:finance",
			expected: []string{"fct_orders"},
		},
		{
			name:     "graph before intersection",
			selector: "+fct_orders,tag:nightly",
			expected: []string{"stg_orders", "int_orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newSelectorTestPipeline(t)
			assets, err := ResolveSelectorAssets(tt.selector, p)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, selectorAssetNames(assets))
		})
	}
}

func TestResolveSelectorAssets_Invalid(t *testing.T) {
	t.Parallel()

	p := newSelectorTestPipeline(t)

	_, err := ResolveSelectorAssets("@fct_orders+", p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot combine @ with + graph operators")

	_, err = ResolveSelectorAssets("tag:missing", p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "matched no assets")
}

func newSelectorTestPipeline(t *testing.T) *Pipeline {
	t.Helper()

	root := t.TempDir()

	return &Pipeline{
		Name: "selector_pipeline",
		DefinitionFile: DefinitionFile{
			Path: filepath.Join(root, "pipeline.yml"),
		},
		Assets: []*Asset{
			{
				Name: "stg_orders",
				Type: AssetTypeBigqueryQuery,
				Tags: []string{"nightly"},
				DefinitionFile: TaskDefinitionFile{
					Path: filepath.Join(root, "assets", "staging", "stg_orders.sql"),
				},
			},
			{
				Name: "int_orders",
				Type: AssetTypeBigqueryQuery,
				Tags: []string{"nightly", "finance"},
				Upstreams: []Upstream{
					{Type: "asset", Value: "stg_orders"},
				},
				DefinitionFile: TaskDefinitionFile{
					Path: filepath.Join(root, "assets", "staging", "int_orders.sql"),
				},
			},
			{
				Name: "fct_orders",
				Type: AssetTypeBigqueryQuery,
				Tags: []string{"finance"},
				Upstreams: []Upstream{
					{Type: "asset", Value: "int_orders"},
				},
				DefinitionFile: TaskDefinitionFile{
					Path: filepath.Join(root, "assets", "marts", "fct_orders.sql"),
				},
			},
			{
				Name: "audit_orders",
				Type: AssetTypeBigqueryQuery,
				Tags: []string{"qa"},
				Upstreams: []Upstream{
					{Type: "asset", Value: "fct_orders"},
					{Type: "asset", Value: "external_seed"},
				},
				DefinitionFile: TaskDefinitionFile{
					Path: filepath.Join(root, "assets", "marts", "audit_orders.sql"),
				},
			},
			{
				Name: "external_seed",
				Type: AssetTypeDuckDBSeed,
				Tags: []string{"seed"},
				DefinitionFile: TaskDefinitionFile{
					Path: filepath.Join(root, "assets", "seeds", "external_seed.asset.yml"),
				},
			},
		},
	}
}

func selectorAssetNames(assets []*Asset) []string {
	names := make([]string, len(assets))
	for i, asset := range assets {
		names[i] = asset.Name
	}
	return names
}
