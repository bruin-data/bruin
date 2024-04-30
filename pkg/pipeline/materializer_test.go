package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializer_Render(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		matMap      AssetMaterializationMap
		fullRefresh bool
		query       string
		expected    string
	}{
		{
			name: "no full refresh, remove comments",
			matMap: AssetMaterializationMap{
				MaterializationTypeTable: {
					MaterializationStrategyMerge: func(task *Asset, query string) (string, error) {
						return query, nil
					},
				},
			},
			fullRefresh: false,
			query:       "/* @bruin some yaml @bruin*/SELECT * FROM table",
			expected:    "SELECT * FROM table",
		},
		{
			name: "full refresh",
			matMap: AssetMaterializationMap{
				MaterializationTypeTable: {
					MaterializationStrategyCreateReplace: func(task *Asset, query string) (string, error) {
						return "SELECT 1;" + query, nil
					},
				},
			},
			fullRefresh: true,
			query:       "SELECT * FROM table",
			expected:    "SELECT 1;SELECT * FROM table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			materializer := Materializer{
				MaterializationMap: tt.matMap,
				FullRefresh:        tt.fullRefresh,
			}

			asset := &Asset{
				Materialization: Materialization{
					Type:     MaterializationTypeTable,
					Strategy: MaterializationStrategyMerge,
				},
			}

			result, err := materializer.Render(asset, tt.query)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
