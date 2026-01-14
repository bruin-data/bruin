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

type stringMaterializer struct {
	out string
}

func (m stringMaterializer) Render(_ *Asset, _ string) (string, error) {
	return m.out, nil
}

type listMaterializer struct {
	out []string
}

func (m listMaterializer) Render(_ *Asset, _ string) ([]string, error) {
	return m.out, nil
}

type listWithLocationMaterializer struct {
	out         []string
	gotLocation string
}

func (m *listWithLocationMaterializer) Render(_ *Asset, _ string, location string) ([]string, error) {
	m.gotLocation = location
	return m.out, nil
}

func TestHookWrapperMaterializer_Render(t *testing.T) {
	t.Parallel()

	asset := &Asset{
		Hooks: Hooks{
			Pre:  []Hook{{Query: "select 1"}},
			Post: []Hook{{Query: "select 2"}},
		},
	}

	wrapper := HookWrapperMaterializer{
		Mat: stringMaterializer{out: "select 3"},
	}

	got, err := wrapper.Render(asset, "ignored")
	require.NoError(t, err)
	assert.Equal(t, "select 1;\nselect 3;\nselect 2;", got)
}

func TestHookWrapperMaterializerList_Render(t *testing.T) {
	t.Parallel()

	asset := &Asset{
		Hooks: Hooks{
			Pre:  []Hook{{Query: "select 1"}},
			Post: []Hook{{Query: "select 2"}},
		},
	}

	wrapper := HookWrapperMaterializerList{
		Mat: listMaterializer{out: []string{"select 3"}},
	}

	got, err := wrapper.Render(asset, "ignored")
	require.NoError(t, err)
	assert.Equal(t, []string{"select 1;", "select 3", "select 2;"}, got)
}

func TestHookWrapperMaterializerListWithLocation_Render(t *testing.T) {
	t.Parallel()

	asset := &Asset{
		Hooks: Hooks{
			Pre:  []Hook{{Query: "select 1"}},
			Post: []Hook{{Query: "select 2"}},
		},
	}
	base := &listWithLocationMaterializer{out: []string{"select 3"}}

	wrapper := HookWrapperMaterializerListWithLocation{
		Mat: base,
	}

	got, err := wrapper.Render(asset, "ignored", "s3://bucket")
	require.NoError(t, err)
	assert.Equal(t, []string{"select 1;", "select 3", "select 2;"}, got)
	assert.Equal(t, "s3://bucket", base.gotLocation)
}
