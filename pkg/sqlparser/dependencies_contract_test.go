package sqlparser

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestMissingDependenciesContractAcrossAssetTypes(t *testing.T) {
	t.Parallel()
	// Exercise the actual asset-to-dialect path, including platform aliases.
	for assetType := range assetTypeDialectMap {
		t.Run(string(assetType), func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name: "result_table", Type: assetType,
				ExecutableFile: pipeline.ExecutableFile{Content: "SELECT * FROM RAW.Known; SELECT * FROM raw.missing; SELECT * FROM outside_pipeline; SELECT * FROM result_table"},
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "raw.known"},
					{Type: "uri", Value: "raw.missing"}, // a URI does not declare an asset dependency
				},
			}
			pl := &pipeline.Pipeline{Assets: []*pipeline.Asset{
				asset, {Name: "raw.known"}, {Name: "RAW.MISSING"},
			}}
			got, err := sharedSQLParser.GetMissingDependenciesForAsset(asset, pl, jinja.NewRendererWithYesterday("contract", "test"))
			require.NoError(t, err)
			// Matching is case-insensitive, but output retains query spelling.
			require.Equal(t, []string{"raw.missing"}, got)
		})
	}
}

func TestMissingDependenciesContractRenderingAndErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, query string
		assetType   pipeline.AssetType
		macros      []pipeline.Macro
		want        []string
		errorText   string
	}{
		{"macro renders table", "SELECT * FROM {{ source_table() }}", pipeline.AssetTypeDuckDBQuery, []pipeline.Macro{"{% macro source_table() %}raw.orders{% endmacro %}"}, []string{"raw.orders"}, ""},
		{"jinja comment is not SQL", "SELECT 1 {# FROM raw.orders #}", pipeline.AssetTypeDuckDBQuery, nil, []string{}, ""},
		{"cte itself is not a dependency", "WITH raw_orders AS (SELECT * FROM raw.orders) SELECT * FROM raw_orders", pipeline.AssetTypeDuckDBQuery, nil, []string{"raw.orders"}, ""},
		{"rendering error", "SELECT {{", pipeline.AssetTypeDuckDBQuery, nil, []string{}, "failed to render the query before parsing the SQL"},
		{"parse error", "SELECT * FROM", pipeline.AssetTypeDuckDBQuery, nil, []string{}, "failed to get used tables"},
		{"unsupported asset bypasses rendering", "SELECT {{", pipeline.AssetTypePython, nil, []string{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			asset := &pipeline.Asset{Name: "result_table", Type: tc.assetType, ExecutableFile: pipeline.ExecutableFile{Content: tc.query}}
			pl := &pipeline.Pipeline{Assets: []*pipeline.Asset{asset, {Name: "raw.orders"}, {Name: "raw_orders"}}, Macros: tc.macros}
			got, err := sharedSQLParser.GetMissingDependenciesForAsset(asset, pl, jinja.NewRendererWithYesterday("contract", "test"))
			if tc.errorText != "" {
				require.ErrorContains(t, err, tc.errorText)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}
