package inference

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

type connectionWithType struct{ platform string }

func (c connectionWithType) GetConnection(string) any        { return nil }
func (c connectionWithType) GetConnectionDetails(string) any { return nil }
func (c connectionWithType) GetConnectionType(string) string { return c.platform }

func TestResolveInputQuery(t *testing.T) {
	t.Parallel()
	t.Run("query unchanged", func(t *testing.T) {
		t.Parallel()
		asset := testAsset()
		query, err := resolveInputQuery(&pipeline.Pipeline{}, asset, connectionWithType{})
		require.NoError(t, err)
		require.Equal(t, asset.Parameters["input_query"], query)
	})

	for _, tc := range []struct{ platform, expected string }{
		{"google_cloud_platform", "SELECT * FROM `project`.`dataset`.`tickets`"},
		{"postgres", `SELECT * FROM "analytics"."tickets"`},
		{"mssql", "SELECT * FROM [dbo].[tickets]"},
	} {
		t.Run(tc.platform, func(t *testing.T) {
			t.Parallel()
			asset := testAsset()
			delete(asset.Parameters, "input_query")
			sourceName := map[string]string{"google_cloud_platform": "project.dataset.tickets", "postgres": "analytics.tickets", "mssql": "dbo.tickets"}[tc.platform]
			asset.Parameters["input_asset"] = sourceName
			source := &pipeline.Asset{Name: sourceName, Connection: asset.Connection}
			pipe := &pipeline.Pipeline{Assets: []*pipeline.Asset{source, asset}}
			query, err := resolveInputQuery(pipe, asset, connectionWithType{platform: tc.platform})
			require.NoError(t, err)
			require.Equal(t, tc.expected, query)
		})
	}
}

func TestInferenceInputConfigExclusive(t *testing.T) {
	t.Parallel()
	asset := testAsset()
	asset.Parameters["input_asset"] = "source"
	require.ErrorContains(t, ValidateAsset(asset), "exactly one")
	delete(asset.Parameters, "input_query")
	asset.Parameters["input_asset"] = 12
	require.ErrorContains(t, ValidateAsset(asset), "nonempty string")
}

func TestResolveInputAssetRejectsInvalidReferences(t *testing.T) {
	t.Parallel()
	asset := testAsset()
	delete(asset.Parameters, "input_query")
	pipe := &pipeline.Pipeline{Assets: []*pipeline.Asset{asset}}
	asset.Parameters["input_asset"] = asset.Name
	_, err := resolveInputQuery(pipe, asset, connectionWithType{platform: "postgres"})
	require.ErrorContains(t, err, "itself")

	asset.Parameters["input_asset"] = "missing.table"
	_, err = resolveInputQuery(pipe, asset, connectionWithType{platform: "postgres"})
	require.ErrorContains(t, err, "does not exist")

	source := &pipeline.Asset{Name: "source.table", Connection: "other"}
	pipe = &pipeline.Pipeline{Assets: []*pipeline.Asset{asset, source}}
	asset.Parameters["input_asset"] = source.Name
	_, err = resolveInputQuery(pipe, asset, connectionWithType{platform: "postgres"})
	require.ErrorContains(t, err, "cross-connection")
}
