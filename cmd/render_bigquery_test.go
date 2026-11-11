package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestBigQueryAssetForRender(t *testing.T) {
	t.Parallel()
	cm := &config.Config{SelectedEnvironment: &config.Environment{Connections: &config.Connections{
		GoogleCloudPlatform: []config.GoogleCloudPlatformConnection{
			{ConnectionMetadata: config.ConnectionMetadata{Name: "gcp"}, StorageURI: "gs://bucket/iceberg"},
		},
	}}}
	// Populate the connection cache before sharing it with parallel subtests.
	require.NotNil(t, cm.SelectedEnvironment.Connections.GetConnection("gcp"))
	pl := &pipeline.Pipeline{DefaultConnections: pipeline.EmptyStringMap{"google_cloud_platform": "gcp"}}
	icebergAsset := func(storageURI string) *pipeline.Asset {
		return &pipeline.Asset{
			Name:            "sales.orders",
			Type:            pipeline.AssetTypeBigqueryQuery,
			Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, TableFormat: pipeline.MaterializationTableFormatIceberg},
			BigQuery:        pipeline.BigQueryConfig{StorageURI: storageURI},
		}
	}
	for _, tc := range []struct {
		name   string
		asset  *pipeline.Asset
		config *config.Config
		want   string
	}{
		{name: "connection folder", asset: icebergAsset(""), config: cm, want: "gs://bucket/iceberg/sales/orders"},
		{name: "asset folder wins", asset: icebergAsset("gs://other/orders"), config: cm, want: "gs://other/orders"},
		{name: "offline", asset: icebergAsset("")},
		{name: "regular table", asset: &pipeline.Asset{Name: "sales.orders", Type: pipeline.AssetTypeBigqueryQuery}, config: cm},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := bigQueryAssetForRender(tc.config, pl, tc.asset)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.BigQuery.StorageURI)
		})
	}
}
