package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestClickHouseClusterForRender(t *testing.T) {
	t.Parallel()
	cm := &config.Config{SelectedEnvironment: &config.Environment{Connections: &config.Connections{
		ClickHouse: []config.ClickHouseConnection{
			{ConnectionMetadata: config.ConnectionMetadata{Name: "default"}, Cluster: "analytics"},
			{ConnectionMetadata: config.ConnectionMetadata{Name: "override"}, Cluster: "replicas"},
			{ConnectionMetadata: config.ConnectionMetadata{Name: "local"}},
		},
	}}}
	// Populate the connection cache before sharing it with parallel subtests.
	require.NotNil(t, cm.SelectedEnvironment.Connections.GetConnection("default"))
	pl := &pipeline.Pipeline{DefaultConnections: pipeline.EmptyStringMap{"clickhouse": "default"}}
	for _, tc := range []struct {
		name       string
		connection string
		assetType  pipeline.AssetType
		config     *config.Config
		want       string
	}{
		{name: "pipeline default", assetType: pipeline.AssetTypeClickHouse, config: cm, want: "analytics"},
		{name: "asset connection", connection: "override", assetType: pipeline.AssetTypeClickHouse, config: cm, want: "replicas"},
		{name: "local connection", connection: "local", assetType: pipeline.AssetTypeClickHouse, config: cm},
		{name: "sensor", assetType: pipeline.AssetTypeClickHouseQuerySensor, config: cm, want: "analytics"},
		{name: "offline", assetType: pipeline.AssetTypeClickHouse},
		{name: "other platform", assetType: pipeline.AssetTypePostgresQuery, config: cm},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cluster, err := clickHouseClusterForRender(tc.config, pl, &pipeline.Asset{Type: tc.assetType, Connection: tc.connection})
			require.NoError(t, err)
			require.Equal(t, tc.want, cluster)
		})
	}
}

func TestRenderCommandClickHouseClusterHooks(t *testing.T) {
	t.Parallel()
	extractor := new(mockExtractor)
	extractor.On("ExtractQueriesFromString", "SELECT 1 AS id").Return([]*query.Query{{Query: "SELECT 1 AS id"}}, nil)
	writer := new(bytes.Buffer)
	asset := &pipeline.Asset{
		Name:            "warehouse.events",
		Type:            pipeline.AssetTypeClickHouse,
		ExecutableFile:  pipeline.ExecutableFile{Content: "SELECT 1 AS id"},
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
		Hooks: pipeline.Hooks{
			Pre:  []pipeline.Hook{{Query: "SELECT 'pre'"}},
			Post: []pipeline.Hook{{Query: "SELECT 'post'"}},
		},
	}
	render := &RenderCommand{
		extractor: extractor,
		materializers: map[pipeline.AssetType]queryMaterializer{
			pipeline.AssetTypeClickHouse: clickhouse.NewRenderer(false, "analytics"),
		},
		writer: writer,
		output: "json",
	}
	require.NoError(t, render.Run(&pipeline.Pipeline{}, asset, ModifierInfo{}))
	var payload map[string]string
	require.NoError(t, json.Unmarshal(writer.Bytes(), &payload))
	rendered := payload["query"]
	require.Contains(t, rendered, "ON CLUSTER")
	require.Contains(t, rendered, "analytics")
	require.Equal(t, 1, strings.Count(rendered, "SELECT 'pre'"))
	require.Equal(t, 1, strings.Count(rendered, "SELECT 'post'"))
	require.Less(t, strings.Index(rendered, "SELECT 'pre'"), strings.Index(rendered, "CREATE OR REPLACE VIEW"))
	require.Less(t, strings.Index(rendered, "CREATE OR REPLACE VIEW"), strings.Index(rendered, "SELECT 'post'"))
	extractor.AssertExpectations(t)
}
