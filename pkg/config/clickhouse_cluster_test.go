package config

import (
	"encoding/json"
	"testing"

	"github.com/go-viper/mapstructure/v2"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestClickHouseConnectionClusterRoundTrip(t *testing.T) {
	t.Parallel()
	var conn ClickHouseConnection
	require.NoError(t, yaml.Unmarshal([]byte("name: warehouse\ncluster: analytics\n"), &conn))
	require.Equal(t, "analytics", conn.Cluster)
	data, err := yaml.Marshal(conn)
	require.NoError(t, err)
	require.Contains(t, string(data), "cluster: analytics")
	data, err = json.Marshal(conn)
	require.NoError(t, err)
	require.Contains(t, string(data), `"cluster":"analytics"`)
	require.NoError(t, json.Unmarshal([]byte(`{"cluster":"replicas"}`), &conn))
	require.Equal(t, "replicas", conn.Cluster)
	require.NoError(t, mapstructure.Decode(map[string]any{"cluster": "local"}, &conn))
	require.Equal(t, "local", conn.Cluster)
	conn.Cluster = ""
	data, err = json.Marshal(conn)
	require.NoError(t, err)
	require.NotContains(t, string(data), `"cluster"`)
	data, err = yaml.Marshal(conn)
	require.NoError(t, err)
	require.NotContains(t, string(data), "cluster:")
}

func TestClickHouseConnectionClusterSchema(t *testing.T) {
	t.Parallel()
	schema, err := GetConnectionsSchema()
	require.NoError(t, err)
	var document struct {
		Definitions map[string]struct {
			Properties map[string]struct {
				Type string `json:"type"`
			} `json:"properties"`
			Required []string `json:"required"`
		} `json:"$defs"`
	}
	require.NoError(t, json.Unmarshal([]byte(schema), &document))
	definition := document.Definitions["ClickHouseConnection"]
	require.Equal(t, "string", definition.Properties["cluster"].Type)
	require.NotContains(t, definition.Required, "cluster")
}
