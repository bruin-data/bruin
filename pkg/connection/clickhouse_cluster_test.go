package connection

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestManagerClickHouseCluster(t *testing.T) {
	t.Parallel()
	m := Manager{availableConnections: map[string]any{}, AllConnectionDetails: map[string]any{}}
	for _, cluster := range []string{"analytics", ""} {
		connection := &config.ClickHouseConnection{
			ConnectionMetadata: config.ConnectionMetadata{Name: "warehouse"},
			Host:               "localhost",
			Cluster:            cluster,
		}
		require.NoError(t, m.AddClickHouseConnectionFromConfig(connection))
		client, ok := m.GetConnection("warehouse").(*clickhouse.Client)
		require.True(t, ok)
		require.Equal(t, cluster, client.GetCluster())
	}
}
