package onelake

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestNewClient_SQLAnalyticsEndpointWithSASToken_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{WorkspaceName: "ws", LakehouseName: "lh", SASToken: "sv=1", SQLAnalyticsEndpoint: "x.datawarehouse.fabric.microsoft.com"})
	require.ErrorContains(t, err, "sql_analytics_endpoint")
}

func TestClient_Select_WithoutSQLAnalyticsEndpoint_ReturnsError(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{WorkspaceName: "ws", LakehouseName: "lh", SASToken: "sv=1"})
	require.NoError(t, err)

	_, err = client.Select(context.Background(), &query.Query{Query: "SELECT 1"})
	require.ErrorContains(t, err, "set sql_analytics_endpoint")
}

func TestNewClient_SQLAnalyticsEndpointWithServicePrincipal(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		WorkspaceName:        "ws",
		LakehouseName:        "lh",
		TenantID:             "tenant",
		ClientID:             "client",
		ClientSecret:         "secret",
		SQLAnalyticsEndpoint: "x.datawarehouse.fabric.microsoft.com",
	})
	require.NoError(t, err)
	require.NotNil(t, client.sql)
}

func TestSQLAnalyticsEndpointConfig_TrimsLakehouseAndEndpoint(t *testing.T) {
	t.Parallel()

	cfg := sqlAnalyticsEndpointConfig(Config{LakehouseName: "  lh  ", SQLAnalyticsEndpoint: " x.datawarehouse.fabric.microsoft.com "})
	require.Equal(t, "lh", cfg.Database)
	require.Equal(t, "x.datawarehouse.fabric.microsoft.com", cfg.Host)
}
