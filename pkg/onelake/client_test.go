package onelake

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestNewClient_SQLEndpointWithSASToken_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{WorkspaceName: "ws", LakehouseName: "lh", SASToken: "sv=1", SQLEndpoint: "x.datawarehouse.fabric.microsoft.com"})
	require.ErrorContains(t, err, "sql_endpoint")
}

func TestClient_Select_WithoutSQLEndpoint_ReturnsError(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{WorkspaceName: "ws", LakehouseName: "lh", SASToken: "sv=1"})
	require.NoError(t, err)

	_, err = client.Select(context.Background(), &query.Query{Query: "SELECT 1"})
	require.ErrorContains(t, err, "set sql_endpoint")
}

func TestNewClient_SQLEndpointWithServicePrincipal(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		WorkspaceName: "ws",
		LakehouseName: "lh",
		TenantID:      "tenant",
		ClientID:      "client",
		ClientSecret:  "secret",
		SQLEndpoint:   "x.datawarehouse.fabric.microsoft.com",
	})
	require.NoError(t, err)
	require.NotNil(t, client.sql)
}

func TestSQLEndpointConfig_TrimsLakehouseAndEndpoint(t *testing.T) {
	t.Parallel()

	cfg := sqlEndpointConfig(Config{LakehouseName: "  lh  ", SQLEndpoint: " x.datawarehouse.fabric.microsoft.com "})
	require.Equal(t, "lh", cfg.Database)
	require.Equal(t, "x.datawarehouse.fabric.microsoft.com", cfg.Host)
}
