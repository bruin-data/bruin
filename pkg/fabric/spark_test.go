package fabric

import (
	"context"
	"net/url"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type fabricSparkConnectionDetails struct {
	connectionType string
	details        any
}

func (f fabricSparkConnectionDetails) GetConnectionDetails(string) any {
	return f.details
}

func (f fabricSparkConnectionDetails) GetConnectionType(string) string {
	return f.connectionType
}

func TestFabricSparkConfigWithDefaultCredential(t *testing.T) {
	t.Parallel()

	got, err := fabricSparkConfig(&config.FabricConnection{
		UseAzureDefaultCredential: true,
		Lakehouse: &config.FabricLakehouseConfig{
			WorkspaceID: "workspace-id",
			LakehouseID: "lakehouse-id",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "spark://api.fabric.microsoft.com:443", got.URI)
	require.Equal(t, map[string]string{
		"spark.api":                   "livy",
		"spark.auth_type":             "azure_token",
		"spark.livy.azure.credential": "ActiveDirectoryDefault",
		"spark.livy.base_url":         "/v1/workspaces/workspace-id/lakehouses/lakehouse-id/livyapi/versions/2023-12-01",
		"spark.livy.session_kind":     "sql",
		"spark.tls":                   "true",
	}, got.Options)
}

func TestFabricLakehouseConfigParsesFromYAML(t *testing.T) {
	t.Parallel()

	var connection config.FabricConnection
	err := yaml.Unmarshal([]byte(`
name: fabric-default
use_azure_default_credential: true
lakehouse:
  workspace_id: workspace-id
  lakehouse_id: lakehouse-id
`), &connection)
	require.NoError(t, err)
	require.NotNil(t, connection.Lakehouse)
	require.Equal(t, "workspace-id", connection.Lakehouse.WorkspaceID)
	require.Equal(t, "lakehouse-id", connection.Lakehouse.LakehouseID)
}

func TestFabricSparkConfigWithServicePrincipal(t *testing.T) {
	t.Parallel()

	got, err := fabricSparkConfig(&config.FabricConnection{
		ClientID:     "client-id",
		ClientSecret: "secret:/?#[]@!$&'()*+,;=",
		TenantID:     "tenant-id",
		Lakehouse: &config.FabricLakehouseConfig{
			WorkspaceID: "workspace-id",
			LakehouseID: "lakehouse-id",
		},
	})
	require.NoError(t, err)
	parsed, err := url.Parse(got.URI)
	require.NoError(t, err)
	require.Equal(t, "client-id@tenant-id", parsed.User.Username())
	password, present := parsed.User.Password()
	require.True(t, present)
	require.Equal(t, "secret:/?#[]@!$&'()*+,;=", password)
	require.Equal(t, "ActiveDirectoryServicePrincipal", got.Options["spark.livy.azure.credential"])
	_, err = got.ToDSN()
	require.NoError(t, err)
}

// The Warehouse path (ToDBConnectionURI) lets use_azure_default_credential win
// over the service principal fields, so Spark has to agree — otherwise one
// connection authenticates as two different identities.
func TestFabricSparkConfigPrefersDefaultCredentialOverServicePrincipal(t *testing.T) {
	t.Parallel()

	got, err := fabricSparkConfig(&config.FabricConnection{
		UseAzureDefaultCredential: true,
		ClientID:                  "client-id",
		ClientSecret:              "client-secret",
		TenantID:                  "tenant-id",
		Lakehouse: &config.FabricLakehouseConfig{
			WorkspaceID: "workspace-id",
			LakehouseID: "lakehouse-id",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "ActiveDirectoryDefault", got.Options["spark.livy.azure.credential"])
	require.Equal(t, "spark://api.fabric.microsoft.com:443", got.URI)

	// The unused service principal secret must not reach the Livy DSN.
	parsed, err := url.Parse(got.URI)
	require.NoError(t, err)
	require.Nil(t, parsed.User)
	require.NotContains(t, got.URI, "client-secret")
}

func TestFabricSparkConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connection *config.FabricConnection
		want       string
	}{
		{
			name:       "missing Lakehouse",
			connection: &config.FabricConnection{UseAzureDefaultCredential: true},
			want:       "lakehouse configuration is required",
		},
		{
			name: "missing Lakehouse ID",
			connection: &config.FabricConnection{
				UseAzureDefaultCredential: true,
				Lakehouse:                 &config.FabricLakehouseConfig{WorkspaceID: "workspace-id"},
			},
			want: "both lakehouse.workspace_id and lakehouse.lakehouse_id are required",
		},
		{
			name: "missing service principal fields",
			connection: &config.FabricConnection{
				ClientID:  "client-id",
				Lakehouse: &config.FabricLakehouseConfig{WorkspaceID: "workspace-id", LakehouseID: "lakehouse-id"},
			},
			want: "service principal authentication requires client_secret, tenant_id",
		},
		{
			name: "SQL authentication",
			connection: &config.FabricConnection{
				Username:  "user",
				Password:  "password",
				Lakehouse: &config.FabricLakehouseConfig{WorkspaceID: "workspace-id", LakehouseID: "lakehouse-id"},
			},
			want: "Microsoft Entra authentication is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := fabricSparkConfig(tt.connection)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestSparkConnectionGetterResolvesAndCachesClient(t *testing.T) {
	t.Parallel()

	connection := &config.FabricConnection{
		UseAzureDefaultCredential: true,
		Lakehouse: &config.FabricLakehouseConfig{
			WorkspaceID: "workspace-id",
			LakehouseID: "lakehouse-id",
		},
	}
	getter := NewSparkConnectionGetter(fabricSparkConnectionDetails{
		connectionType: "fabric",
		details:        connection,
	})

	first, err := getter.ResolveSparkClient(context.Background(), "fabric-default")
	require.NoError(t, err)
	second, err := getter.ResolveSparkClient(context.Background(), "fabric-default")
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Same(t, first, getter.GetConnection("fabric-default"))
}

func TestSparkConnectionGetterRejectsNonFabricConnection(t *testing.T) {
	t.Parallel()

	getter := NewSparkConnectionGetter(fabricSparkConnectionDetails{connectionType: "spark"})
	_, err := getter.ResolveSparkClient(context.Background(), "spark-default")
	require.ErrorContains(t, err, "not a Fabric connection")
}
