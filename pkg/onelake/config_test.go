package onelake

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI_MissingWorkspaceOrLakehouse_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
	}{
		{
			name:   "missing both",
			config: Config{ClientID: "client"},
		},
		{
			name:   "missing lakehouse",
			config: Config{WorkspaceName: "ws", ClientID: "client"},
		},
		{
			name:   "missing workspace",
			config: Config{LakehouseName: "lh", ClientID: "client"},
		},
		{
			name:   "whitespace only",
			config: Config{WorkspaceName: "  ", LakehouseName: "   ", ClientID: "client"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.config.GetIngestrURI()
			require.Error(t, err)
			require.Contains(t, err.Error(), "workspace_name and lakehouse_name")
		})
	}
}

func TestConfig_GetIngestrURI_NoAuth_ReturnsError(t *testing.T) {
	t.Parallel()
	config := Config{WorkspaceName: "ws", LakehouseName: "lh"}
	_, err := config.GetIngestrURI()
	require.Error(t, err)
	require.Contains(t, err.Error(), "authentication required")
}

func TestConfig_GetIngestrURI_ServicePrincipal_ReturnsURI(t *testing.T) {
	t.Parallel()
	config := Config{
		WorkspaceName: "myworkspace",
		LakehouseName: "mylakehouse",
		TenantID:      "tenant",
		ClientID:      "client",
		ClientSecret:  "secret",
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "onelake://myworkspace/mylakehouse?"), "want onelake://myworkspace/mylakehouse prefix, got %s", got)
	require.Contains(t, got, "tenant_id=tenant")
	require.Contains(t, got, "client_id=client")
	require.Contains(t, got, "client_secret=secret")
}

func TestConfig_GetIngestrURI_PartialServicePrincipal_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		wantMissing []string
	}{
		{
			name:        "only client_id",
			config:      Config{WorkspaceName: "ws", LakehouseName: "lh", ClientID: "client"},
			wantMissing: []string{"tenant_id", "client_secret"},
		},
		{
			name:        "client_id and client_secret without tenant",
			config:      Config{WorkspaceName: "ws", LakehouseName: "lh", ClientID: "client", ClientSecret: "secret"},
			wantMissing: []string{"tenant_id"},
		},
		{
			name:        "tenant and client_id without secret",
			config:      Config{WorkspaceName: "ws", LakehouseName: "lh", TenantID: "tenant", ClientID: "client"},
			wantMissing: []string{"client_secret"},
		},
		{
			name:        "only tenant_id",
			config:      Config{WorkspaceName: "ws", LakehouseName: "lh", TenantID: "tenant"},
			wantMissing: []string{"client_id", "client_secret"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.config.GetIngestrURI()
			require.Error(t, err)
			require.Contains(t, err.Error(), "service principal authentication requires")
			for _, field := range tt.wantMissing {
				require.Contains(t, err.Error(), field)
			}
		})
	}
}

func TestConfig_GetIngestrURI_MultipleAuthMethods_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
	}{
		{
			name: "service principal and sas token",
			config: Config{
				WorkspaceName: "ws", LakehouseName: "lh",
				TenantID: "tenant", ClientID: "client", ClientSecret: "secret",
				SASToken: "sv=token",
			},
		},
		{
			name: "service principal and default credential",
			config: Config{
				WorkspaceName: "ws", LakehouseName: "lh",
				TenantID: "tenant", ClientID: "client", ClientSecret: "secret",
				UseAzureDefaultCredential: true,
			},
		},
		{
			name: "sas token and default credential",
			config: Config{
				WorkspaceName: "ws", LakehouseName: "lh",
				SASToken:                  "sv=token",
				UseAzureDefaultCredential: true,
			},
		},
		{
			name: "partial service principal and sas token",
			config: Config{
				WorkspaceName: "ws", LakehouseName: "lh",
				ClientID: "client",
				SASToken: "sv=token",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.config.GetIngestrURI()
			require.Error(t, err)
			require.Contains(t, err.Error(), "multiple authentication methods configured")
		})
	}
}

func TestConfig_GetIngestrURI_SASToken_ReturnsURI(t *testing.T) {
	t.Parallel()
	config := Config{
		WorkspaceName: "ws",
		LakehouseName: "lh",
		SASToken:      "sv=token",
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "onelake://ws/lh?"), "got %s", got)
	require.Contains(t, got, "sas_token=")
	require.NotContains(t, got, "client_id=")
}

func TestConfig_GetIngestrURI_DefaultAzureCredential_ReturnsURIWithoutAuthParams(t *testing.T) {
	t.Parallel()
	config := Config{
		WorkspaceName:             "ws",
		LakehouseName:             "lh",
		UseAzureDefaultCredential: true,
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "onelake://ws/lh", got)
}

func TestConfig_GetIngestrURI_TrimsWorkspaceAndLakehouse(t *testing.T) {
	t.Parallel()
	config := Config{
		WorkspaceName: "  myworkspace  ",
		LakehouseName: "  mylakehouse  ",
		SASToken:      "token",
	}
	got, err := config.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "onelake://myworkspace/mylakehouse"), "got %s", got)
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()
	client, err := NewClient(Config{
		WorkspaceName: "ws",
		LakehouseName: "lh",
		SASToken:      "token",
	})
	require.NoError(t, err)
	got, err := client.GetIngestrURI()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "onelake://ws/lh"), "got %s", got)
}
