package onelake

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

type staticCredential struct{}

func (staticCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "test-token"}, nil
}

func newFabricAPI(t *testing.T) *httptest.Server {
	t.Helper()

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/workspaces":
			if r.URL.Query().Get("continuationToken") == "" {
				fmt.Fprintf(w, `{"value":[{"id":"ws-1","displayName":"Other"}],"continuationUri":"%s/workspaces?continuationToken=2"}`, server.URL)
				return
			}
			fmt.Fprint(w, `{"value":[{"id":"ws-2","displayName":"Analytics"}]}`)
		case "/workspaces/ws-2/lakehouses":
			fmt.Fprint(w, `{"value":[
				{"id":"lh-1","displayName":"silver","properties":{"sqlEndpointProperties":{"connectionString":"abc.datawarehouse.fabric.microsoft.com","provisioningStatus":"Success"}}},
				{"id":"lh-2","displayName":"pending","properties":{"sqlEndpointProperties":{"provisioningStatus":"InProgress"}}}
			]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func TestClient_FindSQLAnalyticsEndpoint(t *testing.T) {
	t.Parallel()

	server := newFabricAPI(t)

	tests := []struct {
		name         string
		workspace    string
		lakehouse    string
		wantHost     string
		wantDatabase string
		wantErr      string
	}{
		{name: "by names across pages", workspace: "Analytics", lakehouse: "silver", wantHost: "abc.datawarehouse.fabric.microsoft.com", wantDatabase: "silver"},
		{name: "by ids with suffix", workspace: "ws-2", lakehouse: "lh-1.Lakehouse", wantHost: "abc.datawarehouse.fabric.microsoft.com", wantDatabase: "silver"},
		{name: "unknown workspace", workspace: "missing", lakehouse: "silver", wantErr: `workspace "missing" not found`},
		{name: "unknown lakehouse", workspace: "Analytics", lakehouse: "gold", wantErr: `lakehouse "gold" not found`},
		{name: "endpoint not provisioned", workspace: "Analytics", lakehouse: "pending", wantErr: "InProgress"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &Client{
				config:     Config{WorkspaceName: tt.workspace, LakehouseName: tt.lakehouse},
				apiURL:     server.URL,
				credential: staticCredential{},
			}
			host, database, err := client.findSQLAnalyticsEndpoint(context.Background())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantHost, host)
			require.Equal(t, tt.wantDatabase, database)
		})
	}
}

func TestClient_Select_WithSASToken_ReturnsError(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{WorkspaceName: "ws", LakehouseName: "lh", SASToken: "sv=1"})
	require.NoError(t, err)

	_, err = client.Select(context.Background(), &query.Query{Query: "SELECT 1"})
	require.ErrorContains(t, err, "a SAS token cannot query")
}

func TestClient_Select_WaitingForLookupRespectsContext(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{WorkspaceName: "ws", LakehouseName: "lh", ClientID: "c", TenantID: "t", ClientSecret: "s"})
	require.NoError(t, err)
	client.sqlLock <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.Select(ctx, &query.Query{Query: "SELECT 1"})
	require.ErrorIs(t, err, context.Canceled)
}
