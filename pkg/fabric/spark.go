package fabric

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/spark"
	"github.com/pkg/errors"
)

const (
	fabricSparkHost = "api.fabric.microsoft.com:443"
	fabricLivyAPI   = "2023-12-01"
)

// SparkConnectionGetter resolves a Fabric connection as a Spark client while
// leaving the connection manager's regular Fabric/TDS client untouched.
type SparkConnectionGetter struct {
	connections config.ConnectionDetailsGetter

	mu      sync.Mutex
	clients map[string]*spark.Client
}

func NewSparkConnectionGetter(connections config.ConnectionDetailsGetter) *SparkConnectionGetter {
	return &SparkConnectionGetter{
		connections: connections,
		clients:     make(map[string]*spark.Client),
	}
}

// GetConnection implements config.ConnectionGetter. Callers that support
// spark.ClientResolver use ResolveSparkClient directly and retain configuration
// errors; generic check runners receive nil when the Fabric Spark configuration
// is invalid.
func (g *SparkConnectionGetter) GetConnection(name string) any {
	client, err := g.ResolveSparkClient(context.Background(), name)
	if err != nil {
		return nil
	}
	return client
}

// ResolveSparkClient builds and caches the Spark client derived from a Fabric
// connection's shared Entra credentials and Lakehouse coordinates.
func (g *SparkConnectionGetter) ResolveSparkClient(ctx context.Context, name string) (*spark.Client, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if client, ok := g.clients[name]; ok {
		return client, nil
	}
	if connectionType := g.connections.GetConnectionType(name); connectionType != "fabric" {
		if connectionType == "" {
			return nil, errors.Errorf("connection %q does not exist", name)
		}
		return nil, errors.Errorf("connection %q is a %s connection, not a Fabric connection", name, connectionType)
	}

	details, ok := g.connections.GetConnectionDetails(name).(*config.FabricConnection)
	if !ok || details == nil {
		return nil, errors.Errorf("connection %q does not contain Fabric connection details", name)
	}
	sparkConfig, err := fabricSparkConfig(details)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid Fabric Spark configuration for connection %q", name)
	}
	client, err := spark.NewClient(ctx, sparkConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Fabric Spark client for connection %q", name)
	}
	g.clients[name] = client
	return client, nil
}

func fabricSparkConfig(connection *config.FabricConnection) (spark.Config, error) {
	if connection.Lakehouse == nil {
		return spark.Config{}, errors.New("lakehouse configuration is required; set lakehouse.workspace_id and lakehouse.lakehouse_id")
	}
	workspaceID := strings.TrimSpace(connection.Lakehouse.WorkspaceID)
	lakehouseID := strings.TrimSpace(connection.Lakehouse.LakehouseID)
	if workspaceID == "" || lakehouseID == "" {
		return spark.Config{}, errors.New("both lakehouse.workspace_id and lakehouse.lakehouse_id are required")
	}

	endpoint := &url.URL{Scheme: "spark", Host: fabricSparkHost}
	var credential string
	switch {
	// use_azure_default_credential takes precedence over the service principal
	// fields, matching ToDBConnectionURI, so that one Fabric connection
	// authenticates as the same identity on both execution engines.
	case connection.UseAzureDefaultCredential:
		credential = "ActiveDirectoryDefault"
	case connection.ClientID != "" || connection.ClientSecret != "" || connection.TenantID != "":
		missing := make([]string, 0, 3)
		if connection.ClientID == "" {
			missing = append(missing, "client_id")
		}
		if connection.ClientSecret == "" {
			missing = append(missing, "client_secret")
		}
		if connection.TenantID == "" {
			missing = append(missing, "tenant_id")
		}
		if len(missing) > 0 {
			return spark.Config{}, fmt.Errorf("service principal authentication requires %s", strings.Join(missing, ", "))
		}
		endpoint.User = url.UserPassword(connection.ClientID+"@"+connection.TenantID, connection.ClientSecret)
		credential = "ActiveDirectoryServicePrincipal"
	default:
		return spark.Config{}, errors.New("Microsoft Entra authentication is required; set client_id/client_secret/tenant_id or use_azure_default_credential")
	}

	baseURL := fmt.Sprintf(
		"/v1/workspaces/%s/lakehouses/%s/livyapi/versions/%s",
		url.PathEscape(workspaceID),
		url.PathEscape(lakehouseID),
		fabricLivyAPI,
	)
	// net/url leaves semicolons unescaped in userinfo, while the ADBC
	// database/sql bridge uses them as DSN option delimiters.
	uri := strings.ReplaceAll(endpoint.String(), ";", "%3B")
	return spark.Config{
		URI: uri,
		Options: map[string]string{
			"spark.api":                   "livy",
			"spark.auth_type":             "azure_token",
			"spark.livy.azure.credential": credential,
			"spark.livy.base_url":         baseURL,
			"spark.livy.session_kind":     "sql",
			"spark.tls":                   "true",
		},
	}, nil
}
