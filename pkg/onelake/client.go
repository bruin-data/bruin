package onelake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/bruin-data/bruin/pkg/fabric"
	"github.com/bruin-data/bruin/pkg/query"
)

const (
	fabricAPIURL   = "https://api.fabric.microsoft.com/v1"
	fabricAPIScope = "https://api.fabric.microsoft.com/.default"
)

type Client struct {
	config     Config
	apiURL     string
	credential azcore.TokenCredential

	sqlLock chan struct{}
	sql     *fabric.DB
}

func NewClient(c Config) (*Client, error) {
	return &Client{config: c, apiURL: fabricAPIURL, sqlLock: make(chan struct{}, 1)}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI()
}

// Select runs quality check queries against the lakehouse's SQL analytics endpoint.
func (c *Client) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	db, err := c.sqlDB(ctx)
	if err != nil {
		return nil, err
	}

	return db.Select(ctx, q)
}

func (c *Client) sqlDB(ctx context.Context) (*fabric.DB, error) {
	select {
	case c.sqlLock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.sqlLock }()

	if c.sql != nil {
		return c.sql, nil
	}

	host, database, err := c.findSQLAnalyticsEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	db, err := fabric.NewDB(&fabric.Config{
		Host:                      host,
		Database:                  database,
		TenantID:                  c.config.TenantID,
		ClientID:                  c.config.ClientID,
		ClientSecret:              c.config.ClientSecret,
		UseAzureDefaultCredential: c.config.UseAzureDefaultCredential,
	})
	if err != nil {
		return nil, err
	}
	c.sql = db

	return db, nil
}

type fabricItem struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Properties  struct {
		SQLEndpointProperties struct {
			ConnectionString   string `json:"connectionString"`
			ProvisioningStatus string `json:"provisioningStatus"`
		} `json:"sqlEndpointProperties"`
	} `json:"properties"`
}

// findSQLAnalyticsEndpoint looks up the lakehouse's SQL analytics endpoint host through the Fabric REST API.
func (c *Client) findSQLAnalyticsEndpoint(ctx context.Context) (string, string, error) {
	credential, err := c.tokenCredential()
	if err != nil {
		return "", "", err
	}
	token, err := credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{fabricAPIScope}})
	if err != nil {
		return "", "", fmt.Errorf("onelake: failed to get a Fabric API token: %w", err)
	}

	workspaceName := strings.TrimSpace(c.config.WorkspaceName)
	workspaces, err := c.listItems(ctx, token.Token, c.apiURL+"/workspaces")
	if err != nil {
		return "", "", err
	}
	workspace, err := findItem(workspaces, "workspace", workspaceName)
	if err != nil {
		return "", "", err
	}

	lakehouseName := strings.TrimSuffix(strings.TrimSpace(c.config.LakehouseName), ".Lakehouse")
	lakehouses, err := c.listItems(ctx, token.Token, c.apiURL+"/workspaces/"+url.PathEscape(workspace.ID)+"/lakehouses")
	if err != nil {
		return "", "", err
	}
	lakehouse, err := findItem(lakehouses, "lakehouse", lakehouseName)
	if err != nil {
		return "", "", err
	}

	endpoint := lakehouse.Properties.SQLEndpointProperties
	if endpoint.ConnectionString == "" {
		return "", "", fmt.Errorf("onelake: SQL analytics endpoint of lakehouse %q is not available (provisioning status: %q)", lakehouseName, endpoint.ProvisioningStatus)
	}

	return endpoint.ConnectionString, lakehouse.DisplayName, nil
}

//nolint:ireturn
func (c *Client) tokenCredential() (azcore.TokenCredential, error) {
	if c.credential != nil {
		return c.credential, nil
	}

	switch {
	case c.config.UseAzureDefaultCredential:
		return azidentity.NewDefaultAzureCredential(nil)
	case c.config.ClientID != "":
		return azidentity.NewClientSecretCredential(c.config.TenantID, c.config.ClientID, c.config.ClientSecret, nil)
	default:
		return nil, errors.New("onelake: quality checks need service principal or use_azure_default_credential authentication, a SAS token cannot query the lakehouse SQL analytics endpoint")
	}
}

func (c *Client) listItems(ctx context.Context, token, nextURL string) ([]fabricItem, error) {
	var items []fabricItem
	for nextURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("onelake: Fabric API request failed: %w", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("onelake: Fabric API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var page struct {
			Value           []fabricItem `json:"value"`
			ContinuationURI string       `json:"continuationUri"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("onelake: failed to parse Fabric API response: %w", err)
		}
		items = append(items, page.Value...)
		nextURL = page.ContinuationURI
	}

	return items, nil
}

// findItem prefers an exact name or ID match and only falls back to a case-insensitive one when it is unique.
func findItem(items []fabricItem, kind, nameOrID string) (fabricItem, error) {
	var caseInsensitive []fabricItem
	for _, item := range items {
		if item.ID == nameOrID || item.DisplayName == nameOrID {
			return item, nil
		}
		if strings.EqualFold(item.ID, nameOrID) || strings.EqualFold(item.DisplayName, nameOrID) {
			caseInsensitive = append(caseInsensitive, item)
		}
	}

	switch len(caseInsensitive) {
	case 0:
		return fabricItem{}, fmt.Errorf("onelake: %s %q not found or not accessible", kind, nameOrID)
	case 1:
		return caseInsensitive[0], nil
	default:
		return fabricItem{}, fmt.Errorf("onelake: %s name %q is ambiguous, use the %s ID instead", kind, nameOrID, kind)
	}
}
