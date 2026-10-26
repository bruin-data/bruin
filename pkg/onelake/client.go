package onelake

import (
	"context"
	"errors"
	"strings"

	"github.com/bruin-data/bruin/pkg/fabric"
	"github.com/bruin-data/bruin/pkg/query"
)

type Client struct {
	config Config
	sql    *fabric.DB
}

func NewClient(c Config) (*Client, error) {
	client := &Client{config: c}
	endpoint := strings.TrimSpace(c.SQLEndpoint)
	if endpoint == "" {
		return client, nil
	}
	if c.SASToken != "" {
		return nil, errors.New("onelake: sql_endpoint requires service principal or use_azure_default_credential authentication, a SAS token cannot query the SQL endpoint")
	}

	db, err := fabric.NewDB(sqlEndpointConfig(c))
	if err != nil {
		return nil, err
	}
	client.sql = db

	return client, nil
}

func sqlEndpointConfig(c Config) *fabric.Config {
	return &fabric.Config{
		Host:                      strings.TrimSpace(c.SQLEndpoint),
		Database:                  strings.TrimSpace(c.LakehouseName),
		TenantID:                  c.TenantID,
		ClientID:                  c.ClientID,
		ClientSecret:              c.ClientSecret,
		UseAzureDefaultCredential: c.UseAzureDefaultCredential,
	}
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI()
}

// Select runs quality check queries against the lakehouse's SQL analytics endpoint.
func (c *Client) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	if c.sql == nil {
		return nil, errors.New("onelake: set sql_endpoint on the connection to run quality checks")
	}

	return c.sql.Select(ctx, q)
}
