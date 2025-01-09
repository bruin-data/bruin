package clickhouse

import (
	"context"
	"database/sql"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/bruin-data/bruin/pkg/query"
)

type Client struct {
	connection connection
	config     ClickHouseConfig
}

type ClickHouseConfig interface {
	ToClickHouseAuth() click_house.Auth
	GetIngestrURI() string
}

type connection interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error)
}

func NewClient(c ClickHouseConfig) (*Client, error) {
	// TODO
	return nil, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	// TODO
	return nil
}

// Select runs a query and returns the results.
func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	// TODO
	return nil, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
