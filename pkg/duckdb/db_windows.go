package duck

import (
	"context"
	"database/sql"

	"github.com/bruin-data/bruin/pkg/query"
)

type Client struct {
	connection connection
	config     DuckDBConfig
}

type DuckDBConfig interface {
	ToDBConnectionURI() string
	GetIngestrURI() string
}

type connection interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error)
}

func NewClient(c DuckDBConfig) (*Client, error) {
	return nil, errors.New("duckDB not supported")
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	return errors.New("duckDB not supported")
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

// Select runs a query and returns the results.
func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	return nil, errors.New("duckDB not supported")
}
