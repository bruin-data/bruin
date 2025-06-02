//go:build bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"errors"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

var errDuckDBNotSupported = errors.New("DuckDB support not available in this build")

type Client struct {
	connection    connection
	config        DuckDBConfig
	schemaCreator *ansisql.SchemaCreator
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
	return nil, errDuckDBNotSupported
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	return errDuckDBNotSupported
}

func (c *Client) GetIngestrURI() (string, error) {
	return "", errDuckDBNotSupported
}

func (c *Client) GetDBConnectionURI() (string, error) {
	return "", errDuckDBNotSupported
}

// Select runs a query and returns the results.
func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	return nil, errDuckDBNotSupported
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObject *query.Query) (*query.QueryResult, error) {
	return nil, errDuckDBNotSupported
}

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	return errDuckDBNotSupported
}
