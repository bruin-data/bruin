package duck

import (
	"context"
	"database/sql"

	"github.com/bruin-data/bruin/pkg/query"
	_ "github.com/marcboeker/go-duckdb"
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
	LockDatabase(c.ToDBConnectionURI())
	defer UnlockDatabase(c.ToDBConnectionURI())
	conn, err := NewEphemeralConnection(c)
	if err != nil {
		return nil, err
	}

	return &Client{connection: conn, config: c}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	LockDatabase(c.config.ToDBConnectionURI())
	defer UnlockDatabase(c.config.ToDBConnectionURI())
	_, err := c.connection.ExecContext(ctx, query.String())
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

// Select runs a query and returns the results.
func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	LockDatabase(c.config.ToDBConnectionURI())
	defer UnlockDatabase(c.config.ToDBConnectionURI())

	rows, err := c.connection.QueryContext(ctx, query.String())
	if err != nil {
		return nil, err
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	defer rows.Close()

	result := make([][]interface{}, 0)

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		result = append(result, columns)
	}

	return result, nil
}
