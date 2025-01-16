package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/query"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Querier interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
}

type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
}

type DB interface {
	Querier
	Selector
}

type Client struct {
	conn   *sqlx.DB
	config MySQLConfig
}

type MySQLConfig interface {
	GetIngestrURI() string
	ToDBConnectionURI() string
}

func NewClient(c MySQLConfig) (*Client, error) {
	conn, err := sqlx.Connect("mysql", c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

type connection interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := c.conn.ExecContext(ctx, query.String())
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}

	return nil
}

func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	rows, err := c.conn.QueryContext(ctx, query.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get columns")
	}

	collectedRows := make([][]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		collectedRows = append(collectedRows, values)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error during row iteration")
	}

	return collectedRows, nil
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	// Convert query object to string and execute it
	queryString := queryObj.String()
	rows, err := c.conn.QueryContext(ctx, queryString)
	if err != nil {
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
		return nil, err
	}
	defer rows.Close()

	// Initialize the result struct
	result := &query.QueryResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	}

	// Fetch column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column names")
	}
	result.Columns = cols

	// Fetch rows and scan into result set
	for rows.Next() {
		row := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range row {
			columnPointers[i] = &row[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result.Rows = append(result.Rows, row)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", rows.Err())
	}

	return result, nil

}
