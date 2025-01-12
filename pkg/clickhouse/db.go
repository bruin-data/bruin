package clickhouse

import (
	"context"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

type Client struct {
	connection connection
	config     ClickHouseConfig
}

type ClickHouseConfig interface {
	ToClickHouseOptions() *click_house.Options
	GetIngestrURI() string
}

type connection interface {
	Query(ctx context.Context, sql string, args ...any) (driver.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) error
}

func NewClient(c ClickHouseConfig) (*Client, error) {
	conn, err := click_house.Open(c.ToClickHouseOptions())
	if err != nil {
		return nil, err
	}

	return &Client{connection: conn, config: c}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	err := c.connection.Exec(ctx, query.String())
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
	rows, err := c.connection.Query(ctx, query.String())
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	collectedRows := make([][]interface{}, 0)
	for rows.Next() {
		result := make([]interface{}, len(rows.Columns()))
		if err := rows.Scan(result...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		collectedRows = append(collectedRows, result)
	}

	return collectedRows, nil
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	rows, err := c.connection.Query(ctx, queryObj.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()

	fieldDescriptions := rows.ColumnTypes()
	if fieldDescriptions == nil {
		return nil, errors.New("field descriptions are not available")
	}

	// Extract column names
	columns := make([]string, len(fieldDescriptions))
	for i, field := range fieldDescriptions {
		columns[i] = field.Name()
	}

	collectedRows := make([][]interface{}, 0)
	for rows.Next() {
		result := make([]interface{}, len(rows.Columns()))
		if err := rows.Scan(result...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		collectedRows = append(collectedRows, result)
	}

	return &query.QueryResult{Columns: columns, Rows: collectedRows}, nil
}

// Test runs a simple query (SELECT 1) to validate the connection.
func (c *Client) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}
	err := c.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Postgres connection")
	}

	return nil
}
