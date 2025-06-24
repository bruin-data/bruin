package mysql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
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

// type connection interface {
//	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
//	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
//}

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

		// Convert []byte to string
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		collectedRows = append(collectedRows, values)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error during row iteration")
	}

	return collectedRows, nil
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	queryString := queryObj.String()
	rows, err := c.conn.QueryContext(ctx, queryString)
	if err != nil {
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
		return nil, err
	}
	defer rows.Close()

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

		// convert []byte -> string
		for i, v := range row {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			}
		}

		result.Rows = append(result.Rows, row)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", rows.Err())
	}

	return result, nil
}

func (c *Client) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}

	err := c.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on MySQL connection")
	}

	return nil
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// MySQL uses schemas as databases, so we'll get all databases and their tables
	q := `
SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
    table_type IN ('BASE TABLE', 'VIEW')
    AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
ORDER BY table_schema, table_name;
`

	result, err := c.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL information_schema: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    "mysql", // MySQL instance
		Schemas: []*ansisql.DBSchema{},
	}
	schemas := make(map[string]*ansisql.DBSchema)

	for _, row := range result {
		if len(row) != 2 {
			continue
		}

		schemaName, ok := row[0].(string)
		if !ok {
			continue
		}
		tableName, ok := row[1].(string)
		if !ok {
			continue
		}

		// Create schema if it doesn't exist
		if _, exists := schemas[schemaName]; !exists {
			schema := &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemas[schemaName] = schema
		}

		// Add table to schema
		table := &ansisql.DBTable{
			Name: tableName,
		}
		schemas[schemaName].Tables = append(schemas[schemaName].Tables, table)
	}

	for _, schema := range schemas {
		summary.Schemas = append(summary.Schemas, schema)
	}

	// Sort schemas by name
	sort.Slice(summary.Schemas, func(i, j int) bool {
		return summary.Schemas[i].Name < summary.Schemas[j].Name
	})

	return summary, nil
}
