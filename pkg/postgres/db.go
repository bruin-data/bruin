package postgres

import (
	"context"

	"github.com/bruin-data/bruin/pkg/ansisql"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type Client struct {
	connection connection
	config     PgConfig
}

type PgConfig interface {
	ToDBConnectionURI() string
	GetIngestrURI() string
}

type connection interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func NewClient(ctx context.Context, c PgConfig) (*Client, error) {
	conn, err := pgxpool.New(ctx, c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &Client{connection: conn, config: c}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := c.connection.Exec(ctx, query.String())
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

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]interface{}, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	if len(collectedRows) == 0 {
		return make([][]interface{}, 0), nil
	}

	return collectedRows, nil
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	rows, err := c.connection.Query(ctx, queryObj.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()
	// Retrieve column metadata using FieldDescriptions
	fieldDescriptions := rows.FieldDescriptions()
	if fieldDescriptions == nil {
		return nil, errors.New("field descriptions are not available")
	}

	// Extract column names
	columns := make([]string, len(fieldDescriptions))
	for i, field := range fieldDescriptions {
		columns[i] = field.Name
	}

	// Collect rows
	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]interface{}, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}
	result := &query.QueryResult{
		Columns: columns,
		Rows:    collectedRows,
	}
	return result, nil
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

func (c *Client) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	rows, err := c.connection.Query(ctx, query.ToExplainQuery())
	if err == nil {
		err = rows.Err()
	}

	if rows != nil {
		defer rows.Close()
	}

	return err == nil, err
}

func (c *Client) GetDatabaseSummary(ctx context.Context) ([]ansisql.DBDatabase, error) {
	q := &query.Query{
		Query: `
SELECT
    table_catalog,
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
    table_schema NOT IN ('pg_catalog', 'information_schema') AND table_type = 'BASE TABLE'
ORDER BY table_catalog, table_schema, table_name;
`}

	// Execute the query
	result, err := c.SelectWithSchema(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get database summary")
	}

	// Process the results into the DBDatabase structure
	databases := make(map[string]*ansisql.DBDatabase)
	schemas := make(map[string]*ansisql.DBSchema)

	for _, row := range result.Rows {
		if len(row) != 3 {
			continue
		}

		dbName := row[0].(string)
		schemaName := row[1].(string)
		tableName := row[2].(string)

		// Create database if it doesn't exist
		if _, exists := databases[dbName]; !exists {
			databases[dbName] = &ansisql.DBDatabase{
				Name:    dbName,
				Schemas: []*ansisql.DBSchema{},
			}
		}

		// Create schema if it doesn't exist
		schemaKey := dbName + "." + schemaName
		if _, exists := schemas[schemaKey]; !exists {
			schema := &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemas[schemaKey] = schema
			databases[dbName].Schemas = append(databases[dbName].Schemas, schema)
		}

		// Add table to schema
		table := &ansisql.DBTable{
			Name:    tableName,
		}
		schemas[schemaKey].Tables = append(schemas[schemaKey].Tables, table)
	}

	// Convert map to slice
	result2 := make([]ansisql.DBDatabase, 0, len(databases))
	for _, db := range databases {
		result2 = append(result2, *db)
	}

	return result2, nil
}
