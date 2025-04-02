package postgres

import (
	"context"
	"sort"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type Client struct {
	connection    connection
	config        PgConfig
	schemaCreator *ansisql.SchemaCreator
}

type PgConfig interface {
	ToDBConnectionURI() string
	GetIngestrURI() string
	GetDatabase() string
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

	return &Client{connection: conn, config: c, schemaCreator: ansisql.NewSchemaCreator()}, nil
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

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	db := c.config.GetDatabase()
	q := `
SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
	table_catalog = $1 AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
`

	rows, err := c.connection.Query(ctx, q, db)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]any, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	summary := &ansisql.DBDatabase{
		Name:    db,
		Schemas: []*ansisql.DBSchema{},
	}
	schemas := make(map[string]*ansisql.DBSchema)

	for _, row := range collectedRows {
		if len(row) != 2 {
			continue
		}

		schemaName := row[0].(string)
		tableName := row[1].(string)

		// Create schema if it doesn't exist
		schemaKey := db + "." + schemaName
		if _, exists := schemas[schemaKey]; !exists {
			schema := &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemas[schemaKey] = schema
		}

		// Add table to schema
		table := &ansisql.DBTable{
			Name: tableName,
		}
		schemas[schemaKey].Tables = append(schemas[schemaKey].Tables, table)
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

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	return c.schemaCreator.CreateSchemaIfNotExist(ctx, c, asset)
}
