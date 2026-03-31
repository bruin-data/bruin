package trino

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	_ "github.com/trinodb/trino-go-client/trino"
)

type Client struct {
	connection connection
	config     Config
}

type connection interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func NewClient(c Config) (*Client, error) {
	// Use the official Trino driver
	dsn := c.ToDSN()

	conn, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open trino connection")
	}

	return &Client{
		connection: conn,
		config:     c,
	}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	queryStr := strings.TrimSpace(query.String())
	queryStr = strings.TrimSuffix(queryStr, ";")
	_, err := c.connection.ExecContext(ctx, queryStr)
	return errors.Wrap(err, "failed to execute query")
}

func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	queryStr := strings.TrimSpace(query.String())
	queryStr = strings.TrimSuffix(queryStr, ";")
	rows, err := c.connection.QueryContext(ctx, queryStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute select query")
	}
	defer rows.Close()

	// Get column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column names")
	}

	var result [][]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		result = append(result, columns)
	}

	if rows.Err() != nil {
		return nil, errors.Wrap(rows.Err(), "error during row iteration")
	}

	return result, nil
}

func (c *Client) Ping(ctx context.Context) error {
	// Simple ping query
	q := &query.Query{Query: "SELECT 1"}
	return c.RunQueryWithoutResult(ctx, q)
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	queryStr := strings.TrimSpace(queryObj.String())
	queryStr = strings.TrimSuffix(queryStr, ";")
	rows, err := c.connection.QueryContext(ctx, queryStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute select query")
	}
	defer rows.Close()

	// Initialize the result struct
	result := &query.QueryResult{
		Columns:     []string{},
		ColumnTypes: []string{},
		Rows:        [][]interface{}{},
	}

	// Fetch column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column names")
	}
	result.Columns = cols

	// Fetch column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column types")
	}
	typeStrings := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeStrings[i] = ct.DatabaseTypeName()
	}
	result.ColumnTypes = typeStrings

	// Fetch all rows
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

func toTrinoString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case nil:
		return "", false
	default:
		return "", false
	}
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	currentCatalogRows, err := c.Select(ctx, &query.Query{Query: "SELECT current_catalog"})
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Trino catalog: %w", err)
	}
	if len(currentCatalogRows) == 0 {
		return nil, errors.New("current_catalog returned no rows")
	}
	currentCatalog, ok := toTrinoString(currentCatalogRows[0][0])
	if !ok {
		return nil, errors.New("failed to parse current_catalog value")
	}

	tables, err := c.Select(ctx, &query.Query{Query: `
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema NOT LIKE 'information_schema'
AND table_schema NOT LIKE 'sys'
`})
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.tables: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    currentCatalog,
		Schemas: []*ansisql.DBSchema{},
	}
	schemaCache := make(map[string]*ansisql.DBSchema)

	for _, row := range tables {
		if len(row) < 3 {
			continue
		}

		schemaName, ok := toTrinoString(row[0])
		if !ok {
			continue
		}
		tableName, ok := toTrinoString(row[1])
		if !ok {
			continue
		}
		tableType, ok := toTrinoString(row[2])
		if !ok {
			continue
		}

		dbSchema, ok := schemaCache[schemaName]
		if !ok {
			dbSchema = &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemaCache[schemaName] = dbSchema
			summary.Schemas = append(summary.Schemas, dbSchema)
		}

		dbTableType := ansisql.DBTableTypeTable
		if strings.EqualFold(tableType, "VIEW") || strings.EqualFold(tableType, "MATERIALIZED VIEW") {
			dbTableType = ansisql.DBTableTypeView
		}
		dbSchema.Tables = append(dbSchema.Tables, &ansisql.DBTable{
			Name: tableName,
			Type: dbTableType,
		})
	}

	return summary, nil
}
