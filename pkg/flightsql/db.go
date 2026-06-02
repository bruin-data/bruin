package flightsql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	// Registers the "flightsql" database/sql driver provided by Apache ADBC.
	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
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
	dsn, err := c.ToDSN()
	if err != nil {
		return nil, err
	}

	conn, err := sql.Open("flightsql", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Flight SQL connection")
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

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	queryStr := strings.TrimSpace(queryObj.String())
	queryStr = strings.TrimSuffix(queryStr, ";")
	rows, err := c.connection.QueryContext(ctx, queryStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute select query")
	}
	defer rows.Close()

	result := &query.QueryResult{
		Columns:     []string{},
		ColumnTypes: []string{},
		Rows:        [][]interface{}{},
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column names")
	}
	result.Columns = cols

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column types")
	}
	typeStrings := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeStrings[i] = ct.DatabaseTypeName()
	}
	result.ColumnTypes = typeStrings

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

func (c *Client) Ping(ctx context.Context) error {
	q := &query.Query{Query: "SELECT 1"}
	return c.RunQueryWithoutResult(ctx, q)
}

func toString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

// GetDatabaseSummary introspects the available schemas and tables using the
// ANSI information_schema, which Flight SQL engines such as Dremio expose.
func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	tables, err := c.Select(ctx, &query.Query{Query: `
SELECT table_schema, table_name, table_type
FROM information_schema."tables"
WHERE table_schema NOT IN ('information_schema', 'sys', 'INFORMATION_SCHEMA')
`})
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema tables: %w", err)
	}

	databaseName := c.config.Database
	if databaseName == "" {
		databaseName = "flightsql"
	}

	summary := &ansisql.DBDatabase{
		Name:    databaseName,
		Schemas: []*ansisql.DBSchema{},
	}
	schemaCache := make(map[string]*ansisql.DBSchema)

	for _, row := range tables {
		if len(row) < 3 {
			continue
		}

		schemaName, ok := toString(row[0])
		if !ok {
			continue
		}
		tableName, ok := toString(row[1])
		if !ok {
			continue
		}
		tableType, _ := toString(row[2])

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
