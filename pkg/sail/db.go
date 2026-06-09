package sail

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	// Registers the "flightsql" database/sql driver provided by Apache ADBC.
	// Sail speaks the Arrow Flight SQL protocol, so this is the driver we use.
	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

type Client struct {
	connection  connection
	config      Config
	schemaCache sync.Map
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
		return nil, errors.Wrap(err, "failed to open Sail connection")
	}

	return &Client{
		connection: conn,
		config:     c,
	}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	queryStr := strings.TrimSpace(query.String())
	queryStr = strings.TrimSuffix(queryStr, ";")

	// We deliberately execute via QueryContext (Flight SQL ExecuteQuery) rather
	// than ExecContext. Sail implements neither prepared statements
	// ("do_action_create_prepared_statement has no default implementation", which
	// ExecContext needs) nor statement-update ("do_put_statement_update has no
	// default implementation", the ExecuteUpdate path). Its ExecuteQuery path
	// handles DDL/DML (CREATE SCHEMA, CTAS, INSERT) and returns a small status
	// result, so we run everything through it and drain the rows.
	rows, err := c.connection.QueryContext(ctx, queryStr)
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()

	for rows.Next() { //nolint:revive // drain the reader so the statement fully executes server-side
	}

	return errors.Wrap(rows.Err(), "failed to execute query")
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

// schemaForAsset returns the Spark schema (database) an asset's table lives in.
// Only a flat "schema.table" structure is supported, so the schema is the first
// component of a two-part name. Any other shape (a bare table, which lands in
// the default database, or a deeper path) has no schema to auto-create, so it
// returns "".
func schemaForAsset(name string) string {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return ""
	}
	return parts[0]
}

// CreateSchemaIfNotExist ensures the schema (Spark database) an asset's table
// lives in exists before the table is created. Sail rejects a CREATE TABLE into
// a database that does not exist ("Database not found: ..."), so we create it
// first with CREATE SCHEMA IF NOT EXISTS.
func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	schemaName := schemaForAsset(asset.Name)
	if schemaName == "" {
		return nil
	}
	if _, exists := c.schemaCache.Load(schemaName); exists {
		return nil
	}

	q := &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(schemaName)}
	if err := c.RunQueryWithoutResult(ctx, q); err != nil {
		return errors.Wrapf(err, "failed to ensure Sail schema %q exists", schemaName)
	}
	c.schemaCache.Store(schemaName, true)

	return nil
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
// ANSI information_schema.
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
		databaseName = "sail"
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
