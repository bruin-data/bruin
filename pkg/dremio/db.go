package dremio

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	// Registers the "flightsql" database/sql driver provided by Apache ADBC.
	// Dremio speaks the Arrow Flight SQL protocol, so this is the driver we use.
	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

type Client struct {
	connection  connection
	config      Config
	folderCache sync.Map
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
		return nil, errors.Wrap(err, "failed to open Dremio connection")
	}

	return &Client{
		connection: conn,
		config:     c,
	}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	queryStr := strings.TrimSpace(query.String())
	queryStr = strings.TrimSuffix(queryStr, ";")

	// We execute write statements via ExecContext (Flight SQL ExecuteUpdate)
	// rather than QueryContext. Dremio answers a CTAS/INSERT with an Iceberg
	// write summary whose schema does not match the schema it advertises in the
	// Flight info, so the result-reading path (QueryContext/ExecuteQuery) fails
	// with "endpoint returned inconsistent schema". ExecuteUpdate returns an
	// affected-row count without reading a result stream, avoiding the mismatch.
	// ExecContext prepares the statement first, which Dremio supports.
	if _, err := c.connection.ExecContext(ctx, queryStr); err != nil {
		return errors.Wrap(err, "failed to execute query")
	}

	return nil
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
	// Use the read path: RunQueryWithoutResult goes through ExecuteUpdate, which
	// Dremio rejects for a SELECT. Select reads the result stream, whose schema
	// matches the Flight info for a plain SELECT, so it is the right liveness check.
	_, err := c.Select(ctx, &query.Query{Query: "SELECT 1"})
	return err
}

// folderPathForAsset returns the Dremio folder path an asset's table lives in,
// i.e. the asset name minus its final (table) component. For "folder.table" it
// returns "folder"; for "source.folder.table" it returns "source.folder". A
// bare table name (no dot) has no enclosing folder, so it returns "".
func folderPathForAsset(name string) string {
	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		return ""
	}
	return strings.Join(parts[:len(parts)-1], ".")
}

// CreateSchemaIfNotExist ensures the folder an asset's table lives in exists
// before the table is created. Dremio rejects CREATE TABLE into a folder that
// does not exist yet, so we create it first. The folder path is derived from
// the asset name (see folderPathForAsset) and quoted as a dotted identifier.
func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	folderPath := folderPathForAsset(asset.Name)
	if folderPath == "" {
		return nil
	}
	if _, exists := c.folderCache.Load(folderPath); exists {
		return nil
	}

	q := &query.Query{Query: "CREATE FOLDER IF NOT EXISTS " + quoteIdentifier(folderPath)}
	if err := c.RunQueryWithoutResult(ctx, q); err != nil {
		return errors.Wrapf(err, "failed to ensure Dremio folder %q exists", folderPath)
	}
	c.folderCache.Store(folderPath, true)

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
// ANSI information_schema, which Dremio exposes.
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
		databaseName = "dremio"
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
