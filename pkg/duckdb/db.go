//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	_ "github.com/marcboeker/go-duckdb"
)

type Client struct {
	connection    connection
	config        DuckDBConfig
	schemaCreator *ansisql.SchemaCreator
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

	return &Client{connection: conn, config: c, schemaCreator: ansisql.NewSchemaCreator()}, nil
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

func (c *Client) GetDBConnectionURI() (string, error) {
	return c.config.ToDBConnectionURI(), nil
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

func (c *Client) SelectWithSchema(ctx context.Context, queryObject *query.Query) (*query.QueryResult, error) {
	LockDatabase(c.config.ToDBConnectionURI())
	defer UnlockDatabase(c.config.ToDBConnectionURI())

	rows, err := c.connection.QueryContext(ctx, queryObject.String())
	if err != nil {
		return nil, err
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	defer rows.Close()

	// Initialize QueryResult
	result := &query.QueryResult{
		Columns:     []string{},
		ColumnTypes: []string{},
		Rows:        [][]interface{}{},
	}

	// Fetch column names and populate Columns slice
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result.Columns = cols
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	typeStrings := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeStrings[i] = ct.DatabaseTypeName()
	}
	result.ColumnTypes = typeStrings

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

		result.Rows = append(result.Rows, columns)
	}

	return result, nil
}

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	return c.schemaCreator.CreateSchemaIfNotExist(ctx, c, asset)
}

func (c *Client) GetTableSummary(ctx context.Context, tableName string) (*diff.TableSummaryResult, error) {
	// Get row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s", tableName)
	rows, err := c.connection.QueryContext(ctx, countQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute count query for table '%s': %w", tableName, err)
	}
	// It's important to close rows, but deferring here might be too early if schemaRows.Close() fails later.
	// We will close it explicitly after use.

	var rowCount int64
	if rows.Next() {
		if err := rows.Scan(&rowCount); err != nil {
			rows.Close() // Close before returning
			return nil, fmt.Errorf("failed to scan row count for table '%s': %w", tableName, err)
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close() // Close before returning
		return nil, fmt.Errorf("error after iterating rows for count query on table '%s': %w", tableName, err)
	}
	rows.Close() // Explicitly close rows after we are done with them

	// Get table schema using PRAGMA table_info
	schemaQuery := fmt.Sprintf("PRAGMA table_info('%s')", tableName)
	schemaRows, err := c.connection.QueryContext(ctx, schemaQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute PRAGMA table_info for table '%s': %w", tableName, err)
	}
	defer schemaRows.Close() // Defer close for schemaRows

	var columns []*diff.Column
	for schemaRows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   bool
			dfltValue sql.NullString
			pk        bool
		)

		if err := schemaRows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("failed to scan PRAGMA table_info result for table '%s': %w", tableName, err)
		}

		var stats diff.ColumnStatistics
		switch strings.ToLower(colType) {
		case "integer", "bigint", "tinyint", "smallint", "double", "float", "decimal", "numeric", "real":
			stats = &diff.NumericalStatistics{}
		case "varchar", "char", "text", "string":
			stats = &diff.StringStatistics{}
		case "boolean":
			stats = &diff.BooleanStatistics{}
		case "date", "time", "timestamp", "datetime":
			stats = &diff.DateTimeStatistics{}
		default:
			stats = &diff.UnknownStatistics{}
		}

		columns = append(columns, &diff.Column{
			Name:       name,
			Type:       colType,
			Nullable:   !notNull,
			PrimaryKey: pk,
			Unique:     pk,
			Stats:      stats,
		})
	}
	if err = schemaRows.Err(); err != nil {
		return nil, fmt.Errorf("error after iterating PRAGMA table_info results for table '%s': %w", tableName, err)
	}

	dbTable := &diff.Table{
		Name:    tableName,
		Columns: columns,
	}

	return &diff.TableSummaryResult{
		RowCount: rowCount,
		Table:    dbTable,
	}, nil
}
