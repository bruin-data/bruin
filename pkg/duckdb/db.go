//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/marcboeker/go-duckdb"   //nolint:stylecheck
	_ "github.com/marcboeker/go-duckdb" //nolint:stylecheck
)

type Client struct {
	connection    connection
	config        DuckDBConfig
	schemaCreator *ansisql.SchemaCreator
	typeMapper    *diff.DatabaseTypeMapper
}

type DuckDBConfig interface {
	ToDBConnectionURI() string
	GetIngestrURI() string
}

type connection interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func NewClient(c DuckDBConfig) (*Client, error) {
	LockDatabase(c.ToDBConnectionURI())
	defer UnlockDatabase(c.ToDBConnectionURI())
	conn, err := NewEphemeralConnection(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		connection:    conn,
		config:        c,
		schemaCreator: ansisql.NewSchemaCreator(),
		typeMapper:    diff.NewDuckDBTypeMapper(),
	}, nil
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

		// Convert DuckDB-specific types (especially decimals)
		for i, val := range columns {
			columns[i] = c.convertValue(val)
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

		// Convert DuckDB-specific types (especially decimals)
		for i, val := range columns {
			columns[i] = c.convertValue(val)
		}

		result.Rows = append(result.Rows, columns)
	}

	return result, nil
}

func (c *Client) convertValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	if decimal, ok := val.(duckdb.Decimal); ok {
		return decimal.Float64()
	}

	return val
}

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	return c.schemaCreator.CreateSchemaIfNotExist(ctx, c, asset)
}

func (c *Client) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	var rowCount int64

	// Get row count only if not in schema-only mode
	if !schemaOnly {
		countQuery := "SELECT COUNT(*) as row_count FROM " + tableName
		rows, err := c.connection.QueryContext(ctx, countQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to execute count query for table '%s': %w", tableName, err)
		}
		// It's important to close rows, but deferring here might be too early if schemaRows.Close() fails later.
		// We will close it explicitly after use.

		defer rows.Close()
		if rows.Next() {
			var countValue interface{}
			if err := rows.Scan(&countValue); err != nil {
				return nil, fmt.Errorf("failed to scan row count for table '%s': %w", tableName, err)
			}

			// Handle different numeric types for row count
			switch val := countValue.(type) {
			case int64:
				rowCount = val
			case int:
				rowCount = int64(val)
			case int32:
				rowCount = int64(val)
			case float64:
				rowCount = int64(val)
			case string:
				// Handle string representation of numbers
				parsed, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to parse row count string '%s' for table '%s': %w", val, tableName, err)
				}
				rowCount = parsed
			default:
				return nil, fmt.Errorf("unexpected row count type for table '%s': got %T with value %v", tableName, val, val)
			}
		}
		if err = rows.Err(); err != nil {
			return nil, fmt.Errorf("error after iterating rows for count query on table '%s': %w", tableName, err)
		}
	}

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

		normalizedType := c.typeMapper.MapType(colType)

		var stats diff.ColumnStatistics
		if schemaOnly {
			// In schema-only mode, don't collect statistics
			stats = nil
		} else {
			switch normalizedType {
			case diff.CommonTypeNumeric:
				stats, err = c.fetchNumericalStats(ctx, tableName, name)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", name, err)
				}
			case diff.CommonTypeString:
				stats, err = c.fetchStringStats(ctx, tableName, name)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", name, err)
				}
			case diff.CommonTypeBoolean:
				stats, err = c.fetchBooleanStats(ctx, tableName, name)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", name, err)
				}
			case diff.CommonTypeDateTime:
				stats, err = c.fetchDateTimeStats(ctx, tableName, name)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", name, err)
				}
			case diff.CommonTypeJSON:
				stats, err = c.fetchJSONStats(ctx, tableName, name)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch JSON stats for column '%s': %w", name, err)
				}
			case diff.CommonTypeBinary, diff.CommonTypeUnknown:
				stats = &diff.UnknownStatistics{}
			}
		}

		columns = append(columns, &diff.Column{
			Name:           name,
			Type:           colType,
			NormalizedType: normalizedType,
			Nullable:       !notNull,
			PrimaryKey:     pk,
			Unique:         pk,
			Stats:          stats,
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

func (c *Client) fetchNumericalStats(ctx context.Context, tableName, columnName string) (*diff.NumericalStatistics, error) {
	stats := &diff.NumericalStatistics{}
	query := fmt.Sprintf(`
        SELECT 
            MIN(%s) as min_val,
            MAX(%s) as max_val,
            AVG(%s) as avg_val,
            SUM(%s) as sum_val,
            COUNT(%s) as count_val,
            COUNT(*) - COUNT(%s) as null_count,
            STDDEV(%s) as stddev_val
        FROM %s
    `, columnName, columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	err := c.connection.QueryRowContext(ctx, query).Scan(
		&stats.Min,
		&stats.Max,
		&stats.Avg,
		&stats.Sum,
		&stats.Count,
		&stats.NullCount,
		&stats.StdDev,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (c *Client) fetchStringStats(ctx context.Context, tableName, columnName string) (*diff.StringStatistics, error) {
	stats := &diff.StringStatistics{}

	// Get min length, max length, avg length
	query := fmt.Sprintf(`
        SELECT 
            MIN(LENGTH(%s)) as min_len,
            MAX(LENGTH(%s)) as max_len,
            AVG(LENGTH(%s)) as avg_len,
            COUNT(DISTINCT %s) as distinct_count,
            COUNT(*) as total_count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(CASE WHEN %s = '' THEN 1 END) as empty_count
        FROM %s
    `, columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	err := c.connection.QueryRowContext(ctx, query).Scan(
		&stats.MinLength,
		&stats.MaxLength,
		&stats.AvgLength,
		&stats.DistinctCount,
		&stats.Count,
		&stats.NullCount,
		&stats.EmptyCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (c *Client) fetchBooleanStats(ctx context.Context, tableName, columnName string) (*diff.BooleanStatistics, error) {
	stats := &diff.BooleanStatistics{}

	// Get true count and total count
	query := fmt.Sprintf(`
        SELECT 
            COUNT(CASE WHEN %s = true THEN 1 END) as true_count,
            COUNT(CASE WHEN %s = false THEN 1 END) as false_count,
            COUNT(*) as total_count,
            COUNT(*) - COUNT(%s) as null_count
        FROM %s
    `, columnName, columnName, columnName, tableName)

	err := c.connection.QueryRowContext(ctx, query).Scan(
		&stats.TrueCount,
		&stats.FalseCount,
		&stats.Count,
		&stats.NullCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (c *Client) fetchDateTimeStats(ctx context.Context, tableName, columnName string) (*diff.DateTimeStatistics, error) {
	stats := &diff.DateTimeStatistics{}

	// Get min, max dates with explicit string conversion
	query := fmt.Sprintf(`
        SELECT 
            CAST(MIN(%s) AS VARCHAR) as min_date,
            CAST(MAX(%s) AS VARCHAR) as max_date,
            COUNT(DISTINCT %s) as unique_count,
            COUNT(*) as count_val,
            COUNT(*) - COUNT(%s) as null_count
        FROM %s
    `, columnName, columnName, columnName, columnName, tableName)

	var minDate, maxDate interface{}
	err := c.connection.QueryRowContext(ctx, query).Scan(
		&minDate,
		&maxDate,
		&stats.UniqueCount,
		&stats.Count,
		&stats.NullCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", columnName, err)
	}

	// Handle datetime values - convert to proper time.Time objects
	if minDate != nil {
		if parsedTime, err := diff.ParseDateTime(minDate); err == nil {
			stats.EarliestDate = parsedTime
		}
	}

	if maxDate != nil {
		if parsedTime, err := diff.ParseDateTime(maxDate); err == nil {
			stats.LatestDate = parsedTime
		}
	}

	return stats, nil
}

func (c *Client) fetchJSONStats(ctx context.Context, tableName, columnName string) (*diff.JSONStatistics, error) {
	stats := &diff.JSONStatistics{}

	// Get count and null count for JSON columns
	query := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count_val,
            COUNT(*) - COUNT(%s) as null_count
        FROM %s
    `, columnName, tableName)

	err := c.connection.QueryRowContext(ctx, query).Scan(
		&stats.Count,
		&stats.NullCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JSON stats for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (c *Client) GetDatabases(ctx context.Context) ([]string, error) {
	q := `
SELECT DISTINCT table_schema
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema;
`

	rows, err := c.connection.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("failed to query DuckDB schemas: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, fmt.Errorf("failed to scan schema name: %w", err)
		}
		databases = append(databases, schemaName)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over schema rows: %w", err)
	}

	return databases, nil
}

func (c *Client) GetTables(ctx context.Context, databaseName string) ([]string, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	q := `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = ?
    AND table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_name;
`

	rows, err := c.connection.QueryContext(ctx, q, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables in schema '%s': %w", databaseName, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over table rows: %w", err)
	}

	return tables, nil
}

func (c *Client) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	q := `
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position;
`

	rows, err := c.connection.QueryContext(ctx, q, databaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table '%s.%s': %w", databaseName, tableName, err)
	}
	defer rows.Close()

	var columns []*ansisql.DBColumn
	for rows.Next() {
		var (
			columnName    string
			dataType      string
			isNullable    string
			columnDefault sql.NullString
		)

		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		column := &ansisql.DBColumn{
			Name:       columnName,
			Type:       dataType,
			Nullable:   isNullable == "YES",
			PrimaryKey: false,
			Unique:     false,
		}

		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over column rows: %w", err)
	}

	return columns, nil
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// DuckDB uses a catalog approach, we'll use the INFORMATION_SCHEMA
	// First, let's get all schemas and tables
	q := `
SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
    table_type IN ('BASE TABLE', 'VIEW')
    AND table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema, table_name;
`

	rows, err := c.connection.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("failed to query DuckDB information_schema: %w", err)
	}
	defer rows.Close()

	summary := &ansisql.DBDatabase{
		Name:    "duckdb", // DuckDB doesn't have a specific database name concept like traditional databases
		Schemas: []*ansisql.DBSchema{},
	}
	schemas := make(map[string]*ansisql.DBSchema)

	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan schema and table names: %w", err)
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
			Name:    tableName,
			Columns: []*ansisql.DBColumn{}, // Initialize empty columns array
		}
		schemas[schemaName].Tables = append(schemas[schemaName].Tables, table)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over schema rows: %w", err)
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
