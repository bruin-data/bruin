package postgres

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type Client struct {
	connection      connection
	config          PgConfig
	readOnlyDialect string
	schemaCreator   *ansisql.SchemaCreator
	schemaCache     *sync.Map // Cache for created schemas to avoid repeated CREATE SCHEMA calls
	typeMapper      *diff.DatabaseTypeMapper
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

	var queryConn connection = conn
	dialect := readOnlyDialect(c)
	if dialect != "" {
		queryConn = &readOnlyConnection{pool: conn}
	}

	return &Client{
		connection:      queryConn,
		config:          c,
		readOnlyDialect: dialect,
		schemaCreator:   ansisql.NewSchemaCreator(),
		schemaCache:     &sync.Map{},
		typeMapper:      diff.NewPostgresTypeMapper(),
	}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	if err := c.validateReadOnlyQuery(query.String()); err != nil {
		return err
	}
	_, err := c.connection.Exec(ctx, query.String())
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetIngestrURI() (string, error) {
	if cfg, ok := c.config.(interface{ IsReadOnly() bool }); ok && cfg.IsReadOnly() {
		return "", errors.New("read_only connections cannot be used with ingestr")
	}
	return c.config.GetIngestrURI(), nil
}

// Select runs a query and returns the results.
func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	if err := c.validateReadOnlyQuery(query.String()); err != nil {
		return nil, err
	}
	rows, err := c.connection.Query(ctx, query.String(), query.Args...)
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
	return query.CollectRows(ctx, c, queryObj)
}

func (c *Client) StreamWithSchema(ctx context.Context, queryObj *query.Query, h query.RowHandler) (*query.QueryResult, error) {
	if err := c.validateReadOnlyQuery(queryObj.String()); err != nil {
		return nil, err
	}
	return c.streamWithSchema(ctx, queryObj, h)
}

type rowStreamerFunc func(ctx context.Context, q *query.Query, h query.RowHandler) (*query.QueryResult, error)

func (f rowStreamerFunc) StreamWithSchema(ctx context.Context, q *query.Query, h query.RowHandler) (*query.QueryResult, error) {
	return f(ctx, q, h)
}

func (c *Client) selectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	return query.CollectRows(ctx, rowStreamerFunc(c.streamWithSchema), queryObj)
}

func (c *Client) streamWithSchema(ctx context.Context, queryObj *query.Query, h query.RowHandler) (*query.QueryResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	rows, err := c.connection.Query(ctx, queryObj.String())
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()
	// Closing rows drains the rest of the result, so an early return cancels the query first.
	defer cancel()
	// Retrieve column metadata using FieldDescriptions
	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) == 0 && !query.IsLikelyResultQuery(queryObj.String()) {
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			return nil, errors.Wrap(err, "failed to execute query")
		}

		commandTag := rows.CommandTag()
		rowsAffected := commandTag.RowsAffected()
		statementType := query.StatementTypeFromCommandTag(commandTag.String())
		if statementType == "" {
			statementType = query.SQLStatementType(queryObj.String())
		}

		return &query.QueryResult{
			Columns:     []string{},
			Rows:        [][]interface{}{},
			ColumnTypes: []string{},
			Execution:   query.NewExecutionSummaryFromStatement("postgres", statementType, &rowsAffected),
		}, nil
	}
	if fieldDescriptions == nil {
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, errors.Wrap(err, "failed to execute query")
		}
		return nil, errors.New("field descriptions are not available")
	}
	typeMap := pgtype.NewMap()
	// Extract column names
	columns := make([]string, len(fieldDescriptions))
	columnTypes := make([]string, len(fieldDescriptions))
	for i, field := range fieldDescriptions {
		columns[i] = field.Name
		dataType, ok := typeMap.TypeForOID(field.DataTypeOID)
		if !ok {
			columnTypes[i] = ""
		} else {
			columnTypes[i] = dataType.Name
		}
	}
	if err := h.Schema(columns, columnTypes); err != nil {
		return nil, err
	}

	converters := resultValueConverters(fieldDescriptions, typeMap)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, errors.Wrap(err, "failed to collect row values")
		}
		if converters != nil {
			convertResultValues(converters, rows.RawValues(), values)
		}
		if err := h.Row(values); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	return &query.QueryResult{
		Columns:     columns,
		ColumnTypes: columnTypes,
	}, nil
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
	if err := c.validateReadOnlyQuery(query.String()); err != nil {
		return false, err
	}
	// Wrap the query in a PostgreSQL test pattern that doesn't execute
	testQuery := fmt.Sprintf(`DO $TEST$ BEGIN RETURN;
    %s
END; $TEST$;`, query.String())

	rows, err := c.connection.Query(ctx, testQuery)
	if err == nil {
		err = rows.Err()
	}

	if rows != nil {
		defer rows.Close()
	}

	return err == nil, err
}

func (c *Client) DryRunQuery(ctx context.Context, q *query.Query) (*query.DryRunResult, error) {
	if err := c.validateReadOnlyQuery(q.String()); err != nil {
		return nil, err
	}
	explainQuery := &query.Query{Query: "EXPLAIN " + q.String()}
	explainResult, err := c.selectWithSchema(ctx, explainQuery)
	if err != nil {
		return nil, fmt.Errorf("EXPLAIN failed: %w", err)
	}

	return &query.DryRunResult{
		ConnectionType: "postgres",
		Valid:          true,
		ExplainRows:    explainResult,
	}, nil
}

func (c *Client) GetDatabases(ctx context.Context) ([]string, error) {
	q := `
SELECT datname
FROM pg_database
WHERE datistemplate = false
ORDER BY datname;
`

	rows, err := c.connection.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL databases: %w", err)
	}
	defer rows.Close()

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]any, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	var databases []string
	for _, row := range collectedRows {
		if len(row) > 0 {
			if dbName, ok := row[0].(string); ok {
				databases = append(databases, dbName)
			}
		}
	}

	return databases, nil
}

func (c *Client) GetTablesWithSchemas(ctx context.Context, databaseName string) (map[string][]string, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	q := `
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_catalog = $1
    AND table_schema NOT IN ('pg_catalog', 'information_schema')
    AND table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_schema, table_name;
`

	rows, err := c.connection.Query(ctx, q, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables in database '%s': %w", databaseName, err)
	}
	defer rows.Close()

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]any, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	tables := make(map[string][]string)
	for _, row := range collectedRows {
		if len(row) >= 2 {
			if schemaName, ok := row[0].(string); ok {
				if tableName, ok := row[1].(string); ok {
					tables[schemaName] = append(tables[schemaName], tableName)
				}
			}
		}
	}

	return tables, nil
}

func (c *Client) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
	if databaseName == "" && c.config != nil {
		databaseName = c.config.GetDatabase()
	}
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	// Parse table name to extract schema and table components
	tableComponents := strings.Split(tableName, ".")
	var schemaName, tableNameOnly string

	switch len(tableComponents) {
	case 1:
		// table only - use public schema by default
		schemaName = "public"
		tableNameOnly = tableComponents[0]
	case 2:
		// schema.table format
		schemaName = tableComponents[0]
		tableNameOnly = tableComponents[1]
	default:
		return nil, fmt.Errorf("invalid table name format: %s", tableName)
	}

	return c.getColumns(ctx, databaseName, schemaName, tableNameOnly, tableName)
}

func (c *Client) GetColumnsForTable(ctx context.Context, schemaName, tableName string) ([]*ansisql.DBColumn, error) {
	var databaseName string
	if c.config != nil {
		databaseName = c.config.GetDatabase()
	}
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	if schemaName == "" {
		return nil, errors.New("schema name cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	return c.getColumns(ctx, databaseName, schemaName, tableName, schemaName+"."+tableName)
}

func (c *Client) getColumns(ctx context.Context, databaseName, schemaName, tableNameOnly, displayTableName string) ([]*ansisql.DBColumn, error) {
	q := `
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM information_schema.columns
WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
ORDER BY ordinal_position;
`

	rows, err := c.connection.Query(ctx, q, databaseName, schemaName, tableNameOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table '%s.%s': %w", databaseName, displayTableName, err)
	}
	defer rows.Close()

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]any, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	columns := make([]*ansisql.DBColumn, 0, len(collectedRows))
	for _, row := range collectedRows {
		if len(row) < 7 {
			continue
		}

		columnName, ok := row[0].(string)
		if !ok {
			continue
		}

		dataType, ok := row[1].(string)
		if !ok {
			continue
		}

		isNullableStr, ok := row[2].(string)
		if !ok {
			continue
		}

		// Build the full type name with precision/scale if available
		fullType := dataType
		if row[4] != nil {
			if charMaxLength, ok := row[4].(int32); ok && charMaxLength > 0 {
				fullType = fmt.Sprintf("%s(%d)", dataType, charMaxLength)
			}
		} else if row[5] != nil && row[6] != nil {
			if numericPrecision, ok := row[5].(int32); ok {
				if numericScale, ok := row[6].(int32); ok && numericPrecision > 0 {
					if numericScale > 0 {
						fullType = fmt.Sprintf("%s(%d,%d)", dataType, numericPrecision, numericScale)
					} else {
						fullType = fmt.Sprintf("%s(%d)", dataType, numericPrecision)
					}
				}
			}
		}

		column := &ansisql.DBColumn{
			Name:       columnName,
			Type:       fullType,
			Nullable:   strings.ToUpper(isNullableStr) == "YES",
			PrimaryKey: false,
			Unique:     false,
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	return c.getDatabaseSummary(ctx, nil)
}

func (c *Client) GetDatabaseSummaryForSchemas(ctx context.Context, schemaNames []string) (*ansisql.DBDatabase, error) {
	return c.getDatabaseSummary(ctx, schemaNames)
}

func (c *Client) getDatabaseSummary(ctx context.Context, schemaNames []string) (*ansisql.DBDatabase, error) {
	db := c.config.GetDatabase()
	schemaFilter := ""
	args := []any{db}
	if len(schemaNames) > 0 {
		schemaFilter = "\n    AND t.table_schema = ANY($2)"
		args = append(args, schemaNames)
	}

	q := fmt.Sprintf(`
SELECT
    t.table_schema,
    t.table_name,
    t.table_type,
    v.view_definition,
    pg_catalog.obj_description(c.oid, 'pg_class') as table_comment,
    c.reltuples::bigint as row_count,
    pg_catalog.pg_table_size(c.oid) as size_bytes,
    r.rolname as owner
FROM
    information_schema.tables t
LEFT JOIN
    information_schema.views v ON t.table_schema = v.table_schema AND t.table_name = v.table_name
LEFT JOIN
    pg_catalog.pg_class c ON c.relname = t.table_name
LEFT JOIN
    pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
LEFT JOIN
    pg_catalog.pg_roles r ON r.oid = c.relowner
WHERE
	t.table_catalog = $1 
    AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
    %s
    AND t.table_type IN ('BASE TABLE', 'VIEW')
    AND (n.nspname = t.table_schema OR n.nspname IS NULL)
ORDER BY t.table_schema, t.table_name;
`, schemaFilter)

	rows, err := c.connection.Query(ctx, q, args...)
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
		if len(row) < 4 {
			continue
		}

		schemaName := row[0].(string)
		tableName := row[1].(string)
		tableType := row[2].(string)
		var viewDefinition string
		if row[3] != nil {
			viewDefinition = row[3].(string)
		}

		// Extract additional metadata
		var tableComment string
		if len(row) > 4 && row[4] != nil {
			tableComment = row[4].(string)
		}

		var rowCount *int64
		if len(row) > 5 && row[5] != nil {
			if rc, ok := row[5].(int64); ok && rc >= 0 {
				rowCount = &rc
			}
		}

		var sizeBytes *int64
		if len(row) > 6 && row[6] != nil {
			if sb, ok := row[6].(int64); ok && sb >= 0 {
				sizeBytes = &sb
			}
		}

		var owner string
		if len(row) > 7 && row[7] != nil {
			owner = row[7].(string)
		}

		// Create schema if it doesn't exist
		schemaKey := db + "." + schemaName
		if _, exists := schemas[schemaKey]; !exists {
			schema := &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemas[schemaKey] = schema
		}

		// Determine table type
		var dbTableType ansisql.DBTableType
		if tableType == "VIEW" {
			dbTableType = ansisql.DBTableTypeView
		} else {
			dbTableType = ansisql.DBTableTypeTable
		}

		// Add table to schema
		table := &ansisql.DBTable{
			Name:           tableName,
			Type:           dbTableType,
			ViewDefinition: viewDefinition,
			Columns:        []*ansisql.DBColumn{}, // Initialize empty columns array
			Description:    tableComment,
			RowCount:       rowCount,
			SizeBytes:      sizeBytes,
			Owner:          owner,
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
	tableComponents := strings.Split(asset.Name, ".")
	var schemaName string
	switch len(tableComponents) {
	case 2:
		schemaName = tableComponents[0]
	case 3:
		schemaName = tableComponents[1]
	default:
		return nil
	}

	// Check the cache for the schema using the exact case
	// This is important because quoted identifiers in PostgreSQL are case-sensitive
	if _, exists := c.schemaCache.Load(schemaName); exists {
		return nil
	}

	// Create schema with proper quoting to preserve case sensitivity
	quotedSchemaName := QuoteIdentifier(schemaName)
	createQuery := query.Query{
		Query: "CREATE SCHEMA IF NOT EXISTS " + quotedSchemaName,
	}
	// Assets running in parallel can create the same schema at once, and the one that
	// loses fails with a unique violation although the schema now exists.
	if err := c.RunQueryWithoutResult(ctx, &createQuery); err != nil && !isUniqueViolation(err) {
		return errors.Wrapf(err, "failed to create or ensure schema: %s", schemaName)
	}

	// Cache the exact schema name (case-sensitive)
	c.schemaCache.Store(schemaName, true)
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (c *Client) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	var rowCount int64

	// Parse table name to extract schema and table components
	tableComponents := strings.Split(tableName, ".")
	var schemaName, tableNameOnly string

	switch len(tableComponents) {
	case 1:
		// table only - use public schema by default
		schemaName = "public"
		tableNameOnly = tableComponents[0]
	case 2:
		// schema.table format
		schemaName = tableComponents[0]
		tableNameOnly = tableComponents[1]
	default:
		return nil, fmt.Errorf("invalid table name format: %s", tableName)
	}

	// Get row count only if not in schema-only mode
	if !schemaOnly {
		rows, err := c.connection.Query(ctx, "SELECT COUNT(*) as row_count FROM "+tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to execute count query for table '%s': %w", tableName, err)
		}
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

	// Get table schema using information_schema
	schemaQuery := `
	SELECT
		column_name,
		data_type,
		is_nullable,
		column_default,
		character_maximum_length,
		numeric_precision,
		numeric_scale
	FROM information_schema.columns
	WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
	ORDER BY ordinal_position`

	schemaRows, err := c.connection.Query(ctx, schemaQuery, c.config.GetDatabase(), schemaName, tableNameOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to execute schema query for table '%s': %w", tableName, err)
	}
	defer schemaRows.Close()

	var columns []*diff.Column
	for schemaRows.Next() {
		var (
			columnName       string
			dataType         string
			isNullable       string
			columnDefault    *string
			charMaxLength    *int
			numericPrecision *int
			numericScale     *int
		)

		if err := schemaRows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &charMaxLength, &numericPrecision, &numericScale); err != nil {
			return nil, fmt.Errorf("failed to scan schema info for table '%s': %w", tableName, err)
		}

		// Build the full type name with precision/scale if available
		fullType := dataType
		if charMaxLength != nil && *charMaxLength > 0 {
			fullType = fmt.Sprintf("%s(%d)", dataType, *charMaxLength)
		} else if numericPrecision != nil && numericScale != nil && *numericPrecision > 0 {
			if *numericScale > 0 {
				fullType = fmt.Sprintf("%s(%d,%d)", dataType, *numericPrecision, *numericScale)
			} else {
				fullType = fmt.Sprintf("%s(%d)", dataType, *numericPrecision)
			}
		}

		normalizedType := c.typeMapper.MapType(dataType)
		nullable := strings.ToUpper(isNullable) == "YES"

		// TODO: Add logic to detect primary keys and unique constraints
		// This would require additional queries to information_schema.table_constraints
		// and information_schema.key_column_usage

		var stats diff.ColumnStatistics
		if schemaOnly {
			// In schema-only mode, don't collect statistics
			stats = nil
		} else {
			switch normalizedType {
			case diff.CommonTypeNumeric:
				stats, err = c.fetchNumericalStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeString:
				stats, err = c.fetchStringStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeBoolean:
				stats, err = c.fetchBooleanStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeDateTime:
				stats, err = c.fetchDateTimeStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeJSON:
				stats, err = c.fetchJSONStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch JSON stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeBinary, diff.CommonTypeUnknown:
				stats = &diff.UnknownStatistics{}
			}
		}

		columns = append(columns, &diff.Column{
			Name:           columnName,
			Type:           fullType,
			NormalizedType: normalizedType,
			Nullable:       nullable,
			PrimaryKey:     false, // TODO: Implement PK detection
			Unique:         false, // TODO: Implement unique constraint detection
			Stats:          stats,
		})
	}
	if err = schemaRows.Err(); err != nil {
		return nil, fmt.Errorf("error after iterating schema rows for table '%s': %w", tableName, err)
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
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            MIN(%s) as min_val,
            MAX(%s) as max_val,
            AVG(%s::float) as avg_val,
            SUM(%s::float) as sum_val,
            STDDEV(%s::float) as stddev_val
        FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var minVal, maxVal, avgVal, sumVal, stddevVal *float64
		err := rows.Scan(&stats.Count, &stats.NullCount, &minVal, &maxVal, &avgVal, &sumVal, &stddevVal)
		if err != nil {
			return nil, err
		}

		stats.Min = minVal
		stats.Max = maxVal
		stats.Avg = avgVal
		stats.Sum = sumVal
		stats.StdDev = stddevVal
	}

	return stats, rows.Err()
}

func (c *Client) fetchStringStats(ctx context.Context, tableName, columnName string) (*diff.StringStatistics, error) {
	stats := &diff.StringStatistics{}
	query := fmt.Sprintf(`
        SELECT
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(DISTINCT %s) as distinct_count,
            COUNT(CASE WHEN %s = '' THEN 1 END) as empty_count,
            MIN(LENGTH(%s)) as min_length,
            MAX(LENGTH(%s)) as max_length,
            AVG(LENGTH(%s)) as avg_length
        FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var avgLength float64
		err := rows.Scan(&stats.Count, &stats.NullCount, &stats.DistinctCount,
			&stats.EmptyCount, &stats.MinLength, &stats.MaxLength, &avgLength)
		if err != nil {
			return nil, err
		}

		stats.AvgLength = avgLength
	}

	return stats, rows.Err()
}

func (c *Client) fetchBooleanStats(ctx context.Context, tableName, columnName string) (*diff.BooleanStatistics, error) {
	stats := &diff.BooleanStatistics{}
	query := fmt.Sprintf(`
        SELECT
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(CASE WHEN %s = true THEN 1 END) as true_count,
            COUNT(CASE WHEN %s = false THEN 1 END) as false_count
        FROM %s`,
		columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.Count, &stats.NullCount, &stats.TrueCount, &stats.FalseCount)
		if err != nil {
			return nil, err
		}
	}

	return stats, rows.Err()
}

func (c *Client) fetchDateTimeStats(ctx context.Context, tableName, columnName string) (*diff.DateTimeStatistics, error) {
	stats := &diff.DateTimeStatistics{}
	query := fmt.Sprintf(`
        SELECT
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(DISTINCT %s) as unique_count,
            MIN(%s)::text as earliest_date,
            MAX(%s)::text as latest_date
        FROM %s`,
		columnName, columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var earliestDate, latestDate *string
		err := rows.Scan(&stats.Count, &stats.NullCount, &stats.UniqueCount,
			&earliestDate, &latestDate)
		if err != nil {
			return nil, err
		}

		// Parse datetime strings to time.Time objects
		if earliestDate != nil {
			if parsedTime, err := diff.ParseDateTime(*earliestDate); err == nil {
				stats.EarliestDate = parsedTime
			}
		}
		if latestDate != nil {
			if parsedTime, err := diff.ParseDateTime(*latestDate); err == nil {
				stats.LatestDate = parsedTime
			}
		}
	}

	return stats, rows.Err()
}

func (c *Client) fetchJSONStats(ctx context.Context, tableName, columnName string) (*diff.JSONStatistics, error) {
	stats := &diff.JSONStatistics{}
	query := fmt.Sprintf(`
        SELECT
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count
        FROM %s`,
		columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.Count, &stats.NullCount)
		if err != nil {
			return nil, err
		}
	}

	return stats, rows.Err()
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''") // Escape single quotes for SQL safety
}

func (c *Client) PushColumnDescriptions(ctx context.Context, asset *pipeline.Asset) error {
	tableComponents := strings.Split(asset.Name, ".")
	var schemaName string
	var tableName string
	switch len(tableComponents) {
	case 2:
		schemaName = strings.ToUpper(tableComponents[0])
		tableName = strings.ToUpper(tableComponents[1])
	case 3:
		schemaName = strings.ToUpper(tableComponents[1])
		tableName = strings.ToUpper(tableComponents[2])
	default:
		return errors.Errorf("table name must be in schema.table or table format, '%s' given", asset.Name)
	}

	if asset.Description == "" && len(asset.Columns) == 0 {
		return errors.New("no metadata to push: table and columns have no descriptions")
	}

	var updateQueries []string //nolint:prealloc
	for _, col := range asset.Columns {
		query := fmt.Sprintf(
			`COMMENT ON COLUMN %s.%s.%s IS '%s';`,
			schemaName, tableName, col.Name, escapeSQLString(col.Description),
		)
		updateQueries = append(updateQueries, query)
	}

	if len(updateQueries) > 0 {
		batchQuery := strings.Join(updateQueries, "\n")
		if err := c.RunQueryWithoutResult(ctx, &query.Query{Query: batchQuery}); err != nil {
			return errors.Wrap(err, "failed to update column descriptions")
		}
	}

	if asset.Description != "" {
		updateTableQuery := fmt.Sprintf(
			`COMMENT ON TABLE %s.%s IS '%s';`,
			schemaName, tableName, escapeSQLString(asset.Description),
		)
		if err := c.RunQueryWithoutResult(ctx, &query.Query{Query: updateTableQuery}); err != nil {
			return errors.Wrap(err, "failed to update table description")
		}
	}

	return nil
}

func (c *Client) BuildTableExistsQuery(tableName string) (string, error) {
	tableComponents := strings.Split(tableName, ".")
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
		}
	}

	var schemaName string
	switch len(tableComponents) {
	case 1:
		schemaName = "public"
		tableName = tableComponents[0]
	case 2:
		schemaName = tableComponents[0]
		tableName = tableComponents[1]
	default:
		return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
	}
	targetTable := tableName

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname = '%s' AND tablename = '%s'",
		schemaName,
		targetTable,
	)
	return strings.TrimSpace(query), nil
}

// RelationExists reports whether a relation named tableName exists, so an
// incremental asset can create its table on the first run. The materializer
// writes the asset name with every part quoted, and the name is resolved the
// way the server resolves that quoted name.
func (c *Client) RelationExists(ctx context.Context, tableName string) (bool, error) {
	parts := strings.Split(tableName, ".")
	if len(parts) > 3 {
		return false, fmt.Errorf("table name must be in format `table`, `schema.table`, or `database.schema.table`, %q given", tableName)
	}
	for _, part := range parts {
		// QuoteIdentifier does not escape double quotes, so such a name cannot be resolved.
		if part == "" || strings.Contains(part, `"`) {
			return false, fmt.Errorf("table name %q is not a valid identifier", tableName)
		}
	}

	if _, ok := c.config.(RedShiftConfig); ok {
		return c.redshiftRelationExists(ctx, tableName, parts)
	}

	// to_regclass resolves the quoted name like the materialized statements do:
	// one-part names through search_path, every part case-sensitive. It returns
	// NULL when the relation or its schema is missing, and fails for a database
	// other than the current one or a schema the user cannot use.
	exists, err := queryRow(ctx, c.connection, "SELECT to_regclass($1) IS NOT NULL", pgx.RowTo[bool], QuoteIdentifier(tableName))
	if err != nil {
		return false, fmt.Errorf("failed to check whether table '%s' exists: %w", tableName, err)
	}

	return exists, nil
}

// redshiftRelationExists looks the name up in the catalog, since Redshift has
// no to_regclass. Redshift folds even quoted identifiers to lower case unless
// enable_case_sensitive_identifier is on, so names are compared
// case-insensitively, which can only match more relations than the server
// resolves the name to.
func (c *Client) redshiftRelationExists(ctx context.Context, tableName string, parts []string) (bool, error) {
	if len(parts) == 3 && !strings.EqualFold(parts[0], c.config.GetDatabase()) {
		return false, fmt.Errorf("table '%s' is not in the connection's database '%s'", tableName, c.config.GetDatabase())
	}

	// One-part names resolve through search_path, the way pg_table_is_visible checks them.
	relationScope := "pg_catalog.pg_table_is_visible(c.oid)"
	externalScope := ""
	args := []any{parts[len(parts)-1]}
	if len(parts) > 1 {
		relationScope = "lower(n.nspname) = lower($2)"
		externalScope = " AND lower(schemaname) = lower($2)"
		args = append(args, parts[len(parts)-2])
	}

	// pg_class holds tables, views, late-binding views and materialized views.
	relations, err := queryRow(ctx, c.connection, "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE lower(c.relname) = lower($1) AND "+relationScope, pgx.RowTo[int64], args...)
	if err != nil {
		return false, fmt.Errorf("failed to check whether table '%s' exists: %w", tableName, err)
	}
	if relations > 0 {
		return true, nil
	}

	// Spectrum external tables are not in pg_class. A one-part name matches them in every external schema.
	externalTables, err := queryRow(ctx, c.connection, "SELECT count(*) FROM svv_external_tables WHERE lower(tablename) = lower($1)"+externalScope, pgx.RowTo[int64], args...)
	if err != nil {
		return false, fmt.Errorf("failed to check whether external table '%s' exists: %w", tableName, err)
	}

	return externalTables > 0, nil
}

//nolint:ireturn
func queryRow[T any](ctx context.Context, conn connection, sql string, fn pgx.RowToFunc[T], args ...any) (T, error) {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		var zero T
		return zero, err
	}

	return pgx.CollectExactlyOneRow(rows, fn)
}
