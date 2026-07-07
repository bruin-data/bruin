package athena

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	drv "github.com/uber/athenadriver/go"
)

type DB struct {
	conn       *sqlx.DB
	config     *Config
	mutex      sync.Mutex
	typeMapper *diff.DatabaseTypeMapper
}

func NewDB(c *Config) *DB {
	return &DB{
		config:     c,
		mutex:      sync.Mutex{},
		typeMapper: diff.NewAthenaTypeMapper(),
	}
}

func (db *DB) GetResultsLocation() string {
	return db.config.OutputBucket
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	err := db.initializeDB(ctx)
	if err != nil {
		return err
	}
	_, err = db.Select(ctx, query)
	return err
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	err := db.initializeDB(ctx)
	if err != nil {
		return nil, err
	}
	queryString := query.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err == nil {
		err = rows.Err()
	}

	if err != nil {
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
	}

	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		return nil, err
	}

	var result [][]interface{}

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

	return result, err
}

func (db *DB) SelectWithSchema(ctx context.Context, queryObject *query.Query) (*query.QueryResult, error) {
	// Initialize the database connection
	err := db.initializeDB(ctx)
	if err != nil {
		return nil, err
	}

	// Prepare and execute the query
	queryString := queryObject.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Initialize the QueryResult struct
	result := &query.QueryResult{
		Columns:     []string{},
		Rows:        [][]interface{}{},
		ColumnTypes: []string{},
	}

	// Retrieve column names (schema)
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column names: %w", err)
	}
	result.Columns = columns

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column types: %w", err)
	}
	typeStrings := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeStrings[i] = ct.DatabaseTypeName()
	}
	result.ColumnTypes = typeStrings

	// Fetch rows and add them to the result
	for rows.Next() {
		// Create a slice for column values
		columnValues := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		// Scan the row into column pointers
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Append the row to the result
		result.Rows = append(result.Rows, columnValues)
	}

	// Check for any row errors
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while reading rows: %w", err)
	}

	return result, nil
}

func (db *DB) initializeDB(ctx context.Context) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.conn != nil {
		return nil
	}

	athenaURI, err := db.config.ToDBConnectionURI()
	if err != nil {
		return errors.Wrap(err, "failed to create DSN for Athena")
	}

	if athenaURI == "" {
		return errors.New("failed to create DSN for Athena")
	}

	conn, err := sqlx.ConnectContext(ctx, drv.DriverName, athenaURI)
	if err != nil {
		return errors.Errorf("Failed to connect to Athena: %v", err)
	}

	db.conn = conn
	return nil
}

func (db *DB) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}
	err := db.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Athena connection")
	}

	return nil
}

func (db *DB) GetDatabases(ctx context.Context) ([]string, error) {
	q := `
SELECT DISTINCT table_schema
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema')
ORDER BY table_schema;
`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Athena schemas: %w", err)
	}

	var databases []string
	for _, row := range result {
		if len(row) > 0 {
			if schemaName, ok := row[0].(string); ok {
				databases = append(databases, schemaName)
			}
		}
	}

	return databases, nil
}

func (db *DB) GetTables(ctx context.Context, databaseName string) ([]string, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	q := fmt.Sprintf(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = '%s'
    AND table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_name;
`, databaseName)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query tables in schema '%s': %w", databaseName, err)
	}

	var tables []string
	for _, row := range result {
		if len(row) > 0 {
			if tableName, ok := row[0].(string); ok {
				tables = append(tables, tableName)
			}
		}
	}

	return tables, nil
}

func (db *DB) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	q := fmt.Sprintf(`
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = '%s' AND table_name = '%s'
ORDER BY ordinal_position;
`, databaseName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table '%s.%s': %w", databaseName, tableName, err)
	}

	columns := make([]*ansisql.DBColumn, 0, len(result))
	for _, row := range result {
		if len(row) < 3 {
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

		column := &ansisql.DBColumn{
			Name:       columnName,
			Type:       dataType,
			Nullable:   strings.ToUpper(isNullableStr) == "YES",
			PrimaryKey: false,
			Unique:     false,
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// Athena uses AWS Glue Data Catalog
	// We'll query INFORMATION_SCHEMA to get all schemas and tables with their types
	q := `
SELECT
    table_schema,
    table_name,
    table_type
FROM
    information_schema.tables
WHERE
    table_type IN ('BASE TABLE', 'VIEW')
    AND table_schema NOT IN ('information_schema')
ORDER BY table_schema, table_name;
`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Athena information_schema: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    "athena", // Athena catalog
		Schemas: []*ansisql.DBSchema{},
	}
	schemas := make(map[string]*ansisql.DBSchema)

	for _, row := range result {
		if len(row) != 3 {
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
		tableType, ok := row[2].(string)
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

		// Determine table type
		var dbTableType ansisql.DBTableType
		if tableType == "VIEW" {
			dbTableType = ansisql.DBTableTypeView
		} else {
			dbTableType = ansisql.DBTableTypeTable
		}

		// Add table to schema
		// Note: Athena doesn't provide view definitions in INFORMATION_SCHEMA
		table := &ansisql.DBTable{
			Name:    tableName,
			Type:    dbTableType,
			Columns: []*ansisql.DBColumn{}, // Initialize empty columns array
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

func (db *DB) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	schemaName, tableNameOnly, qualifiedTableName, err := db.parseTableName(tableName)
	if err != nil {
		return nil, err
	}

	var rowCount int64
	if !schemaOnly {
		rowCount, err = db.fetchRowCount(ctx, qualifiedTableName, tableName)
		if err != nil {
			return nil, err
		}
	}

	schemaResult, err := db.Select(ctx, &query.Query{Query: buildAthenaSchemaQuery(schemaName, tableNameOnly)})
	if err != nil {
		return nil, fmt.Errorf("failed to execute schema query for table '%s': %w", tableName, err)
	}

	typeMapper := db.typeMapper
	if typeMapper == nil {
		typeMapper = diff.NewAthenaTypeMapper()
	}

	columns := make([]*diff.Column, 0, len(schemaResult))
	for _, row := range schemaResult {
		if len(row) < 3 {
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

		normalizedType := typeMapper.MapType(dataType)
		nullable := strings.EqualFold(isNullableStr, "YES")

		var stats diff.ColumnStatistics
		if schemaOnly {
			stats = nil
		} else {
			switch normalizedType {
			case diff.CommonTypeNumeric:
				stats, err = db.fetchNumericalStats(ctx, qualifiedTableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeString:
				stats, err = db.fetchStringStats(ctx, qualifiedTableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeBoolean:
				stats, err = db.fetchBooleanStats(ctx, qualifiedTableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeDateTime:
				stats, err = db.fetchDateTimeStats(ctx, qualifiedTableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeJSON:
				stats, err = db.fetchJSONStats(ctx, qualifiedTableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch JSON stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeBinary, diff.CommonTypeUnknown:
				stats = &diff.UnknownStatistics{}
			}
		}

		columns = append(columns, &diff.Column{
			Name:           columnName,
			Type:           dataType,
			NormalizedType: normalizedType,
			Nullable:       nullable,
			PrimaryKey:     false,
			Unique:         false,
			Stats:          stats,
		})
	}

	return &diff.TableSummaryResult{
		RowCount: rowCount,
		Table: &diff.Table{
			Name:    tableName,
			Columns: columns,
		},
	}, nil
}

func (db *DB) parseTableName(tableName string) (string, string, string, error) {
	tableComponents := strings.Split(tableName, ".")
	for _, component := range tableComponents {
		if component == "" {
			return "", "", "", fmt.Errorf("table name must be in table or schema.table format, '%s' given", tableName)
		}
	}

	var schemaName string
	var tableNameOnly string

	switch len(tableComponents) {
	case 1:
		if db.config == nil || db.config.Database == "" {
			return "", "", "", fmt.Errorf("database must be configured when table name is not schema-qualified: %s", tableName)
		}
		schemaName = db.config.Database
		tableNameOnly = tableComponents[0]
	case 2:
		schemaName = tableComponents[0]
		tableNameOnly = tableComponents[1]
	default:
		return "", "", "", fmt.Errorf("table name must be in table or schema.table format, '%s' given", tableName)
	}

	return schemaName, tableNameOnly, quoteAthenaQualifiedTableName(schemaName, tableNameOnly), nil
}

func buildAthenaSchemaQuery(schemaName, tableName string) string {
	return fmt.Sprintf(`
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = '%s' AND table_name = '%s'
ORDER BY ordinal_position;
`, escapeAthenaStringLiteral(schemaName), escapeAthenaStringLiteral(tableName))
}

func (db *DB) fetchRowCount(ctx context.Context, qualifiedTableName, originalTableName string) (int64, error) {
	countQuery := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s", qualifiedTableName)
	countResult, err := db.Select(ctx, &query.Query{Query: countQuery})
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query for table '%s': %w", originalTableName, err)
	}

	if len(countResult) == 0 || len(countResult[0]) == 0 {
		return 0, fmt.Errorf("count query returned no rows for table '%s'", originalTableName)
	}

	rowCount, err := athenaInt64Value(countResult[0][0])
	if err != nil {
		return 0, fmt.Errorf("failed to parse row count for table '%s': %w", originalTableName, err)
	}

	return rowCount, nil
}

func (db *DB) fetchNumericalStats(ctx context.Context, qualifiedTableName, columnName string) (*diff.NumericalStatistics, error) {
	quotedColumn := quoteAthenaIdentifier(columnName)
	statsQuery := fmt.Sprintf(`
SELECT
    MIN(TRY_CAST(%s AS DOUBLE)) as min_val,
    MAX(TRY_CAST(%s AS DOUBLE)) as max_val,
    AVG(TRY_CAST(%s AS DOUBLE)) as avg_val,
    SUM(TRY_CAST(%s AS DOUBLE)) as sum_val,
    COUNT(%s) as count_val,
    COUNT(*) - COUNT(%s) as null_count,
    STDDEV(TRY_CAST(%s AS DOUBLE)) as stddev_val
FROM %s
`, quotedColumn, quotedColumn, quotedColumn, quotedColumn, quotedColumn, quotedColumn, quotedColumn, qualifiedTableName)

	result, err := db.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", columnName, err)
	}
	if len(result) == 0 || len(result[0]) < 7 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.NumericalStatistics{}

	if stats.Min, err = athenaOptionalFloat64Value(row[0]); err != nil {
		return nil, fmt.Errorf("failed to parse min value for column '%s': %w", columnName, err)
	}
	if stats.Max, err = athenaOptionalFloat64Value(row[1]); err != nil {
		return nil, fmt.Errorf("failed to parse max value for column '%s': %w", columnName, err)
	}
	if stats.Avg, err = athenaOptionalFloat64Value(row[2]); err != nil {
		return nil, fmt.Errorf("failed to parse avg value for column '%s': %w", columnName, err)
	}
	if stats.Sum, err = athenaOptionalFloat64Value(row[3]); err != nil {
		return nil, fmt.Errorf("failed to parse sum value for column '%s': %w", columnName, err)
	}
	if stats.Count, err = athenaInt64Value(row[4]); err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}
	if stats.NullCount, err = athenaInt64Value(row[5]); err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}
	if stats.StdDev, err = athenaOptionalFloat64Value(row[6]); err != nil {
		return nil, fmt.Errorf("failed to parse stddev value for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (db *DB) fetchStringStats(ctx context.Context, qualifiedTableName, columnName string) (*diff.StringStatistics, error) {
	quotedColumn := quoteAthenaIdentifier(columnName)
	statsQuery := fmt.Sprintf(`
SELECT
    MIN(LENGTH(CAST(%s AS VARCHAR))) as min_len,
    MAX(LENGTH(CAST(%s AS VARCHAR))) as max_len,
    AVG(LENGTH(CAST(%s AS VARCHAR))) as avg_len,
    COUNT(DISTINCT %s) as distinct_count,
    COUNT(*) as total_count,
    COUNT(*) - COUNT(%s) as null_count,
    SUM(CASE WHEN CAST(%s AS VARCHAR) = '' THEN 1 ELSE 0 END) as empty_count
FROM %s
`, quotedColumn, quotedColumn, quotedColumn, quotedColumn, quotedColumn, quotedColumn, qualifiedTableName)

	result, err := db.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", columnName, err)
	}
	if len(result) == 0 || len(result[0]) < 7 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.StringStatistics{}

	if stats.MinLength, err = athenaIntValue(row[0]); err != nil {
		return nil, fmt.Errorf("failed to parse min length for column '%s': %w", columnName, err)
	}
	if stats.MaxLength, err = athenaIntValue(row[1]); err != nil {
		return nil, fmt.Errorf("failed to parse max length for column '%s': %w", columnName, err)
	}
	if stats.AvgLength, err = athenaFloat64Value(row[2]); err != nil {
		return nil, fmt.Errorf("failed to parse avg length for column '%s': %w", columnName, err)
	}
	if stats.DistinctCount, err = athenaInt64Value(row[3]); err != nil {
		return nil, fmt.Errorf("failed to parse distinct count for column '%s': %w", columnName, err)
	}
	if stats.Count, err = athenaInt64Value(row[4]); err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}
	if stats.NullCount, err = athenaInt64Value(row[5]); err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}
	if stats.EmptyCount, err = athenaInt64Value(row[6]); err != nil {
		return nil, fmt.Errorf("failed to parse empty count for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (db *DB) fetchBooleanStats(ctx context.Context, qualifiedTableName, columnName string) (*diff.BooleanStatistics, error) {
	quotedColumn := quoteAthenaIdentifier(columnName)
	statsQuery := fmt.Sprintf(`
SELECT
    SUM(CASE WHEN %s = true THEN 1 ELSE 0 END) as true_count,
    SUM(CASE WHEN %s = false THEN 1 ELSE 0 END) as false_count,
    COUNT(*) as total_count,
    COUNT(*) - COUNT(%s) as null_count
FROM %s
`, quotedColumn, quotedColumn, quotedColumn, qualifiedTableName)

	result, err := db.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", columnName, err)
	}
	if len(result) == 0 || len(result[0]) < 4 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.BooleanStatistics{}

	if stats.TrueCount, err = athenaInt64Value(row[0]); err != nil {
		return nil, fmt.Errorf("failed to parse true count for column '%s': %w", columnName, err)
	}
	if stats.FalseCount, err = athenaInt64Value(row[1]); err != nil {
		return nil, fmt.Errorf("failed to parse false count for column '%s': %w", columnName, err)
	}
	if stats.Count, err = athenaInt64Value(row[2]); err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}
	if stats.NullCount, err = athenaInt64Value(row[3]); err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (db *DB) fetchDateTimeStats(ctx context.Context, qualifiedTableName, columnName string) (*diff.DateTimeStatistics, error) {
	quotedColumn := quoteAthenaIdentifier(columnName)
	statsQuery := fmt.Sprintf(`
SELECT
    CAST(MIN(%s) AS VARCHAR) as min_date,
    CAST(MAX(%s) AS VARCHAR) as max_date,
    COUNT(DISTINCT %s) as unique_count,
    COUNT(*) as count_val,
    COUNT(*) - COUNT(%s) as null_count
FROM %s
`, quotedColumn, quotedColumn, quotedColumn, quotedColumn, qualifiedTableName)

	result, err := db.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", columnName, err)
	}
	if len(result) == 0 || len(result[0]) < 5 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.DateTimeStatistics{}

	if row[0] != nil {
		if parsedTime, parseErr := diff.ParseDateTime(row[0]); parseErr == nil {
			stats.EarliestDate = parsedTime
		}
	}
	if row[1] != nil {
		if parsedTime, parseErr := diff.ParseDateTime(row[1]); parseErr == nil {
			stats.LatestDate = parsedTime
		}
	}
	if stats.UniqueCount, err = athenaInt64Value(row[2]); err != nil {
		return nil, fmt.Errorf("failed to parse unique count for column '%s': %w", columnName, err)
	}
	if stats.Count, err = athenaInt64Value(row[3]); err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}
	if stats.NullCount, err = athenaInt64Value(row[4]); err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func (db *DB) fetchJSONStats(ctx context.Context, qualifiedTableName, columnName string) (*diff.JSONStatistics, error) {
	quotedColumn := quoteAthenaIdentifier(columnName)
	statsQuery := fmt.Sprintf(`
SELECT
    COUNT(*) as count_val,
    COUNT(*) - COUNT(%s) as null_count
FROM %s
`, quotedColumn, qualifiedTableName)

	result, err := db.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JSON stats for column '%s': %w", columnName, err)
	}
	if len(result) == 0 || len(result[0]) < 2 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.JSONStatistics{}

	if stats.Count, err = athenaInt64Value(row[0]); err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}
	if stats.NullCount, err = athenaInt64Value(row[1]); err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	return stats, nil
}

func athenaIntValue(value interface{}) (int, error) {
	int64Value, err := athenaInt64Value(value)
	if err != nil {
		return 0, err
	}

	return int(int64Value), nil
}

func athenaInt64Value(value interface{}) (int64, error) {
	switch val := value.(type) {
	case nil:
		return 0, nil
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	case []byte:
		return athenaInt64Value(string(val))
	case string:
		trimmed := strings.TrimSpace(val)
		if trimmed == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(trimmed, 10, 64)
		if err == nil {
			return parsed, nil
		}
		parsedFloat, floatErr := strconv.ParseFloat(trimmed, 64)
		if floatErr != nil {
			return 0, err
		}
		return int64(parsedFloat), nil
	default:
		return 0, fmt.Errorf("unexpected numeric value type %T with value %v", val, val)
	}
}

func athenaFloat64Value(value interface{}) (float64, error) {
	switch val := value.(type) {
	case nil:
		return 0, nil
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case []byte:
		return athenaFloat64Value(string(val))
	case string:
		trimmed := strings.TrimSpace(val)
		if trimmed == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected numeric value type %T with value %v", val, val)
	}
}

func athenaOptionalFloat64Value(value interface{}) (*float64, error) {
	if value == nil {
		return nil, nil
	}

	parsed, err := athenaFloat64Value(value)
	if err != nil {
		return nil, err
	}

	return &parsed, nil
}

func quoteAthenaQualifiedTableName(schemaName, tableName string) string {
	return quoteAthenaIdentifier(schemaName) + "." + quoteAthenaIdentifier(tableName)
}

func quoteAthenaIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func escapeAthenaStringLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func (db *DB) BuildTableExistsQuery(tableName string) (string, error) {
	tableComponents := strings.Split(tableName, ".")

	if len(tableComponents) != 1 {
		return "", fmt.Errorf("table name must be in table format, '%s' given", tableName)
	}

	tableName = tableComponents[0]
	schemaName := db.config.Database // db.config.Database returns TABLE_SCHEMA

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		schemaName,
		tableName,
	)

	return strings.TrimSpace(query), nil
}
