package athena

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	drv "github.com/uber/athenadriver/go"
)

type DB struct {
	conn   *sqlx.DB
	config *Config
	mutex  sync.Mutex
	// typeMapper normalizes Athena column types for data-diff support.
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
	err := db.initializeDB()
	if err != nil {
		return err
	}
	_, err = db.Select(ctx, query)
	return err
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	err := db.initializeDB()
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
	err := db.initializeDB()
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

func (db *DB) initializeDB() error {
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

	conn, err := sqlx.Open(drv.DriverName, athenaURI)
	if err != nil {
		return errors.Errorf("Failed to open database connection: %v", err)
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

func (db *DB) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	if err := db.initializeDB(); err != nil {
		return nil, err
	}

	if db.config == nil {
		return nil, errors.New("athena config is not initialized")
	}

	schemaName, tableNameOnly, err := db.parseTableName(tableName)
	if err != nil {
		return nil, err
	}

	fullTableIdentifier := buildFullyQualifiedTableName(schemaName, tableNameOnly)

	var rowCount int64
	if !schemaOnly {
		rowCount, err = db.fetchRowCount(ctx, fullTableIdentifier)
		if err != nil {
			return nil, err
		}
	}

	columns, err := db.fetchColumns(ctx, schemaName, tableNameOnly, fullTableIdentifier, schemaOnly)
	if err != nil {
		return nil, err
	}

	table := &diff.Table{
		Name:    tableName,
		Columns: columns,
	}

	return &diff.TableSummaryResult{
		RowCount: rowCount,
		Table:    table,
	}, nil
}

func (db *DB) fetchRowCount(ctx context.Context, fullTableIdentifier string) (int64, error) {
	queryString := fmt.Sprintf(`SELECT COUNT(*) AS row_count FROM %s`, fullTableIdentifier)
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err != nil {
		return 0, fmt.Errorf("failed to execute row count query for '%s': %w", fullTableIdentifier, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, errors.New("row count query returned no rows")
	}

	var countValue interface{}
	if err := rows.Scan(&countValue); err != nil {
		return 0, fmt.Errorf("failed to scan row count for '%s': %w", fullTableIdentifier, err)
	}

	count, err := asInt64(countValue)
	if err != nil {
		return 0, fmt.Errorf("failed to parse row count for '%s': %w", fullTableIdentifier, err)
	}

	return count, rows.Err()
}

func (db *DB) fetchColumns(ctx context.Context, schemaName, tableName, fullTableIdentifier string, schemaOnly bool) ([]*diff.Column, error) {
	schemaQuery := strings.TrimSpace(fmt.Sprintf(`
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = '%s' AND table_name = '%s'
ORDER BY ordinal_position;
`, schemaName, tableName))

	result, err := db.Select(ctx, &query.Query{Query: schemaQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch column metadata for '%s.%s': %w", schemaName, tableName, err)
	}

	columns := make([]*diff.Column, 0, len(result))
	for _, row := range result {
		if len(row) < 3 {
			continue
		}

		columnName := toString(row[0])
		if columnName == "" {
			continue
		}

		dataType := toString(row[1])
		if dataType == "" {
			continue
		}

		isNullable := strings.EqualFold(toString(row[2]), "YES")

		normalizedType := db.getTypeMapper().MapType(dataType)

		var stats diff.ColumnStatistics
		if !schemaOnly {
			stats, err = db.fetchColumnStatistics(ctx, normalizedType, fullTableIdentifier, columnName)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch statistics for column '%s': %w", columnName, err)
			}
		}

		columns = append(columns, &diff.Column{
			Name:           columnName,
			Type:           dataType,
			NormalizedType: normalizedType,
			Nullable:       isNullable,
			PrimaryKey:     false,
			Unique:         false,
			Stats:          stats,
		})
	}

	return columns, nil
}

func (db *DB) fetchColumnStatistics(ctx context.Context, normalizedType diff.CommonDataType, fullTableIdentifier, columnName string) (diff.ColumnStatistics, error) {
	switch normalizedType {
	case diff.CommonTypeNumeric:
		return db.fetchNumericalStats(ctx, fullTableIdentifier, columnName)
	case diff.CommonTypeString:
		return db.fetchStringStats(ctx, fullTableIdentifier, columnName)
	case diff.CommonTypeBoolean:
		return db.fetchBooleanStats(ctx, fullTableIdentifier, columnName)
	case diff.CommonTypeDateTime:
		return db.fetchDateTimeStats(ctx, fullTableIdentifier, columnName)
	case diff.CommonTypeJSON:
		return db.fetchJSONStats(ctx, fullTableIdentifier, columnName)
	default:
		return &diff.UnknownStatistics{}, nil
	}
}

func (db *DB) fetchNumericalStats(ctx context.Context, fullTableIdentifier, columnName string) (*diff.NumericalStatistics, error) {
	columnExpr := quoteIdentifier(columnName)
	queryString := strings.TrimSpace(fmt.Sprintf(`
SELECT
    COUNT(*) AS count,
    COUNT(*) - COUNT(%[1]s) AS null_count,
    MIN(%[1]s) AS min_val,
    MAX(%[1]s) AS max_val,
    AVG(CAST(%[1]s AS DOUBLE)) AS avg_val,
    SUM(CAST(%[1]s AS DOUBLE)) AS sum_val,
    STDDEV_POP(CAST(%[1]s AS DOUBLE)) AS stddev_val
FROM %[2]s
`, columnExpr, fullTableIdentifier))

	row := db.conn.QueryRowContext(ctx, queryString)

	var (
		countVal, nullCountVal                    interface{}
		minVal, maxVal, avgVal, sumVal, stddevVal interface{}
	)

	if err := row.Scan(&countVal, &nullCountVal, &minVal, &maxVal, &avgVal, &sumVal, &stddevVal); err != nil {
		return nil, fmt.Errorf("failed to scan numerical stats for column '%s': %w", columnName, err)
	}

	count, err := asInt64(countVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}

	nullCount, err := asInt64(nullCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	return &diff.NumericalStatistics{
		Count:     count,
		NullCount: nullCount,
		Min:       asFloatPointer(minVal),
		Max:       asFloatPointer(maxVal),
		Avg:       asFloatPointer(avgVal),
		Sum:       asFloatPointer(sumVal),
		StdDev:    asFloatPointer(stddevVal),
	}, nil
}

func (db *DB) fetchStringStats(ctx context.Context, fullTableIdentifier, columnName string) (*diff.StringStatistics, error) {
	columnExpr := quoteIdentifier(columnName)
	queryString := strings.TrimSpace(fmt.Sprintf(`
SELECT
    COUNT(*) AS count,
    COUNT(*) - COUNT(%[1]s) AS null_count,
    APPROX_DISTINCT(%[1]s) AS distinct_count,
    COUNT_IF(%[1]s = '') AS empty_count,
    MIN(LENGTH(%[1]s)) AS min_length,
    MAX(LENGTH(%[1]s)) AS max_length,
    AVG(CAST(LENGTH(%[1]s) AS DOUBLE)) AS avg_length
FROM %[2]s
`, columnExpr, fullTableIdentifier))

	row := db.conn.QueryRowContext(ctx, queryString)

	var (
		countVal, nullCountVal, distinctCountVal, emptyCountVal interface{}
		minLengthVal, maxLengthVal                              interface{}
		avgLengthVal                                            interface{}
	)

	if err := row.Scan(&countVal, &nullCountVal, &distinctCountVal, &emptyCountVal, &minLengthVal, &maxLengthVal, &avgLengthVal); err != nil {
		return nil, fmt.Errorf("failed to scan string stats for column '%s': %w", columnName, err)
	}

	count, err := asInt64(countVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}

	nullCount, err := asInt64(nullCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	distinctCount, err := asInt64(distinctCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse distinct count for column '%s': %w", columnName, err)
	}

	emptyCount, err := asInt64(emptyCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse empty count for column '%s': %w", columnName, err)
	}

	minLength, err := asInt(minLengthVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse min length for column '%s': %w", columnName, err)
	}

	maxLength, err := asInt(maxLengthVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max length for column '%s': %w", columnName, err)
	}

	avgLength := asFloatPointer(avgLengthVal)
	avgLengthValue := 0.0
	if avgLength != nil {
		avgLengthValue = *avgLength
	}

	return &diff.StringStatistics{
		Count:         count,
		NullCount:     nullCount,
		DistinctCount: distinctCount,
		EmptyCount:    emptyCount,
		MinLength:     minLength,
		MaxLength:     maxLength,
		AvgLength:     avgLengthValue,
	}, nil
}

func (db *DB) fetchBooleanStats(ctx context.Context, fullTableIdentifier, columnName string) (*diff.BooleanStatistics, error) {
	columnExpr := quoteIdentifier(columnName)
	queryString := strings.TrimSpace(fmt.Sprintf(`
SELECT
    COUNT(*) AS count,
    COUNT(*) - COUNT(%[1]s) AS null_count,
    COUNT_IF(%[1]s = TRUE) AS true_count,
    COUNT_IF(%[1]s = FALSE) AS false_count
FROM %[2]s
`, columnExpr, fullTableIdentifier))

	row := db.conn.QueryRowContext(ctx, queryString)

	var (
		countVal, nullCountVal, trueCountVal, falseCountVal interface{}
	)

	if err := row.Scan(&countVal, &nullCountVal, &trueCountVal, &falseCountVal); err != nil {
		return nil, fmt.Errorf("failed to scan boolean stats for column '%s': %w", columnName, err)
	}

	count, err := asInt64(countVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}

	nullCount, err := asInt64(nullCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	trueCount, err := asInt64(trueCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse true count for column '%s': %w", columnName, err)
	}

	falseCount, err := asInt64(falseCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse false count for column '%s': %w", columnName, err)
	}

	return &diff.BooleanStatistics{
		Count:      count,
		NullCount:  nullCount,
		TrueCount:  trueCount,
		FalseCount: falseCount,
	}, nil
}

func (db *DB) fetchDateTimeStats(ctx context.Context, fullTableIdentifier, columnName string) (*diff.DateTimeStatistics, error) {
	columnExpr := quoteIdentifier(columnName)
	queryString := strings.TrimSpace(fmt.Sprintf(`
SELECT
    COUNT(*) AS count,
    COUNT(*) - COUNT(%[1]s) AS null_count,
    APPROX_DISTINCT(%[1]s) AS unique_count,
    MIN(%[1]s) AS earliest_date,
    MAX(%[1]s) AS latest_date
FROM %[2]s
`, columnExpr, fullTableIdentifier))

	row := db.conn.QueryRowContext(ctx, queryString)

	var (
		countVal, nullCountVal, uniqueCountVal interface{}
		earliestDateVal, latestDateVal         interface{}
	)

	if err := row.Scan(&countVal, &nullCountVal, &uniqueCountVal, &earliestDateVal, &latestDateVal); err != nil {
		return nil, fmt.Errorf("failed to scan datetime stats for column '%s': %w", columnName, err)
	}

	count, err := asInt64(countVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}

	nullCount, err := asInt64(nullCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	uniqueCount, err := asInt64(uniqueCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse unique count for column '%s': %w", columnName, err)
	}

	earliest, err := asTimePointer(earliestDateVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse earliest date for column '%s': %w", columnName, err)
	}

	latest, err := asTimePointer(latestDateVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse latest date for column '%s': %w", columnName, err)
	}

	return &diff.DateTimeStatistics{
		Count:        count,
		NullCount:    nullCount,
		UniqueCount:  uniqueCount,
		EarliestDate: earliest,
		LatestDate:   latest,
	}, nil
}

func (db *DB) fetchJSONStats(ctx context.Context, fullTableIdentifier, columnName string) (*diff.JSONStatistics, error) {
	columnExpr := quoteIdentifier(columnName)
	queryString := strings.TrimSpace(fmt.Sprintf(`
SELECT
    COUNT(*) AS count,
    COUNT(*) - COUNT(%[1]s) AS null_count
FROM %[2]s
`, columnExpr, fullTableIdentifier))

	row := db.conn.QueryRowContext(ctx, queryString)

	var (
		countVal, nullCountVal interface{}
	)

	if err := row.Scan(&countVal, &nullCountVal); err != nil {
		return nil, fmt.Errorf("failed to scan JSON stats for column '%s': %w", columnName, err)
	}

	count, err := asInt64(countVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse count value for column '%s': %w", columnName, err)
	}

	nullCount, err := asInt64(nullCountVal)
	if err != nil {
		return nil, fmt.Errorf("failed to parse null count for column '%s': %w", columnName, err)
	}

	return &diff.JSONStatistics{
		Count:     count,
		NullCount: nullCount,
	}, nil
}

func (db *DB) parseTableName(tableName string) (string, string, error) {
	parts := strings.Split(tableName, ".")
	switch len(parts) {
	case 1:
		if db.config == nil || db.config.Database == "" {
			return "", "", errors.New("athena database (schema) must be specified")
		}
		return normalizeIdentifier(db.config.Database), normalizeIdentifier(strings.TrimSpace(parts[0])), nil
	case 2:
		return normalizeIdentifier(strings.TrimSpace(parts[0])), normalizeIdentifier(strings.TrimSpace(parts[1])), nil
	default:
		return "", "", fmt.Errorf("invalid table name format '%s', expected schema.table or table", tableName)
	}
}

func (db *DB) getTypeMapper() *diff.DatabaseTypeMapper {
	if db.typeMapper == nil {
		db.typeMapper = diff.NewAthenaTypeMapper()
	}
	return db.typeMapper
}

func buildFullyQualifiedTableName(schema, table string) string {
	if schema == "" {
		return quoteIdentifier(table)
	}
	return fmt.Sprintf("%s.%s", quoteIdentifier(schema), quoteIdentifier(table))
}

func quoteIdentifier(identifier string) string {
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

func normalizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimSuffix(value, `"`)
	return value
}

func toString(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func asInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		if v == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case []byte:
		if len(v) == 0 {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported integer type %T", value)
	}
}

func asInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case int32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case uint32:
		return int(v), nil
	case float64:
		return int(v), nil
	case float32:
		return int(v), nil
	case string:
		if v == "" {
			return 0, nil
		}
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case []byte:
		if len(v) == 0 {
			return 0, nil
		}
		parsed, err := strconv.Atoi(string(v))
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported integer type %T", value)
	}
}

func asFloatPointer(value interface{}) *float64 {
	switch v := value.(type) {
	case nil:
		return nil
	case float64:
		return &v
	case float32:
		f := float64(v)
		return &f
	case int64:
		f := float64(v)
		return &f
	case int32:
		f := float64(v)
		return &f
	case int:
		f := float64(v)
		return &f
	case uint64:
		f := float64(v)
		return &f
	case uint32:
		f := float64(v)
		return &f
	case string:
		if v == "" {
			return nil
		}
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil
		}
		return &parsed
	case []byte:
		if len(v) == 0 {
			return nil
		}
		parsed, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return nil
		}
		return &parsed
	default:
		parsed, err := strconv.ParseFloat(fmt.Sprint(v), 64)
		if err != nil {
			return nil
		}
		return &parsed
	}
}

func asTimePointer(value interface{}) (*time.Time, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case time.Time:
		return &v, nil
	case *time.Time:
		return v, nil
	case string:
		if v == "" {
			return nil, nil
		}
		return diff.ParseDateTime(v)
	case []byte:
		if len(v) == 0 {
			return nil, nil
		}
		return diff.ParseDateTime(string(v))
	default:
		return diff.ParseDateTime(fmt.Sprint(v))
	}
}

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// Athena uses AWS Glue Data Catalog
	// We'll query INFORMATION_SCHEMA to get all schemas and tables
	q := `
SELECT
    table_schema,
    table_name
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
		if len(row) != 2 {
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

	for _, schema := range schemas {
		summary.Schemas = append(summary.Schemas, schema)
	}

	// Sort schemas by name
	sort.Slice(summary.Schemas, func(i, j int) bool {
		return summary.Schemas[i].Name < summary.Schemas[j].Name
	})

	return summary, nil
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
