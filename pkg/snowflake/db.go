package snowflake

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
)

const (
	invalidQueryError       = "SQL compilation error"
	snowflakeRetryAttempts  = 3
	snowflakeRetryBaseDelay = 500 * time.Millisecond
)

type DB struct {
	conn          *sqlx.DB
	config        *Config
	schemaCreator *ansisql.SchemaCreator
	dsn           string
	mutex         sync.Mutex
	typeMapper    *diff.DatabaseTypeMapper
	connect       func(ctx context.Context) (*sqlx.DB, error)
	retryDelay    func(attempt int) time.Duration
}

func NewDB(c *Config) (*DB, error) {
	dsn, err := c.DSN()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create DSN")
	}

	gosnowflake.GetLogger().SetOutput(io.Discard)

	return &DB{
		config:        c,
		schemaCreator: ansisql.NewSchemaCreator(),
		dsn:           dsn,
		mutex:         sync.Mutex{},
		typeMapper:    diff.NewSnowflakeTypeMapper(),
		connect: func(ctx context.Context) (*sqlx.DB, error) {
			return sqlx.ConnectContext(ctx, "snowflake", dsn)
		},
		retryDelay: defaultSnowflakeRetryDelay,
	}, nil
}

func (db *DB) initializeDB(ctx context.Context) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.conn != nil {
		return nil
	}

	conn, err := db.connectDB(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to snowflake")
	}

	db.conn = conn
	return nil
}

func (db *DB) connectDB(ctx context.Context) (*sqlx.DB, error) {
	if db.connect != nil {
		return db.connect(ctx)
	}

	return sqlx.ConnectContext(ctx, "snowflake", db.dsn)
}

func (db *DB) delayBeforeRetry(ctx context.Context, attempt int) error {
	delayFunc := db.retryDelay
	if delayFunc == nil {
		delayFunc = defaultSnowflakeRetryDelay
	}

	delay := delayFunc(attempt)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func defaultSnowflakeRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return snowflakeRetryBaseDelay
	}

	return time.Duration(math.Pow(2, float64(attempt-1))) * snowflakeRetryBaseDelay
}

func isRetriableSnowflakeError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}

	var netErr net.Error
	if stderrors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	message := strings.ToLower(err.Error())
	retriableMessages := []string{
		"client.timeout exceeded",
		"context deadline exceeded",
		"connection reset",
		"connection refused",
		"connection timed out",
		"eof",
		"i/o timeout",
		"no such host",
		"temporary failure",
		"tls handshake timeout",
		"timeout awaiting response headers",
		"unexpected eof",
	}

	for _, retriable := range retriableMessages {
		if strings.Contains(message, retriable) {
			return true
		}
	}

	return false
}

func (db *DB) withIdempotentRetry(ctx context.Context, fn func() error) error {
	for attempt := 1; attempt <= snowflakeRetryAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		if attempt == snowflakeRetryAttempts || !isRetriableSnowflakeError(ctx, err) {
			return err
		}

		if delayErr := db.delayBeforeRetry(ctx, attempt); delayErr != nil {
			return delayErr
		}
	}

	return nil
}

// logSnowflakeQueryID tries to read a query ID from the channel and prints it.
// It is non-blocking, so it is safe to call even if no ID was sent.
func logSnowflakeQueryID(ctx context.Context, ch <-chan string) {
	if ch == nil {
		return
	}

	select {
	case qid := <-ch:
		query.LogQueryID(ctx, "Snowflake", qid)
	default:
	}
}

func withSnowflakeRequestID(ctx context.Context, requestID *gosnowflake.UUID) context.Context {
	if requestID == nil {
		return ctx
	}

	return gosnowflake.WithRequestID(ctx, *requestID)
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := db.selectOnce(ctx, query, nil)
	return err
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI()
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	var result [][]interface{}
	requestID := gosnowflake.NewUUID()
	err := db.withIdempotentRetry(ctx, func() error {
		var err error
		result, err = db.selectOnce(ctx, query, &requestID)
		return err
	})
	return result, err
}

func (db *DB) selectOnce(ctx context.Context, query *query.Query, requestID *gosnowflake.UUID) ([][]interface{}, error) {
	if err := db.initializeDB(ctx); err != nil {
		return nil, err
	}

	// Attach a query ID channel and multi-statement context
	qidChan := make(chan string, 1)
	ctx = withSnowflakeRequestID(ctx, requestID)
	ctx = gosnowflake.WithQueryIDChan(ctx, qidChan)
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snowflake context")
	}

	queryString := query.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	// Try to print the query ID once the function returns
	defer logSnowflakeQueryID(ctx, qidChan)

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

func (db *DB) SelectOnlyLastResult(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	var result [][]interface{}
	requestID := gosnowflake.NewUUID()
	err := db.withIdempotentRetry(ctx, func() error {
		var err error
		result, err = db.selectOnlyLastResultOnce(ctx, query, &requestID)
		return err
	})
	return result, err
}

func (db *DB) selectOnlyLastResultOnce(ctx context.Context, query *query.Query, requestID *gosnowflake.UUID) ([][]interface{}, error) {
	if err := db.initializeDB(ctx); err != nil {
		return nil, err
	}

	// Attach a query ID channel and multi-statement context
	qidChan := make(chan string, 1)
	ctx = withSnowflakeRequestID(ctx, requestID)
	ctx = gosnowflake.WithQueryIDChan(ctx, qidChan)
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snowflake context")
	}

	queryString := query.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	// Try to print the query ID once the function returns
	defer logSnowflakeQueryID(ctx, qidChan)

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

	for {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		currentResult := [][]interface{}{}

		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			if err := rows.Scan(columnPointers...); err != nil {
				return nil, err
			}
			currentResult = append(currentResult, columns)
		}

		// Check for row errors after reading all rows in this result set
		if rows.Err() != nil {
			return nil, rows.Err()
		}

		// Overwrite result — so only the last result set remains
		result = currentResult

		if !rows.NextResultSet() {
			break
		}
	}

	return result, nil
}

func (db *DB) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	var valid bool
	requestID := gosnowflake.NewUUID()
	err := db.withIdempotentRetry(ctx, func() error {
		var err error
		valid, err = db.isValidOnce(ctx, query, &requestID)
		return err
	})
	return valid, err
}

func (db *DB) isValidOnce(ctx context.Context, query *query.Query, requestID *gosnowflake.UUID) (bool, error) {
	if err := db.initializeDB(ctx); err != nil {
		return false, err
	}

	// Attach a query ID channel and multi-statement context
	qidChan := make(chan string, 1)
	ctx = withSnowflakeRequestID(ctx, requestID)
	ctx = gosnowflake.WithQueryIDChan(ctx, qidChan)
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return false, errors.Wrap(err, "failed to create snowflake context")
	}

	rows, err := db.conn.QueryContext(ctx, query.ToExplainQuery())
	// Try to print the query ID once the function returns
	defer logSnowflakeQueryID(ctx, qidChan)

	if err == nil {
		err = rows.Err()
	}

	if err != nil {
		errorMessage := err.Error()
		if strings.Contains(errorMessage, invalidQueryError) {
			errorSegments := strings.Split(errorMessage, "\n")
			if len(errorSegments) > 1 {
				err = errors.New(errorSegments[1])
			}
		}
	}

	if rows != nil {
		defer rows.Close()
	}

	return err == nil, err
}

func (db *DB) DryRunQuery(ctx context.Context, q *query.Query) (*query.DryRunResult, error) {
	explainQuery := &query.Query{Query: q.ToExplainQuery()}
	explainResult, err := db.SelectWithSchema(ctx, explainQuery)
	if err != nil {
		return nil, fmt.Errorf("EXPLAIN failed: %w", err)
	}

	return &query.DryRunResult{
		ConnectionType: "snowflake",
		Valid:          true,
		ExplainRows:    explainResult,
	}, nil
}

// Test runs a simple query (SELECT 1) to validate the connection.
func (db *DB) Ping(ctx context.Context) error {
	// Define the test query
	q := query.Query{
		Query: "SELECT 1",
	}

	// Use the existing RunQueryWithoutResult method
	err := db.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Snowflake connection")
	}

	return nil // Return nil if the query runs successfully
}

func (db *DB) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	var result *query.QueryResult
	requestID := gosnowflake.NewUUID()
	err := db.withIdempotentRetry(ctx, func() error {
		var err error
		result, err = db.selectWithSchemaOnce(ctx, queryObj, &requestID)
		return err
	})
	return result, err
}

func (db *DB) selectWithSchemaOnce(ctx context.Context, queryObj *query.Query, requestID *gosnowflake.UUID) (*query.QueryResult, error) {
	if err := db.initializeDB(ctx); err != nil {
		return nil, err
	}
	// Prepare Snowflake context for the query execution
	// Attach a query ID channel and multi-statement context
	qidChan := make(chan string, 1)
	ctx = withSnowflakeRequestID(ctx, requestID)
	ctx = gosnowflake.WithQueryIDChan(ctx, qidChan)
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snowflake context")
	}

	// Convert query object to string and execute it
	queryString := queryObj.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	// Try to print the query ID once the function returns
	defer logSnowflakeQueryID(ctx, qidChan)

	if err != nil {
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
		return nil, err
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

func (db *DB) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	return db.schemaCreator.CreateSchemaIfNotExist(ctx, db, asset)
}

func (db *DB) RecreateTableOnMaterializationTypeMismatch(ctx context.Context, asset *pipeline.Asset) error {
	tableComponents := strings.Split(asset.Name, ".")
	var databaseName string
	var schemaName string
	var tableName string
	switch len(tableComponents) {
	case 2:
		databaseName = db.config.Database
		schemaName = strings.ToUpper(tableComponents[0])
		tableName = strings.ToUpper(tableComponents[1])
	case 3:
		databaseName = strings.ToUpper(tableComponents[0])
		schemaName = strings.ToUpper(tableComponents[1])
		tableName = strings.ToUpper(tableComponents[2])
	default:
		return nil
	}

	queryStr := fmt.Sprintf(
		`SELECT TABLE_TYPE FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`,
		databaseName, schemaName, tableName,
	)

	result, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return errors.Wrapf(err, "unable to retrieve table metadata for '%s.%s'", schemaName, tableName)
	}

	if len(result) == 0 {
		return nil
	}

	var materializationType string
	if typeField, ok := result[0][0].(string); ok {
		materializationType = typeField
	}

	if materializationType == "" {
		return errors.New("could not determine the materialization type")
	}
	var dbMaterializationType pipeline.MaterializationType
	switch materializationType {
	case "BASE TABLE":
		dbMaterializationType = pipeline.MaterializationTypeTable
		materializationType = "TABLE"
	case "VIEW":
		dbMaterializationType = pipeline.MaterializationTypeView
	default:
		dbMaterializationType = pipeline.MaterializationTypeNone
	}
	if dbMaterializationType != asset.Materialization.Type {
		dropQuery := query.Query{
			Query: fmt.Sprintf("DROP %s IF EXISTS %s.%s.%s", materializationType, databaseName, schemaName, tableName),
		}

		if dropErr := db.RunQueryWithoutResult(ctx, &dropQuery); dropErr != nil {
			return errors.Wrapf(dropErr, "failed to drop existing %s: %s.%s.%s", materializationType, databaseName, schemaName, tableName)
		}
	}

	return nil
}

func (db *DB) PushColumnDescriptions(ctx context.Context, asset *pipeline.Asset) error {
	tableComponents := strings.Split(asset.Name, ".")
	var databaseName string
	var schemaName string
	var tableName string
	switch len(tableComponents) {
	case 2:
		databaseName = db.config.Database
		schemaName = strings.ToUpper(tableComponents[0])
		tableName = strings.ToUpper(tableComponents[1])
	case 3:
		databaseName = strings.ToUpper(tableComponents[0])
		schemaName = strings.ToUpper(tableComponents[1])
		tableName = strings.ToUpper(tableComponents[2])
	default:
		return nil
	}

	if asset.Description == "" && len(asset.Columns) == 0 {
		return errors.New("no metadata to push: table and columns have no descriptions")
	}

	queryStr := fmt.Sprintf(
		`SELECT COLUMN_NAME, COMMENT
          FROM %s.INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`,
		databaseName, schemaName, tableName,
	)

	rows, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return errors.Wrapf(err, "failed to query column metadata for %s.%s", schemaName, tableName)
	}

	existingComments := make(map[string]string)
	for _, row := range rows {
		columnName := row[0].(string)
		comment := ""
		if row[1] != nil {
			comment = row[1].(string)
		}
		existingComments[columnName] = comment
	}

	// Find columns that need updates
	var updateQueries []string
	for _, col := range asset.Columns {
		if col.Description != "" && existingComments[col.Name] != col.Description {
			query := fmt.Sprintf(
				`ALTER TABLE %s.%s.%s MODIFY COLUMN %s COMMENT '%s'`,
				databaseName, schemaName, tableName, col.Name, escapeSQLString(col.Description),
			)
			updateQueries = append(updateQueries, query)
		}
	}
	if len(updateQueries) > 0 {
		batchQuery := strings.Join(updateQueries, "; ")
		if err := db.RunQueryWithoutResult(ctx, &query.Query{Query: batchQuery}); err != nil {
			return errors.Wrap(err, "failed to update column descriptions")
		}
	}

	if asset.Description != "" {
		updateTableQuery := fmt.Sprintf(
			`COMMENT ON TABLE %s.%s.%s IS '%s'`,
			databaseName, schemaName, tableName, escapeSQLString(asset.Description),
		)
		if err := db.RunQueryWithoutResult(ctx, &query.Query{Query: updateTableQuery}); err != nil {
			return errors.Wrap(err, "failed to update table description")
		}
	}

	return nil
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''") // Escape single quotes for SQL safety
}

func (db *DB) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	var rowCount int64

	// Get row count only if not in schema-only mode
	if !schemaOnly {
		countResult, err := db.Select(ctx, &query.Query{Query: "SELECT COUNT(*) as row_count FROM " + tableName})
		if err != nil {
			return nil, fmt.Errorf("failed to execute count query for table '%s': %w", tableName, err)
		}

		if len(countResult) > 0 && len(countResult[0]) > 0 {
			parsedRowCount, err := parseSnowflakeInt64(countResult[0][0], "row count")
			if err != nil {
				return nil, fmt.Errorf("failed to parse row count for table '%s': %w", tableName, err)
			}
			rowCount = parsedRowCount
		}
	}

	// Parse table name components for Snowflake
	tableComponents := strings.Split(tableName, ".")
	var databaseName, schemaName, tableNameOnly string

	switch len(tableComponents) {
	case 1:
		// Use current database and schema
		databaseName = db.config.Database
		schemaName = db.config.Schema
		tableNameOnly = strings.ToUpper(tableComponents[0])
	case 2:
		// schema.table format
		databaseName = db.config.Database
		schemaName = strings.ToUpper(tableComponents[0])
		tableNameOnly = strings.ToUpper(tableComponents[1])
	case 3:
		// database.schema.table format
		databaseName = strings.ToUpper(tableComponents[0])
		schemaName = strings.ToUpper(tableComponents[1])
		tableNameOnly = strings.ToUpper(tableComponents[2])
	default:
		return nil, fmt.Errorf("invalid table name format: %s", tableName)
	}

	// Get table schema using information_schema
	schemaQuery := fmt.Sprintf(`
	SELECT 
		column_name,
		data_type,
		is_nullable,
		column_default,
		character_maximum_length,
		numeric_precision,
		numeric_scale,
		is_identity
	FROM %s.information_schema.columns 
	WHERE table_schema = '%s' AND table_name = '%s'
	ORDER BY ordinal_position`, databaseName, schemaName, tableNameOnly)

	schemaResult, err := db.Select(ctx, &query.Query{Query: schemaQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to execute schema query for table '%s': %w", tableName, err)
	}

	columns := make([]*diff.Column, 0)
	for _, row := range schemaResult {
		if len(row) < 8 {
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

		// Optional fields
		var charMaxLength *int64
		if row[4] != nil {
			if val, ok := row[4].(int64); ok {
				charMaxLength = &val
			}
		}

		var numericPrecision, numericScale *int64
		if row[5] != nil {
			if val, ok := row[5].(int64); ok {
				numericPrecision = &val
			}
		}
		if row[6] != nil {
			if val, ok := row[6].(int64); ok {
				numericScale = &val
			}
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

		normalizedType := db.typeMapper.MapType(strings.ToLower(dataType))
		nullable := strings.ToUpper(isNullableStr) == "YES"

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
				stats, err = db.fetchNumericalStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeString:
				stats, err = db.fetchStringStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeBoolean:
				stats, err = db.fetchBooleanStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeDateTime:
				stats, err = db.fetchDateTimeStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeJSON:
				stats, err = db.fetchJSONStats(ctx, tableName, columnName)
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

	dbTable := &diff.Table{
		Name:    tableName,
		Columns: columns,
	}

	return &diff.TableSummaryResult{
		RowCount: rowCount,
		Table:    dbTable,
	}, nil
}

func parseSnowflakeInt64(value interface{}, fieldName string) (int64, error) {
	switch val := value.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse %s string %q: %w", fieldName, val, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected %s type: got %T with value %v", fieldName, val, val)
	}
}

func (db *DB) fetchNumericalStats(ctx context.Context, tableName, columnName string) (*diff.NumericalStatistics, error) {
	stats := &diff.NumericalStatistics{}
	queryStr := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            MIN(%s) as min_val,
            MAX(%s) as max_val,
            AVG(%s) as avg_val,
            SUM(%s) as sum_val,
            STDDEV(%s) as stddev_val
        FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return nil, err
	}

	if len(result) > 0 && len(result[0]) >= 7 { //nolint:nestif
		row := result[0]

		var err error
		stats.Count, err = parseSnowflakeInt64(row[0], "count")
		if err != nil {
			return nil, err
		}
		stats.NullCount, err = parseSnowflakeInt64(row[1], "null count")
		if err != nil {
			return nil, err
		}

		// Handle nullable numeric fields
		if row[2] != nil {
			if val, ok := row[2].(float64); ok {
				stats.Min = &val
			}
		}
		if row[3] != nil {
			if val, ok := row[3].(float64); ok {
				stats.Max = &val
			}
		}
		if row[4] != nil {
			if val, ok := row[4].(float64); ok {
				stats.Avg = &val
			}
		}
		if row[5] != nil {
			if val, ok := row[5].(float64); ok {
				stats.Sum = &val
			}
		}
		if row[6] != nil {
			if val, ok := row[6].(float64); ok {
				stats.StdDev = &val
			}
		}
	}

	return stats, nil
}

func (db *DB) fetchStringStats(ctx context.Context, tableName, columnName string) (*diff.StringStatistics, error) {
	stats := &diff.StringStatistics{}
	queryStr := fmt.Sprintf(`
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

	result, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return nil, err
	}

	if len(result) > 0 && len(result[0]) >= 7 {
		row := result[0]

		var err error
		stats.Count, err = parseSnowflakeInt64(row[0], "count")
		if err != nil {
			return nil, err
		}
		stats.NullCount, err = parseSnowflakeInt64(row[1], "null count")
		if err != nil {
			return nil, err
		}
		stats.DistinctCount, err = parseSnowflakeInt64(row[2], "distinct count")
		if err != nil {
			return nil, err
		}
		stats.EmptyCount, err = parseSnowflakeInt64(row[3], "empty count")
		if err != nil {
			return nil, err
		}
		if row[4] != nil {
			minLength, err := parseSnowflakeInt64(row[4], "min length")
			if err != nil {
				return nil, err
			}
			stats.MinLength = int(minLength)
		}
		if row[5] != nil {
			maxLength, err := parseSnowflakeInt64(row[5], "max length")
			if err != nil {
				return nil, err
			}
			stats.MaxLength = int(maxLength)
		}
		if val, ok := row[6].(float64); ok {
			stats.AvgLength = val
		}
	}

	return stats, nil
}

func (db *DB) fetchBooleanStats(ctx context.Context, tableName, columnName string) (*diff.BooleanStatistics, error) {
	stats := &diff.BooleanStatistics{}
	queryStr := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(CASE WHEN %s = true THEN 1 END) as true_count,
            COUNT(CASE WHEN %s = false THEN 1 END) as false_count
        FROM %s`,
		columnName, columnName, columnName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return nil, err
	}

	if len(result) > 0 && len(result[0]) >= 4 {
		row := result[0]

		var err error
		stats.Count, err = parseSnowflakeInt64(row[0], "count")
		if err != nil {
			return nil, err
		}
		stats.NullCount, err = parseSnowflakeInt64(row[1], "null count")
		if err != nil {
			return nil, err
		}
		stats.TrueCount, err = parseSnowflakeInt64(row[2], "true count")
		if err != nil {
			return nil, err
		}
		stats.FalseCount, err = parseSnowflakeInt64(row[3], "false count")
		if err != nil {
			return nil, err
		}
	}

	return stats, nil
}

func (db *DB) fetchDateTimeStats(ctx context.Context, tableName, columnName string) (*diff.DateTimeStatistics, error) {
	stats := &diff.DateTimeStatistics{}
	queryStr := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(DISTINCT %s) as unique_count,
            MIN(%s)::string as earliest_date,
            MAX(%s)::string as latest_date
        FROM %s`,
		columnName, columnName, columnName, columnName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return nil, err
	}

	if len(result) > 0 && len(result[0]) >= 5 {
		row := result[0]

		var err error
		stats.Count, err = parseSnowflakeInt64(row[0], "count")
		if err != nil {
			return nil, err
		}
		stats.NullCount, err = parseSnowflakeInt64(row[1], "null count")
		if err != nil {
			return nil, err
		}
		stats.UniqueCount, err = parseSnowflakeInt64(row[2], "unique count")
		if err != nil {
			return nil, err
		}
		// Handle datetime values - convert to proper time.Time objects
		if row[3] != nil {
			if parsedTime, err := diff.ParseDateTime(row[3]); err == nil {
				stats.EarliestDate = parsedTime
			}
		}

		if row[4] != nil {
			if parsedTime, err := diff.ParseDateTime(row[4]); err == nil {
				stats.LatestDate = parsedTime
			}
		}
	}

	return stats, nil
}

func (db *DB) fetchJSONStats(ctx context.Context, tableName, columnName string) (*diff.JSONStatistics, error) {
	stats := &diff.JSONStatistics{}
	queryStr := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count
        FROM %s`,
		columnName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return nil, err
	}

	if len(result) > 0 && len(result[0]) >= 2 {
		row := result[0]

		var err error
		stats.Count, err = parseSnowflakeInt64(row[0], "count")
		if err != nil {
			return nil, err
		}
		stats.NullCount, err = parseSnowflakeInt64(row[1], "null count")
		if err != nil {
			return nil, err
		}
	}

	return stats, nil
}

func (db *DB) GetDatabases(ctx context.Context) ([]string, error) {
	q := `SHOW DATABASES`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Snowflake databases: %w", err)
	}

	var databases []string
	for _, row := range result {
		if len(row) > 1 {
			if dbName, ok := row[1].(string); ok {
				databases = append(databases, dbName)
			}
		}
	}

	sort.Strings(databases)
	return databases, nil
}

func (db *DB) GetTables(ctx context.Context, databaseName string) ([]string, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	q := fmt.Sprintf(`
SELECT table_name
FROM %s.INFORMATION_SCHEMA.TABLES
WHERE table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_name;
`, databaseName)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query tables in database '%s': %w", databaseName, err)
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

func (db *DB) GetTablesWithSchemas(ctx context.Context, databaseName string) (map[string][]string, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	q := fmt.Sprintf(`
SELECT table_schema, table_name
FROM %s.INFORMATION_SCHEMA.TABLES
WHERE table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_schema, table_name;
`, databaseName)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query tables in database '%s': %w", databaseName, err)
	}

	tables := make(map[string][]string)
	for _, row := range result {
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

func (db *DB) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
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
		// Use current schema from config
		schemaName = db.config.Schema
		tableNameOnly = strings.ToUpper(tableComponents[0])
	case 2:
		// schema.table format
		schemaName = strings.ToUpper(tableComponents[0])
		tableNameOnly = strings.ToUpper(tableComponents[1])
	default:
		return nil, fmt.Errorf("invalid table name format: %s", tableName)
	}

	q := fmt.Sprintf(`
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM %s.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = '%s' AND table_name = '%s'
ORDER BY ordinal_position;
`, databaseName, schemaName, tableNameOnly)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table '%s.%s': %w", databaseName, tableName, err)
	}

	columns := make([]*ansisql.DBColumn, 0, len(result))
	for _, row := range result {
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
			if charMaxLength, ok := row[4].(int64); ok && charMaxLength > 0 {
				fullType = fmt.Sprintf("%s(%d)", dataType, charMaxLength)
			}
		} else if row[5] != nil && row[6] != nil {
			if numericPrecision, ok := row[5].(int64); ok {
				if numericScale, ok := row[6].(int64); ok && numericPrecision > 0 {
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

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// Get the current database name
	databaseName := db.config.Database

	// Query to get all schemas and tables in the database with view definitions and metadata
	q := fmt.Sprintf(`
SELECT
    t.table_schema,
    t.table_name,
    t.table_type,
    v.view_definition,
    t.created,
    t.last_altered,
    t.row_count,
    t.bytes,
    t.comment,
    t.table_owner
FROM
    %s.INFORMATION_SCHEMA.TABLES t
LEFT JOIN
    %s.INFORMATION_SCHEMA.VIEWS v ON t.table_schema = v.table_schema AND t.table_name = v.table_name
WHERE
    t.table_type IN ('BASE TABLE', 'VIEW') 
AND t.table_schema != 'INFORMATION_SCHEMA'
ORDER BY t.table_schema, t.table_name;
`, databaseName, databaseName)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Snowflake INFORMATION_SCHEMA: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    databaseName,
		Schemas: []*ansisql.DBSchema{},
	}
	schemas := make(map[string]*ansisql.DBSchema)

	for _, row := range result {
		if len(row) < 4 {
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
		var viewDefinition string
		if row[3] != nil {
			if vd, ok := row[3].(string); ok {
				viewDefinition = vd
			}
		}

		// Extract additional metadata
		var createdAt, lastModified *time.Time
		if len(row) > 4 && row[4] != nil {
			if t, ok := row[4].(time.Time); ok {
				createdAt = &t
			}
		}
		if len(row) > 5 && row[5] != nil {
			if t, ok := row[5].(time.Time); ok {
				lastModified = &t
			}
		}

		var rowCount *int64
		if len(row) > 6 && row[6] != nil {
			switch v := row[6].(type) {
			case int64:
				rowCount = &v
			case float64:
				rc := int64(v)
				rowCount = &rc
			}
		}

		var sizeBytes *int64
		if len(row) > 7 && row[7] != nil {
			switch v := row[7].(type) {
			case int64:
				sizeBytes = &v
			case float64:
				sb := int64(v)
				sizeBytes = &sb
			}
		}

		var tableComment string
		if len(row) > 8 && row[8] != nil {
			if c, ok := row[8].(string); ok {
				tableComment = c
			}
		}

		var owner string
		if len(row) > 9 && row[9] != nil {
			if o, ok := row[9].(string); ok {
				owner = o
			}
		}

		// Create schema if it doesn't exist
		schemaKey := databaseName + "." + schemaName
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
			CreatedAt:      createdAt,
			LastModified:   lastModified,
			RowCount:       rowCount,
			SizeBytes:      sizeBytes,
			Description:    tableComment,
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

func (db *DB) BuildTableExistsQuery(tableName string) (string, error) {
	tableComponents := strings.Split(tableName, ".")
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in schema.table or database.schema.table format, '%s' given", tableName)
		}
	}

	var databaseName string
	var schemaRef, targetTable string

	switch len(tableComponents) {
	case 2:
		// schema.table → use default database from config.
		if db.config.Database == "" {
			return "", errors.New("no database name provided")
		}
		databaseName = strings.ToUpper(db.config.Database)
		schemaRef = databaseName + ".INFORMATION_SCHEMA.TABLES"
		targetTable = tableComponents[1]
	case 3:
		// database.schema.table
		databaseName = strings.ToUpper(tableComponents[0])
		schemaRef = databaseName + ".INFORMATION_SCHEMA.TABLES"
		targetTable = tableComponents[2]
	default:
		return "", fmt.Errorf("table name must be in schema.table or database.schema.table format, '%s' given", tableName)
	}

	// Snowflake stores unquoted identifiers in uppercase.
	schemaName := strings.ToUpper(tableComponents[len(tableComponents)-2])
	targetTable = strings.ToUpper(targetTable)

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
		schemaRef,
		schemaName,
		targetTable,
	)

	return strings.TrimSpace(query), nil
}
