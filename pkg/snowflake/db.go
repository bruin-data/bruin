package snowflake

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
)

const (
	invalidQueryError = "SQL compilation error"
)

type DB struct {
	conn          *sqlx.DB
	config        *Config
	schemaCreator *ansisql.SchemaCreator
	dsn           string
	mutex         sync.Mutex
	typeMapper    *diff.DatabaseTypeMapper
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
	}, nil
}

func (db *DB) initializeDB() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.conn != nil {
		return nil
	}

	conn, err := sqlx.Open("snowflake", db.dsn)
	if err != nil {
		return errors.Wrapf(err, "failed to open snowflake connection")
	}

	db.conn = conn
	return nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := db.Select(ctx, query)
	return err
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI()
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	if err := db.initializeDB(); err != nil {
		return nil, err
	}
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snowflake context")
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

func (db *DB) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	if err := db.initializeDB(); err != nil {
		return false, err
	}
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return false, errors.Wrap(err, "failed to create snowflake context")
	}

	rows, err := db.conn.QueryContext(ctx, query.ToExplainQuery())
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
	if err := db.initializeDB(); err != nil {
		return nil, err
	}
	// Prepare Snowflake context for the query execution
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snowflake context")
	}

	// Convert query object to string and execute it
	queryString := queryObj.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
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
		return nil
	}

	queryStr := fmt.Sprintf(
		`SELECT TABLE_TYPE FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`,
		db.config.Database, schemaName, tableName,
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
			Query: fmt.Sprintf("DROP %s IF EXISTS %s.%s", materializationType, schemaName, tableName),
		}

		if dropErr := db.RunQueryWithoutResult(ctx, &dropQuery); dropErr != nil {
			return errors.Wrapf(dropErr, "failed to drop existing %s: %s.%s", materializationType, schemaName, tableName)
		}
	}

	return nil
}

func (db *DB) PushColumnDescriptions(ctx context.Context, asset *pipeline.Asset) error {
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
		return nil
	}

	if asset.Description == "" && len(asset.Columns) == 0 {
		return errors.New("no metadata to push: table and columns have no descriptions")
	}

	queryStr := fmt.Sprintf(
		`SELECT COLUMN_NAME, COMMENT 
          FROM %s.INFORMATION_SCHEMA.COLUMNS 
          WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`,
		db.config.Database, schemaName, tableName)

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
				db.config.Database, schemaName, tableName, col.Name, escapeSQLString(col.Description),
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
			db.config.Database, schemaName, tableName, escapeSQLString(asset.Description),
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
			if val, ok := countResult[0][0].(int64); ok {
				rowCount = val
			} else if val, ok := countResult[0][0].(int); ok {
				rowCount = int64(val)
			} else {
				return nil, fmt.Errorf("unexpected row count type for table '%s'", tableName)
			}
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

		if val, ok := row[0].(int64); ok {
			stats.Count = val
		}
		if val, ok := row[1].(int64); ok {
			stats.NullCount = val
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

		if val, ok := row[0].(int64); ok {
			stats.Count = val
		}
		if val, ok := row[1].(int64); ok {
			stats.NullCount = val
		}
		if val, ok := row[2].(int64); ok {
			stats.DistinctCount = val
		}
		if val, ok := row[3].(int64); ok {
			stats.EmptyCount = val
		}
		if val, ok := row[4].(int64); ok {
			stats.MinLength = int(val)
		}
		if val, ok := row[5].(int64); ok {
			stats.MaxLength = int(val)
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

		if val, ok := row[0].(int64); ok {
			stats.Count = val
		}
		if val, ok := row[1].(int64); ok {
			stats.NullCount = val
		}
		if val, ok := row[2].(int64); ok {
			stats.TrueCount = val
		}
		if val, ok := row[3].(int64); ok {
			stats.FalseCount = val
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

		if val, ok := row[0].(int64); ok {
			stats.Count = val
		}
		if val, ok := row[1].(int64); ok {
			stats.NullCount = val
		}
		if val, ok := row[2].(int64); ok {
			stats.UniqueCount = val
		}
		if row[3] != nil {
			if val, ok := row[3].(string); ok {
				stats.EarliestDate = &val
			}
		}
		if row[4] != nil {
			if val, ok := row[4].(string); ok {
				stats.LatestDate = &val
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

		if val, ok := row[0].(int64); ok {
			stats.Count = val
		}
		if val, ok := row[1].(int64); ok {
			stats.NullCount = val
		}
	}

	return stats, nil
}

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// Get the current database name
	databaseName := db.config.Database

	// Query to get all schemas and tables in the database
	q := fmt.Sprintf(`
SELECT
    table_schema,
    table_name
FROM
    %s.INFORMATION_SCHEMA.TABLES
WHERE
    table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_schema, table_name;
`, databaseName)

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
		schemaKey := databaseName + "." + schemaName
		if _, exists := schemas[schemaKey]; !exists {
			schema := &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemas[schemaKey] = schema
		}

		// Add table to schema
		table := &ansisql.DBTable{
			Name: tableName,
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
