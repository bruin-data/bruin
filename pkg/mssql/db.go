package mssql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
)

type DB struct {
	conn   *sqlx.DB
	config *Config
}

type Limiter interface {
	Limit(query string, limit int64) string
}

// QuoteIdentifier quotes a SQL Server identifier (table, column, etc.) to handle special characters.
// It splits the identifier on "." and quotes each part separately with square brackets.
// For example, "schema.my-table" becomes "[schema].[my-table]".
func QuoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quotedParts := make([]string, len(parts))
	for i, part := range parts {
		// Escape any existing right brackets by doubling them
		escapedPart := strings.ReplaceAll(part, "]", "]]")
		quotedParts[i] = fmt.Sprintf("[%s]", escapedPart)
	}
	return strings.Join(quotedParts, ".")
}

func NewDB(c *Config) (*DB, error) {
	conn, err := sqlx.Open("mssql", c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &DB{conn: conn, config: c}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := db.Select(ctx, query)
	return err
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
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

func (db *DB) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	queryString := queryObj.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err != nil {
		errorMessage := err.Error()
		return nil, errors.Wrap(errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  ")), "failed to execute query")
	}
	defer rows.Close()

	// Get column information
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column names")
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column types")
	}

	// Extract column names and types
	columns := make([]string, len(cols))
	columnTypeNames := make([]string, len(columnTypes))
	copy(columns, cols)
	for i, colType := range columnTypes {
		columnTypeNames[i] = colType.DatabaseTypeName()
	}

	// Collect rows
	var result [][]interface{}
	for rows.Next() {
		rowColumns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range rowColumns {
			columnPointers[i] = &rowColumns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row values")
		}

		result = append(result, rowColumns)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating rows")
	}

	queryResult := &query.QueryResult{
		Columns:     columns,
		Rows:        result,
		ColumnTypes: columnTypeNames,
	}
	return queryResult, nil
}

func (db *DB) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}
	err := db.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on SQL Server connection")
	}

	return nil
}

func (db *DB) Limit(query string, limit int64) string {
	query = strings.TrimRight(query, "; \n\t")
	return fmt.Sprintf("SELECT TOP %d * FROM (\n%s\n) as t", limit, query)
}

func (db *DB) GetDatabases(ctx context.Context) ([]string, error) {
	q := `
SELECT name
FROM sys.databases
WHERE database_id > 4  -- Exclude system databases (master, model, msdb, tempdb)
ORDER BY name;
`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query SQL Server databases: %w", err)
	}

	var databases []string
	for _, row := range result {
		if len(row) > 0 {
			if dbName, ok := row[0].(string); ok {
				databases = append(databases, dbName)
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
USE [%s];
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY TABLE_NAME;
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
		// table only - use dbo schema by default
		schemaName = "dbo"
		tableNameOnly = tableComponents[0]
	case 2:
		// schema.table format
		schemaName = tableComponents[0]
		tableNameOnly = tableComponents[1]
	default:
		return nil, fmt.Errorf("invalid table name format: %s", tableName)
	}

	q := fmt.Sprintf(`
USE [%s];
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`, databaseName)

	// Use parameterized query to avoid SQL injection and handle special characters
	rows, err := db.conn.QueryContext(ctx, q, schemaName, tableNameOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table '%s.%s': %w", databaseName, tableName, err)
	}
	defer rows.Close()

	var result [][]interface{}
	for rows.Next() {
		var columnName, dataType, isNullable string
		var columnDefault interface{}
		var charMaxLength, numericPrecision, numericScale interface{}

		err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &charMaxLength, &numericPrecision, &numericScale)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column row: %w", err)
		}

		result = append(result, []interface{}{columnName, dataType, isNullable, columnDefault, charMaxLength, numericPrecision, numericScale})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column rows: %w", err)
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

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// Get the current database name from config
	currentDB := db.config.Database
	if currentDB == "" {
		return nil, errors.New("database name not configured")
	}

	q := fmt.Sprintf(`
USE [%s];
SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    v.VIEW_DEFINITION,
    o.create_date,
    o.modify_date,
    CAST(p.rows AS BIGINT) as row_count,
    CAST(SUM(a.total_pages) * 8 * 1024 AS BIGINT) as size_bytes,
    CAST(ep.value AS NVARCHAR(MAX)) as table_comment
FROM
    INFORMATION_SCHEMA.TABLES t
LEFT JOIN
    INFORMATION_SCHEMA.VIEWS v ON t.TABLE_SCHEMA = v.TABLE_SCHEMA AND t.TABLE_NAME = v.TABLE_NAME
LEFT JOIN
    sys.objects o ON o.name = t.TABLE_NAME AND SCHEMA_NAME(o.schema_id) = t.TABLE_SCHEMA
LEFT JOIN
    sys.partitions p ON o.object_id = p.object_id AND p.index_id IN (0, 1)
LEFT JOIN
    sys.allocation_units a ON p.partition_id = a.container_id
LEFT JOIN
    sys.extended_properties ep ON o.object_id = ep.major_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
WHERE
    t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND t.TABLE_SCHEMA NOT IN ('sys', 'information_schema')
GROUP BY
    t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE, v.VIEW_DEFINITION, o.create_date, o.modify_date, p.rows, ep.value
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
`, currentDB)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query SQL Server information_schema: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    currentDB,
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
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
		}
	}

	var schemaName, targetTable string

	switch len(tableComponents) {
	case 1:
		schemaName = "dbo"
		targetTable = tableComponents[0]
	case 2:
		schemaName = tableComponents[0]
		targetTable = tableComponents[1]
	default:
		return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
	}

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		schemaName,
		targetTable,
	)

	return strings.TrimSpace(query), nil
}
