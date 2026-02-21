package vertica

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	_ "github.com/vertica/vertica-sql-go"
	"github.com/pkg/errors"
)

type DB struct {
	conn   *sqlx.DB
	config *Config
}

// QuoteIdentifier quotes a Vertica identifier (table, column, etc.) with double quotes.
// It splits the identifier on "." and quotes each part separately.
func QuoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quotedParts := make([]string, len(parts))
	for i, part := range parts {
		escapedPart := strings.ReplaceAll(part, `"`, `""`)
		quotedParts[i] = fmt.Sprintf(`"%s"`, escapedPart)
	}
	return strings.Join(quotedParts, ".")
}

func NewDB(c *Config) (*DB, error) {
	conn, err := sqlx.Open("vertica", c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &DB{conn: conn, config: c}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, q *query.Query) error {
	_, err := db.Select(ctx, q)
	return err
}

func (db *DB) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	queryString := q.String()
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

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column names")
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column types")
	}

	columns := make([]string, len(cols))
	columnTypeNames := make([]string, len(columnTypes))
	copy(columns, cols)
	for i, colType := range columnTypes {
		columnTypeNames[i] = colType.DatabaseTypeName()
	}

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
		return errors.Wrap(err, "failed to run test query on Vertica connection")
	}

	return nil
}

func (db *DB) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	tableParts := strings.Split(asset.Name, ".")
	if len(tableParts) < 2 {
		return nil
	}

	schemaName := tableParts[0]
	q := &query.Query{
		Query: fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", QuoteIdentifier(schemaName)),
	}

	return db.RunQueryWithoutResult(ctx, q)
}

func (db *DB) GetDatabases(ctx context.Context) ([]string, error) {
	q := `SELECT database_name FROM v_catalog.databases ORDER BY database_name`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Vertica databases: %w", err)
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

func (db *DB) GetTablesWithSchemas(ctx context.Context, databaseName string) (map[string][]string, error) {
	q := `
SELECT table_schema, table_name
FROM v_catalog.tables
WHERE is_system_table = false
ORDER BY table_schema, table_name`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}

	tables := make(map[string][]string)
	for _, row := range result {
		if len(row) >= 2 {
			schema, _ := row[0].(string)
			table, _ := row[1].(string)
			tables[schema] = append(tables[schema], table)
		}
	}

	return tables, nil
}

func (db *DB) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	tableComponents := strings.Split(tableName, ".")
	var schemaName, tableNameOnly string

	switch len(tableComponents) {
	case 1:
		schemaName = "public"
		tableNameOnly = tableComponents[0]
	case 2:
		schemaName = tableComponents[0]
		tableNameOnly = tableComponents[1]
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
FROM v_catalog.columns
WHERE table_schema = '%s' AND table_name = '%s'
ORDER BY ordinal_position`,
		schemaName, tableNameOnly)

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table '%s': %w", tableName, err)
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

		isNullableStr, _ := row[2].(string)

		column := &ansisql.DBColumn{
			Name:       columnName,
			Type:       dataType,
			Nullable:   strings.ToUpper(isNullableStr) == "YES" || strings.ToUpper(isNullableStr) == "TRUE",
			PrimaryKey: false,
			Unique:     false,
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	currentDB := db.config.Database
	if currentDB == "" {
		return nil, errors.New("database name not configured")
	}

	q := `
SELECT
    t.table_schema,
    t.table_name,
    'TABLE' as table_type
FROM v_catalog.tables t
WHERE t.is_system_table = false
UNION ALL
SELECT
    v.table_schema,
    v.table_name,
    'VIEW' as table_type
FROM v_catalog.views v
WHERE v.is_system_view = false
ORDER BY 1, 2`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Vertica catalog: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    currentDB,
		Schemas: []*ansisql.DBSchema{},
	}
	schemas := make(map[string]*ansisql.DBSchema)

	for _, row := range result {
		if len(row) < 3 {
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

		if _, exists := schemas[schemaName]; !exists {
			schema := &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemas[schemaName] = schema
		}

		var dbTableType ansisql.DBTableType
		if strings.ToUpper(tableType) == "VIEW" {
			dbTableType = ansisql.DBTableTypeView
		} else {
			dbTableType = ansisql.DBTableTypeTable
		}

		table := &ansisql.DBTable{
			Name:    tableName,
			Type:    dbTableType,
			Columns: []*ansisql.DBColumn{},
		}
		schemas[schemaName].Tables = append(schemas[schemaName].Tables, table)
	}

	for _, schema := range schemas {
		summary.Schemas = append(summary.Schemas, schema)
	}

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
		schemaName = "public"
		targetTable = tableComponents[0]
	case 2:
		schemaName = tableComponents[0]
		targetTable = tableComponents[1]
	default:
		return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
	}

	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM v_catalog.tables WHERE table_schema = '%s' AND table_name = '%s'",
		schemaName,
		targetTable,
	)

	return strings.TrimSpace(q), nil
}
