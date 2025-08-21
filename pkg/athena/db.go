package athena

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	drv "github.com/uber/athenadriver/go"
)

type DB struct {
	conn   *sqlx.DB
	config *Config
	mutex  sync.Mutex
}

func NewDB(c *Config) *DB {
	return &DB{
		config: c,
		mutex:  sync.Mutex{},
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

	tableName = strings.ToLower(tableComponents[0])
	schemaName := db.config.Database

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		schemaName,
		tableName,
	)

	return strings.TrimSpace(query), nil
}
