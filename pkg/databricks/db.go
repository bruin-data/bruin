package databricks

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	_ "github.com/databricks/databricks-sql-go"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DB struct {
	conn          *sqlx.DB
	config        *Config
	schemaCreator *ansisql.SchemaCreator
	mu            sync.Mutex
}

func NewDB(c *Config) (*DB, error) {
	return &DB{
		config:        c,
		schemaCreator: ansisql.NewSchemaCreator(),
	}, nil
}

func (db *DB) ensureConnection() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.conn != nil {
		return nil
	}

	conn, err := sqlx.Open("databricks", db.config.ToDBConnectionURI())
	if err != nil {
		return err
	}

	db.conn = conn
	return nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := db.Select(ctx, query)
	return err
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	if err := db.ensureConnection(); err != nil {
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

func (db *DB) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	if err := db.ensureConnection(); err != nil {
		return nil, err
	}

	queryString := queryObj.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err != nil {
		errorMessage := err.Error()
		return nil, errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
	}
	defer rows.Close()

	result := &query.QueryResult{
		Columns:     []string{},
		Rows:        [][]interface{}{},
		ColumnTypes: []string{},
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result.Columns = cols

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		result.ColumnTypes = make([]string, len(cols))
	} else {
		result.ColumnTypes = make([]string, len(columnTypes))
		for i, columnType := range columnTypes {
			typeName := columnType.DatabaseTypeName()
			if typeName == "" && columnType.ScanType() != nil {
				typeName = columnType.ScanType().String()
			}
			result.ColumnTypes[i] = typeName
		}
	}

	for rows.Next() {
		row := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range row {
			columnPointers[i] = &row[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (db *DB) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}
	err := db.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Databricks connection")
	}

	return nil
}

func (db *DB) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	return db.schemaCreator.CreateSchemaIfNotExist(ctx, db, asset)
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

func (db *DB) GetDatabases(ctx context.Context) ([]string, error) {
	q := `SHOW DATABASES`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Databricks databases: %w", err)
	}

	var databases []string
	for _, row := range result {
		if len(row) > 0 {
			if dbName, ok := row[0].(string); ok {
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

	q := `SHOW TABLES IN ` + databaseName

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query tables in database '%s': %w", databaseName, err)
	}

	var tables []string
	for _, row := range result {
		// SHOW TABLES returns multiple columns, table name is typically in the second column
		if len(row) > 1 {
			if tableName, ok := row[1].(string); ok {
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

	q := fmt.Sprintf(`DESCRIBE TABLE %s.%s`, databaseName, tableName)

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

		// Third column contains comment, but we don't use it

		// Skip partition information and metadata rows
		if strings.HasPrefix(columnName, "#") || columnName == "" {
			continue
		}

		// Databricks nullable information is not directly available in DESCRIBE
		// Most columns are nullable by default unless explicitly marked as NOT NULL
		nullable := true
		if strings.Contains(strings.ToLower(dataType), "not null") {
			nullable = false
		}

		column := &ansisql.DBColumn{
			Name:       columnName,
			Type:       dataType,
			Nullable:   nullable,
			PrimaryKey: false, // Primary key info not available in DESCRIBE
			Unique:     false,
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// Databricks uses a catalog.schema.table structure
	// We'll query INFORMATION_SCHEMA to get all schemas and tables
	q := `
SELECT
    schema_name,
    table_name
FROM
    information_schema.tables
WHERE
    table_type IN ('TABLE', 'VIEW')
    AND schema_name NOT IN ('information_schema')
ORDER BY schema_name, table_name;
`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Databricks information_schema: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    "databricks", // Databricks doesn't have a specific database name in this context
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
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in format schema.table, '%s' given", tableName)
		}
	}

	if len(tableComponents) != 2 {
		return "", fmt.Errorf("table name must be in format schema.table, '%s' given", tableName)
	}

	schemaName := tableComponents[0]
	targetTable := tableComponents[1]

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		schemaName,
		targetTable,
	)

	return strings.TrimSpace(query), nil
}
