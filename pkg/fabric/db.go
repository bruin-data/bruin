package fabric

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/tablename"
	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
)

type DB struct {
	conn   *sqlx.DB
	config *Config
}

// QuoteIdentifier quotes a Fabric identifier using square brackets.
// Fabric is case-sensitive, so proper quoting is important.
func QuoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		escaped := strings.ReplaceAll(part, "]", "]]")
		quoted[i] = fmt.Sprintf("[%s]", escaped)
	}
	return strings.Join(quoted, ".")
}

func NewDB(c *Config) (*DB, error) {
	conn, err := sqlx.Open(c.DriverName(), c.ToDBConnectionURI())
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Fabric Warehouse connection")
	}

	return &DB{conn: conn, config: c}, nil
}

func (db *DB) Ping(ctx context.Context) error {
	return db.conn.PingContext(ctx)
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
	copy(columns, cols)

	typeNames := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeNames[i] = ct.DatabaseTypeName()
	}

	var resultRows [][]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		resultRows = append(resultRows, columns)
	}

	return &query.QueryResult{
		Columns:     columns,
		Rows:        resultRows,
		ColumnTypes: typeNames,
	}, rows.Err()
}

func (db *DB) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	cb, ok := tablename.For("fabric")
	if !ok {
		return nil, errors.New("fabric table-name capability not found")
	}
	tn, err := cb.Parse(tableName, tablename.Defaults{Catalog: databaseName, Schema: "dbo"})
	if err != nil {
		return nil, err
	}

	return db.getColumns(ctx, tn.Catalog, tn.Schema, tn.Table, tn.String("."))
}

func (db *DB) GetColumnsForTable(ctx context.Context, schemaName, tableName string) ([]*ansisql.DBColumn, error) {
	if schemaName == "" {
		schemaName = "dbo"
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	return db.getColumns(ctx, "", schemaName, tableName, schemaName+"."+tableName)
}

func (db *DB) getColumns(ctx context.Context, databaseName, schemaName, tableName, displayName string) ([]*ansisql.DBColumn, error) {
	infoSchema := "INFORMATION_SCHEMA"
	if databaseName != "" {
		infoSchema = QuoteIdentifier(databaseName) + ".INFORMATION_SCHEMA"
	}

	q := fmt.Sprintf(`
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM %s.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`, infoSchema)

	rows, err := db.conn.QueryContext(ctx, q, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table '%s': %w", displayName, err)
	}
	defer rows.Close()

	columns := make([]*ansisql.DBColumn, 0)
	for rows.Next() {
		var columnName, dataType, isNullable string
		var charMaxLength, numericPrecision, numericScale interface{}

		err := rows.Scan(&columnName, &dataType, &isNullable, &charMaxLength, &numericPrecision, &numericScale)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column row: %w", err)
		}

		columns = append(columns, &ansisql.DBColumn{
			Name:       columnName,
			Type:       formatColumnType(dataType, charMaxLength, numericPrecision, numericScale),
			Nullable:   strings.EqualFold(isNullable, "YES"),
			PrimaryKey: false,
			Unique:     false,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column rows: %w", err)
	}

	return columns, nil
}

func formatColumnType(dataType string, charMaxLength, numericPrecision, numericScale interface{}) string {
	switch strings.ToLower(dataType) {
	case "char", "varchar", "nchar", "nvarchar", "binary", "varbinary":
		if length, ok := int64Value(charMaxLength); ok {
			if length < 0 {
				return dataType + "(max)"
			}
			if length > 0 {
				return fmt.Sprintf("%s(%d)", dataType, length)
			}
		}
	case "decimal", "numeric":
		precision, hasPrecision := int64Value(numericPrecision)
		if !hasPrecision || precision <= 0 {
			return dataType
		}
		if scale, hasScale := int64Value(numericScale); hasScale {
			return fmt.Sprintf("%s(%d,%d)", dataType, precision, scale)
		}
		return fmt.Sprintf("%s(%d)", dataType, precision)
	}

	return dataType
}

func int64Value(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return unsignedToInt64(uint64(v))
	case uint8:
		return unsignedToInt64(uint64(v))
	case uint16:
		return unsignedToInt64(uint64(v))
	case uint32:
		return unsignedToInt64(uint64(v))
	case uint64:
		return unsignedToInt64(v)
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 64)
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func unsignedToInt64(value uint64) (int64, bool) {
	const maxInt64 = 1<<63 - 1
	if value > maxInt64 {
		return 0, false
	}

	return int64(value), true
}

func fromFabricValue(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case nil:
		return "", false
	default:
		return "", false
	}
}

func (db *DB) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	currentDB := db.config.Database
	if currentDB == "" {
		return nil, errors.New("database name not configured")
	}

	const schemaQuery = `
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM information_schema.tables
WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
`

	tables, err := db.Select(ctx, &query.Query{Query: schemaQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.tables: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    currentDB,
		Schemas: []*ansisql.DBSchema{},
	}
	schemaCache := make(map[string]*ansisql.DBSchema)

	for _, row := range tables {
		if len(row) < 3 {
			continue
		}

		schemaName, ok := fromFabricValue(row[0])
		if !ok {
			continue
		}
		tableName, ok := fromFabricValue(row[1])
		if !ok {
			continue
		}
		tableType, ok := fromFabricValue(row[2])
		if !ok {
			continue
		}

		schema, ok := schemaCache[schemaName]
		if !ok {
			schema = &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemaCache[schemaName] = schema
			summary.Schemas = append(summary.Schemas, schema)
		}

		dbTableType := ansisql.DBTableTypeTable
		if strings.EqualFold(tableType, "VIEW") || strings.EqualFold(tableType, "MATERIALIZED VIEW") {
			dbTableType = ansisql.DBTableTypeView
		}

		schema.Tables = append(schema.Tables, &ansisql.DBTable{
			Name: tableName,
			Type: dbTableType,
		})
	}

	return summary, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI()
}

func (db *DB) Close() error {
	return db.conn.Close()
}
