package fabric

import (
	"context"
	"fmt"
	"strings"

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
	return db.config.GetIngestrURI(), nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}
