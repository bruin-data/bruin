package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Querier interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
}

type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
}

type DB interface {
	Querier
	Selector
}

type Client struct {
	conn   *sqlx.DB
	config MySQLConfig
	mutex  sync.Mutex
}

type MySQLConfig interface {
	GetIngestrURI() string
	ToDBConnectionURI() string
}

func NewClient(c MySQLConfig) (*Client, error) {
	return NewClientWithContext(context.Background(), c)
}

func NewClientWithContext(ctx context.Context, c MySQLConfig) (*Client, error) {
	// Don't connect here - use lazy initialization when queries execute
	return &Client{
		config: c,
		mutex:  sync.Mutex{},
	}, nil
}

func (c *Client) initializeDB(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		return nil
	}

	conn, err := sqlx.ConnectContext(ctx, "mysql", c.config.ToDBConnectionURI())
	if err != nil {
		return errors.Wrapf(err, "failed to connect to mysql")
	}

	c.conn = conn
	return nil
}

func (c *Client) GetIngestrURI() (string, error) {
	if cfg, ok := c.config.(interface{ IsReadOnly() bool }); ok && cfg.IsReadOnly() {
		return "", errors.New("read_only connections cannot be used with ingestr")
	}
	return c.config.GetIngestrURI(), nil
}

// type connection interface {
//	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
//	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
//}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	if err := c.initializeDB(ctx); err != nil {
		return err
	}
	conn, cleanup, err := c.queryConnection(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	_, err = conn.ExecContext(ctx, query.String())
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}

	return nil
}

func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	if err := c.initializeDB(ctx); err != nil {
		return nil, err
	}
	conn, cleanup, err := c.queryConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	rows, err := conn.QueryContext(ctx, query.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get columns")
	}

	collectedRows := make([][]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		// Convert []byte to string
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		collectedRows = append(collectedRows, values)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error during row iteration")
	}

	return collectedRows, nil
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	return query.CollectRows(ctx, c, queryObj)
}

func (c *Client) StreamWithSchema(ctx context.Context, queryObj *query.Query, h query.RowHandler) (*query.QueryResult, error) {
	if err := c.initializeDB(ctx); err != nil {
		return nil, err
	}
	queryString := queryObj.String()
	conn, cleanup, err := c.queryConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	ctx, cancel := context.WithCancel(ctx)
	rows, err := conn.QueryContext(ctx, queryString)
	if err != nil {
		cancel()
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
		return nil, err
	}
	defer rows.Close()
	// Cancel before closing rows so an output failure does not drain the result.
	defer cancel()

	result := &query.QueryResult{
		Columns: []string{},
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
	result.ColumnTypes = make([]string, len(columnTypes))
	for i, columnType := range columnTypes {
		result.ColumnTypes[i] = columnType.DatabaseTypeName()
	}
	if err := h.Schema(result.Columns, result.ColumnTypes); err != nil {
		return nil, err
	}

	// Fetch rows and pass them to the handler without retaining them.
	for rows.Next() {
		row := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range row {
			columnPointers[i] = &row[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// convert []byte -> string
		for i, v := range row {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			}
		}

		if err := h.Row(row); err != nil {
			return nil, err
		}
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", rows.Err())
	}

	return result, nil
}

func (c *Client) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}

	err := c.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on MySQL connection")
	}

	return nil
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// MySQL uses schemas as databases, so we'll get all databases and their tables
	q := `
SELECT
    t.table_schema,
    t.table_name,
    t.table_type,
    v.view_definition,
    t.create_time,
    t.update_time,
    t.table_rows,
    t.data_length,
    t.table_comment
FROM
    information_schema.tables t
LEFT JOIN
    information_schema.views v ON t.table_schema = v.table_schema AND t.table_name = v.table_name
WHERE
    t.table_type IN ('BASE TABLE', 'VIEW')
    AND t.table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
ORDER BY t.table_schema, t.table_name;
`

	result, err := c.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL information_schema: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    "mysql", // MySQL instance
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
			case uint64:
				rc := int64(v) //nolint:gosec // G115: MySQL row count uint64->int64 is safe; table row counts won't exceed int64 max
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
			case uint64:
				sb := int64(v) //nolint:gosec // G115: MySQL data size uint64->int64 is safe; table sizes won't exceed int64 max
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

func (c *Client) BuildTableExistsQuery(tableName string) (string, error) {
	tableComponents := strings.Split(tableName, ".")
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
		}
	}

	switch len(tableComponents) {
	case 1:
		return strings.TrimSpace(fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'",
			tableComponents[0],
		)), nil
	case 2:
		return strings.TrimSpace(fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
			tableComponents[0],
			tableComponents[1],
		)), nil
	default:
		return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
	}
}

// RelationExists reports whether a table or view named tableName exists, so an
// incremental asset can create its table on the first run. The materializer
// writes the name unquoted, so `table` resolves in the connection's default
// database and `database.table` as written. Whether names are case-sensitive
// depends on the server's lower_case_table_names, so they are compared
// case-insensitively, which can only report more relations as existing.
func (c *Client) RelationExists(ctx context.Context, tableName string) (bool, error) {
	switch c.config.(type) {
	case Config, *Config:
	default:
		// Vitess and PlanetScale share this client, but their information_schema
		// is routed and rewritten per keyspace, so a lookup is not reliable there.
		// Reporting the table as existing keeps the incremental load, without a
		// warning on every run.
		return true, nil
	}

	parts := strings.Split(tableName, ".")
	if len(parts) > 2 {
		return false, fmt.Errorf("table name must be in format database.table or table, '%s' given", tableName)
	}
	for _, part := range parts {
		if !isUnquotedIdentifier(part) {
			return false, fmt.Errorf("cannot check whether table '%s' exists: only unquoted names made of letters, digits, '_' and '$' are supported", tableName)
		}
	}

	schema := "DATABASE()"
	args := []any{parts[len(parts)-1]}
	if len(parts) == 2 {
		schema = "?"
		args = append(args, parts[0])
	}

	if err := c.initializeDB(ctx); err != nil {
		return false, err
	}
	conn, cleanup, err := c.queryConnection(ctx)
	if err != nil {
		return false, err
	}
	defer cleanup()

	q := "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?) AND LOWER(table_schema) = LOWER(" + schema + ")"
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return false, errors.Wrapf(err, "failed to check whether table '%s' exists", tableName)
	}
	defer rows.Close()

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return false, errors.Wrapf(err, "failed to check whether table '%s' exists", tableName)
		}
	}
	if err := rows.Err(); err != nil {
		return false, errors.Wrapf(err, "failed to check whether table '%s' exists", tableName)
	}

	return count > 0, nil
}

// isUnquotedIdentifier reports whether name only has characters that MySQL
// accepts in an unquoted identifier, so it can be looked up literally.
func isUnquotedIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r < 0x80 && r != '_' && r != '$' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	if asset == nil {
		return errors.New("asset cannot be nil")
	}

	nameParts := strings.Split(asset.Name, ".")
	if len(nameParts) < 2 {
		return nil
	}

	schemaName := nameParts[0]
	if schemaName == "" {
		return nil
	}

	queryString := "CREATE DATABASE IF NOT EXISTS " + schemaName
	return c.RunQueryWithoutResult(ctx, &query.Query{Query: queryString})
}

type queryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (c *Client) queryConnection(ctx context.Context) (queryExecutor, func(), error) {
	cfg, ok := c.config.(interface{ IsReadOnly() bool })
	if !ok || !cfg.IsReadOnly() {
		return c.conn, func() {}, nil
	}
	tx, err := c.conn.BeginTxx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to start read-only transaction")
	}
	return tx, func() { _ = tx.Rollback() }, nil
}
