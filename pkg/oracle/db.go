package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	_ "github.com/sijms/go-ora/v2"
)

type Client struct {
	conn   *sql.DB
	config *Config
}

func NewClient(c Config) (*Client, error) {
	dsn, err := c.DSN()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create DSN")
	}

	conn, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open oracle connection")
	}

	return &Client{
		conn:   conn,
		config: &c,
	}, nil
}

func (db *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	queryStr := strings.TrimSpace(query.String())
	queryStr = strings.TrimSuffix(queryStr, ";")
	_, err := db.conn.ExecContext(ctx, queryStr)
	return errors.Wrap(err, "failed to execute query")
}

func (db *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	queryStr := strings.TrimSpace(query.String())
	queryStr = strings.TrimSuffix(queryStr, ";")
	rows, err := db.conn.QueryContext(ctx, queryStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute select query")
	}
	defer rows.Close()

	// Get column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column names")
	}

	var result [][]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		result = append(result, columns)
	}

	if rows.Err() != nil {
		return nil, errors.Wrap(rows.Err(), "error during row iteration")
	}

	return result, nil
}

func (db *Client) Ping(ctx context.Context) error {
	// Simple ping query
	q := &query.Query{Query: "SELECT 1 FROM DUAL"}
	return db.RunQueryWithoutResult(ctx, q)
}

func (db *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	queryStr := strings.TrimSpace(queryObj.String())
	queryStr = strings.TrimSuffix(queryStr, ";")
	rows, err := db.conn.QueryContext(ctx, queryStr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute select query")
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

	// Fetch column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column types")
	}
	typeStrings := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeStrings[i] = ct.DatabaseTypeName()
	}
	result.ColumnTypes = typeStrings

	// Fetch all rows
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

func (db *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	// Query to get all schemas and tables in the database
	q := `
SELECT
    owner as schema_name,
    table_name
FROM
    all_tables
WHERE
    owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM', 'APPQOSSYS', 'DBSNMP', 'CTXSYS', 'XDB', 'ANONYMOUS', 'EXFSYS', 'MDDATA', 'DBSFWUSER', 'REMOTE_SCHEDULER_AGENT', 'SI_INFORMTN_SCHEMA', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS', 'WMSYS', 'APEX_040000', 'APEX_PUBLIC_USER', 'FLOWS_FILES', 'MDDATA', 'SI_INFORMTN_SCHEMA', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'HR', 'OE', 'PM', 'IX', 'SH', 'BI', 'SCOTT')
ORDER BY owner, table_name`

	result, err := db.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Oracle all_tables: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    db.config.ServiceName,
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

	return summary, nil
}
