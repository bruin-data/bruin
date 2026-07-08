package doris

import (
	"context"
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
	config DorisConfig
	mutex  sync.Mutex
}

type DorisConfig interface {
	GetIngestrURI() string
	ToDBConnectionURI() string
}

func NewClient(c DorisConfig) (*Client, error) {
	return NewClientWithContext(context.Background(), c)
}

func NewClientWithContext(_ context.Context, c DorisConfig) (*Client, error) {
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
		return errors.Wrap(err, "failed to connect to Doris")
	}

	c.conn = conn
	return nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	if err := c.initializeDB(ctx); err != nil {
		return err
	}

	_, err := c.conn.ExecContext(ctx, query.String())
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}

	return nil
}

func (c *Client) Select(ctx context.Context, queryObj *query.Query) ([][]interface{}, error) {
	if err := c.initializeDB(ctx); err != nil {
		return nil, err
	}

	rows, err := c.conn.QueryContext(ctx, queryObj.String())
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

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

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
	if err := c.initializeDB(ctx); err != nil {
		return nil, err
	}

	rows, err := c.conn.QueryContext(ctx, queryObj.String())
	if err != nil {
		return nil, errors.New(strings.ReplaceAll(err.Error(), "\n", "  -  "))
	}
	defer rows.Close()

	result := &query.QueryResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column names")
	}
	result.Columns = cols

	for rows.Next() {
		row := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range row {
			columnPointers[i] = &row[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		for i, v := range row {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			}
		}

		result.Rows = append(result.Rows, row)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", rows.Err())
	}

	return result, nil
}

func (c *Client) Ping(ctx context.Context) error {
	q := query.Query{Query: "SELECT 1"}
	if err := c.RunQueryWithoutResult(ctx, &q); err != nil {
		return errors.Wrap(err, "failed to run test query on Doris connection")
	}

	return nil
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
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
    AND t.table_schema NOT IN ('information_schema', 'mysql')
ORDER BY t.table_schema, t.table_name;
`

	result, err := c.Select(ctx, &query.Query{Query: q})
	if err != nil {
		return nil, fmt.Errorf("failed to query Doris information_schema: %w", err)
	}

	summary := &ansisql.DBDatabase{
		Name:    "doris",
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
				rc := int64(v) //nolint:gosec // G115: row counts fit int64 in metadata display.
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
				sb := int64(v) //nolint:gosec // G115: sizes fit int64 in metadata display.
				sizeBytes = &sb
			}
		}

		var tableComment string
		if len(row) > 8 && row[8] != nil {
			if c, ok := row[8].(string); ok {
				tableComment = c
			}
		}

		if _, exists := schemas[schemaName]; !exists {
			schemas[schemaName] = &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
		}

		var dbTableType ansisql.DBTableType
		if tableType == "VIEW" {
			dbTableType = ansisql.DBTableTypeView
		} else {
			dbTableType = ansisql.DBTableTypeTable
		}

		schemas[schemaName].Tables = append(schemas[schemaName].Tables, &ansisql.DBTable{
			Name:           tableName,
			Type:           dbTableType,
			ViewDefinition: viewDefinition,
			Columns:        []*ansisql.DBColumn{},
			CreatedAt:      createdAt,
			LastModified:   lastModified,
			RowCount:       rowCount,
			SizeBytes:      sizeBytes,
			Description:    tableComment,
		})
	}

	for _, schema := range schemas {
		summary.Schemas = append(summary.Schemas, schema)
	}

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
		return "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = " + quoteStringLiteral(tableComponents[0]), nil
	case 2:
		return strings.TrimSpace(fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
			quoteStringLiteral(tableComponents[0]),
			quoteStringLiteral(tableComponents[1]),
		)), nil
	default:
		return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
	}
}

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	if asset == nil {
		return errors.New("asset cannot be nil")
	}

	nameParts := strings.Split(asset.Name, ".")
	if len(nameParts) < 2 {
		return nil
	}

	schemaName := strings.TrimSpace(nameParts[0])
	if schemaName == "" {
		return nil
	}

	return c.RunQueryWithoutResult(ctx, &query.Query{Query: "CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(schemaName)})
}
