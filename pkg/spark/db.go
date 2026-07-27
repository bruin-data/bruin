package spark

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/tablename"
	"github.com/pkg/errors"
)

type connection interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Client struct {
	// connection is only set by tests that inject a mock; production clients
	// build their pool lazily through conn().
	connection  connection
	config      Config
	schemaCache sync.Map

	poolOnce sync.Once
	pool     *sql.DB
	poolErr  error
}

func NewClient(_ context.Context, c Config) (*Client, error) {
	// The connection is not opened here: the ADBC driver has to be downloaded
	// and installed before it can be used, and every Bruin command builds all
	// configured connections up front. Doing that work eagerly would make a
	// single Spark entry in .bruin.yml fail commands that touch no Spark asset.
	if _, err := c.ToDSN(); err != nil {
		return nil, err
	}
	return &Client{config: c}, nil
}

// conn lazily installs the ADBC driver and opens the connection pool.
func (c *Client) conn(ctx context.Context) (connection, error) { //nolint:ireturn
	if c.connection != nil {
		return c.connection, nil
	}
	c.poolOnce.Do(func() {
		dsn, err := c.config.ToDSN()
		if err != nil {
			c.poolErr = err
			return
		}
		if err := EnsureADBCDriverInstalled(ctx); err != nil {
			c.poolErr = err
			return
		}
		pool, err := sql.Open(ADBCDriverName(), dsn)
		if err != nil {
			c.poolErr = errors.Wrap(err, "failed to open Spark connection")
			return
		}
		c.pool = pool
	})
	if c.poolErr != nil {
		return nil, c.poolErr
	}
	return c.pool, nil
}

// adbcConnection opens a raw ADBC connection for the APIs that are not exposed
// through database/sql, such as bulk ingest and the object catalog.
func (c *Client) adbcConnection(ctx context.Context) (adbc.Database, adbc.Connection, error) { //nolint:ireturn
	if err := EnsureADBCDriverInstalled(ctx); err != nil {
		return nil, nil, err
	}
	options, err := c.config.ToOptions()
	if err != nil {
		return nil, nil, err
	}
	database, err := newADBCDatabase(options)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create Spark ADBC database")
	}
	conn, err := database.Open(ctx)
	if err != nil {
		database.Close()
		return nil, nil, errors.Wrap(err, "failed to open Spark ADBC connection")
	}
	return database, conn, nil
}

func trimQuery(queryObj *query.Query) string {
	return strings.TrimSuffix(strings.TrimSpace(queryObj.String()), ";")
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, queryObj *query.Query) error {
	return c.RunQueriesWithoutResult(ctx, []*query.Query{queryObj})
}

func (c *Client) RunQueriesWithoutResult(ctx context.Context, queries []*query.Query) error {
	conn, err := c.conn(ctx)
	if err != nil {
		return err
	}

	executor := interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}(conn)

	// Session statements such as USE and SET only apply to the statements that
	// follow them on the same connection, so all the queries of a task are
	// pinned to a single session.
	if database, ok := conn.(*sql.DB); ok {
		session, err := database.Conn(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to acquire Spark session")
		}
		// That same state would otherwise be handed to the next caller through
		// the idle pool, so a session that ran one is discarded rather than
		// reused: the ADBC driver implements no session reset.
		discard := false
		defer func() {
			if discard {
				_ = session.Raw(func(any) error { return driver.ErrBadConn })
			}
			_ = session.Close()
		}()
		executor = session

		for _, queryObj := range queries {
			if isSparkSessionStatement(queryObj.Query) {
				discard = true
			}
			if _, err := executor.ExecContext(ctx, trimQuery(queryObj)); err != nil {
				return errors.Wrap(err, "failed to execute Spark query")
			}
		}
		return nil
	}

	for _, queryObj := range queries {
		if _, err := executor.ExecContext(ctx, trimQuery(queryObj)); err != nil {
			return errors.Wrap(err, "failed to execute Spark query")
		}
	}
	return nil
}

func (c *Client) Select(ctx context.Context, queryObj *query.Query) ([][]interface{}, error) {
	conn, err := c.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, trimQuery(queryObj))
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute Spark query")
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve Spark result columns")
	}

	result := make([][]interface{}, 0)
	for rows.Next() {
		row := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range row {
			pointers[i] = &row[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, errors.Wrap(err, "failed to scan Spark result row")
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed while reading Spark result rows")
	}
	return result, nil
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	conn, err := c.conn(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, trimQuery(queryObj))
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute Spark query")
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve Spark result columns")
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve Spark result column types")
	}

	result := &query.QueryResult{
		Columns:     columns,
		ColumnTypes: make([]string, len(columnTypes)),
		Rows:        make([][]interface{}, 0),
	}
	for i, columnType := range columnTypes {
		result.ColumnTypes[i] = columnType.DatabaseTypeName()
	}
	for rows.Next() {
		row := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range row {
			pointers[i] = &row[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, errors.Wrap(err, "failed to scan Spark result row")
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed while reading Spark result rows")
	}
	return result, nil
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Select(ctx, &query.Query{Query: "SELECT 1"})
	return err
}

func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	results, err := c.TablesExist(ctx, []string{tableName})
	if err != nil {
		return false, err
	}
	return results[tableName], nil
}

func (c *Client) TablesExist(ctx context.Context, tableNames []string) (map[string]bool, error) {
	results := make(map[string]bool, len(tableNames))
	if len(tableNames) == 0 {
		return results, nil
	}

	database, connection, err := c.adbcConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer database.Close()
	defer connection.Close()

	defaultCatalog, defaultSchema, err := sparkNamespaceDefaults(ctx, connection, c.config.Catalog)
	if err != nil {
		return nil, err
	}
	for _, tableName := range tableNames {
		exists, err := tableExists(ctx, connection, tableName, defaultCatalog, defaultSchema)
		if err != nil {
			return nil, err
		}
		results[tableName] = exists
	}
	return results, nil
}

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset, pipelineName string) error {
	capability, ok := tablename.For("spark")
	if !ok {
		return errors.New("Spark table-name capability not found")
	}
	name, err := capability.Parse(asset.Name, tablename.Defaults{})
	if err != nil {
		return err
	}
	if name.Schema == "" {
		return nil
	}

	schemaName := quoteIdentifier(name.Schema)
	cacheKey := name.Schema
	if name.Catalog != "" {
		schemaName = quoteIdentifier(name.Catalog) + "." + schemaName
		cacheKey = name.Catalog + "." + name.Schema
	}
	if _, exists := c.schemaCache.Load(cacheKey); exists {
		return nil
	}
	ctx = query.WithQueryType(ctx, query.QueryTypeSchema)
	schemaQuery, err := ansisql.AddAnnotationComment(
		ctx,
		&query.Query{Query: "CREATE SCHEMA IF NOT EXISTS " + schemaName},
		asset.Name,
		"schema",
		pipelineName,
	)
	if err != nil {
		return errors.Wrap(err, "failed to add Spark schema annotation")
	}
	if err := c.RunQueryWithoutResult(ctx, schemaQuery); err != nil {
		return errors.Wrapf(err, "failed to ensure Spark schema %q exists", cacheKey)
	}
	c.schemaCache.Store(cacheKey, true)
	return nil
}

type objectColumn struct {
	Name     string  `json:"column_name"`
	Type     *string `json:"xdbc_type_name"`
	Nullable *string `json:"xdbc_is_nullable"`
	Remarks  *string `json:"remarks"`
}

type objectTable struct {
	Name    string         `json:"table_name"`
	Type    string         `json:"table_type"`
	Columns []objectColumn `json:"table_columns"`
}

type objectSchema struct {
	Name   *string       `json:"db_schema_name"`
	Tables []objectTable `json:"db_schema_tables"`
}

type objectCatalog struct {
	Name    *string        `json:"catalog_name"`
	Schemas []objectSchema `json:"catalog_db_schemas"`
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	database, connection, err := c.adbcConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer database.Close()
	defer connection.Close()

	databaseName := c.config.Catalog
	var catalogFilter *string
	if c.config.Catalog != "" {
		catalogFilter = &c.config.Catalog
	} else {
		databaseName, _, err = currentSparkNamespace(ctx, connection)
		if err != nil {
			return nil, err
		}
		if databaseName == "" {
			return nil, errors.New("Spark current-namespace query returned an empty catalog")
		}
	}
	reader, err := connection.GetObjects(ctx, adbc.ObjectDepthAll, catalogFilter, nil, nil, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve Spark database objects")
	}
	defer reader.Release()

	summary := &ansisql.DBDatabase{Name: databaseName, Schemas: []*ansisql.DBSchema{}}
	schemas := make(map[string]*ansisql.DBSchema)
	for reader.Next() {
		data, err := reader.RecordBatch().MarshalJSON()
		if err != nil {
			return nil, errors.Wrap(err, "failed to encode Spark database objects")
		}
		var catalogs []objectCatalog
		if err := json.Unmarshal(data, &catalogs); err != nil {
			return nil, errors.Wrap(err, "failed to decode Spark database objects")
		}
		appendObjectCatalogs(schemas, catalogs, c.config.Catalog == "")
	}
	if err := reader.Err(); err != nil {
		return nil, errors.Wrap(err, "failed while reading Spark database objects")
	}

	for _, schema := range schemas {
		sort.Slice(schema.Tables, func(i, j int) bool {
			return schema.Tables[i].Name < schema.Tables[j].Name
		})
		summary.Schemas = append(summary.Schemas, schema)
	}
	sort.Slice(summary.Schemas, func(i, j int) bool {
		return summary.Schemas[i].Name < summary.Schemas[j].Name
	})
	return summary, nil
}

func appendObjectCatalogs(
	schemas map[string]*ansisql.DBSchema,
	catalogs []objectCatalog,
	includeCatalog bool,
) {
	for _, catalog := range catalogs {
		for _, schema := range catalog.Schemas {
			if schema.Name == nil || strings.EqualFold(*schema.Name, "information_schema") ||
				strings.EqualFold(*schema.Name, "sys") {
				continue
			}
			schemaName := *schema.Name
			if includeCatalog && catalog.Name != nil && *catalog.Name != "" {
				schemaName = *catalog.Name + "." + schemaName
			}
			dbSchema, exists := schemas[schemaName]
			if !exists {
				dbSchema = &ansisql.DBSchema{Name: schemaName, Tables: []*ansisql.DBTable{}}
				schemas[schemaName] = dbSchema
			}
			for _, table := range schema.Tables {
				kind := ansisql.DBTableTypeTable
				if strings.Contains(strings.ToUpper(table.Type), "VIEW") {
					kind = ansisql.DBTableTypeView
				}
				columns := make([]*ansisql.DBColumn, 0, len(table.Columns))
				for _, column := range table.Columns {
					columnType := ""
					if column.Type != nil {
						columnType = *column.Type
					}
					description := ""
					if column.Remarks != nil {
						description = *column.Remarks
					}
					nullable := column.Nullable == nil || !strings.EqualFold(*column.Nullable, "NO")
					columns = append(columns, &ansisql.DBColumn{
						Name:        column.Name,
						Type:        columnType,
						Nullable:    nullable,
						Description: description,
					})
				}
				dbSchema.Tables = append(dbSchema.Tables, &ansisql.DBTable{
					Name:    table.Name,
					Type:    kind,
					Columns: columns,
				})
			}
		}
	}
}

func quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	for i, part := range parts {
		parts[i] = "`" + strings.ReplaceAll(part, "`", "``") + "`"
	}
	return strings.Join(parts, ".")
}
