package spark

import (
	"context"
	"database/sql"
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
	connection  connection
	config      Config
	schemaCache sync.Map
}

func NewClient(ctx context.Context, c Config) (*Client, error) {
	dsn, err := c.ToDSN()
	if err != nil {
		return nil, err
	}
	if err := EnsureADBCDriverInstalled(ctx); err != nil {
		return nil, err
	}
	conn, err := sql.Open(ADBCDriverName(), dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Spark connection")
	}
	return &Client{connection: conn, config: c}, nil
}

func trimQuery(queryObj *query.Query) string {
	return strings.TrimSuffix(strings.TrimSpace(queryObj.String()), ";")
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, queryObj *query.Query) error {
	return c.RunQueriesWithoutResult(ctx, []*query.Query{queryObj})
}

func (c *Client) RunQueriesWithoutResult(ctx context.Context, queries []*query.Query) error {
	executor := interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}(c.connection)

	if database, ok := c.connection.(*sql.DB); ok {
		session, err := database.Conn(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to acquire Spark session")
		}
		defer session.Close()
		executor = session
	}

	for _, queryObj := range queries {
		if _, err := executor.ExecContext(ctx, trimQuery(queryObj)); err != nil {
			return errors.Wrap(err, "failed to execute Spark query")
		}
	}
	return nil
}

func (c *Client) Select(ctx context.Context, queryObj *query.Query) ([][]interface{}, error) {
	rows, err := c.connection.QueryContext(ctx, trimQuery(queryObj))
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
	rows, err := c.connection.QueryContext(ctx, trimQuery(queryObj))
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

	options, err := c.config.ToOptions()
	if err != nil {
		return nil, err
	}
	database, err := newADBCDatabase(options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Spark ADBC database")
	}
	defer database.Close()
	connection, err := database.Open(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Spark ADBC connection")
	}
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
	options, err := c.config.ToOptions()
	if err != nil {
		return nil, err
	}
	database, err := newADBCDatabase(options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Spark ADBC database")
	}
	defer database.Close()
	connection, err := database.Open(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Spark ADBC connection")
	}
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
