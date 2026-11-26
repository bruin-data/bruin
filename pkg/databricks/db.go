package databricks

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/tablename"
	_ "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

// disableDriverLogs ensures the Databricks driver's noisy logs are disabled.
// Uses sync.Once to ensure this only happens once across all DB instances.
var disableDriverLogs = sync.OnceFunc(func() {
	zerolog.SetGlobalLevel(zerolog.Disabled)
})

type DB struct {
	conn          *sqlx.DB
	config        *Config
	schemaCreator *ansisql.SchemaCreator
	mu            sync.Mutex
}

func NewDB(c *Config) (*DB, error) {
	return &DB{
		config:        c,
		schemaCreator: ansisql.NewSchemaCreatorWithContainer("CATALOG"),
	}, nil
}

func (db *DB) ensureConnection() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.conn != nil {
		return nil
	}

	disableDriverLogs()

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

func withNativeQueryTags(ctx context.Context, queryObj *query.Query) context.Context {
	if len(queryObj.Annotations) == 0 {
		return ctx
	}
	return driverctx.NewContextWithQueryTags(ctx, queryObj.Annotations)
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	if err := db.ensureConnection(); err != nil {
		return nil, err
	}

	queryString := query.String()
	ctx = withNativeQueryTags(ctx, query)
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
	ctx = withNativeQueryTags(ctx, queryObj)
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

type annotatedSchemaQueryRunner struct {
	db           *DB
	assetName    string
	pipelineName string
}

func (r *annotatedSchemaQueryRunner) RunQueryWithoutResult(ctx context.Context, q *query.Query) error {
	annotatedQuery, err := ansisql.AddAnnotationComment(ctx, q, r.assetName, "schema", r.pipelineName)
	if err != nil {
		return errors.Wrap(err, "failed to add schema annotation comment")
	}

	return r.db.RunQueryWithoutResult(ctx, annotatedQuery)
}

func (db *DB) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset, pipelineName string) error {
	runner := &annotatedSchemaQueryRunner{
		db:           db,
		assetName:    asset.Name,
		pipelineName: pipelineName,
	}
	return db.schemaCreator.CreateSchemaIfNotExist(ctx, runner, asset)
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
	// We'll query INFORMATION_SCHEMA to get all schemas and tables with their types
	// Note: In Databricks, view_definition is available in information_schema.views
	q := `
SELECT
    t.table_schema,
    t.table_name,
    t.table_type,
    v.view_definition
FROM
    information_schema.tables t
LEFT JOIN
    information_schema.views v ON t.table_schema = v.table_schema AND t.table_name = v.table_name
WHERE
    t.table_type IN ('TABLE', 'VIEW')
    AND t.table_schema NOT IN ('information_schema')
ORDER BY t.table_schema, t.table_name;
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
		if len(row) != 4 {
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

var (
	// plainIdentifier matches the asset name components that Databricks reads
	// without backticks.
	plainIdentifier = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	// likeSafeName matches names that mean the same in a SHOW ... LIKE pattern,
	// whether the pattern is read as a regular expression or as SQL LIKE (where
	// `_` matches any character, which only widens the listing).
	likeSafeName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

// relationName is an asset name split into the identifiers it refers to. The
// catalog is empty for a two-part name, which resolves against the session's
// current catalog.
type relationName struct {
	catalog string
	schema  string
	table   string
}

func (r relationName) schemaRef() string {
	if r.catalog == "" {
		return quoteIdentifier(r.schema)
	}
	return quoteIdentifier(r.catalog) + "." + quoteIdentifier(r.schema)
}

func quoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

// parseRelationName splits an asset name the way the materializer's SQL reads
// it: the last component is the table, the one before it the schema, and the
// first of three the catalog. Components are plain identifiers or quoted with
// backticks; anything else is an error rather than a guess.
func parseRelationName(name string) (relationName, error) {
	cb, ok := tablename.For("databricks")
	if !ok {
		return relationName{}, errors.New("databricks table-name capability not found")
	}
	if err := cb.CheckName(name); err != nil {
		return relationName{}, err
	}

	parts := strings.Split(name, ".")
	identifiers := make([]string, len(parts))
	for i, part := range parts {
		identifier, err := parseIdentifier(part)
		if err != nil {
			return relationName{}, fmt.Errorf("cannot resolve table name %q: %w", name, err)
		}
		identifiers[i] = identifier
	}

	n := len(identifiers)
	relation := relationName{schema: identifiers[n-2], table: identifiers[n-1]}
	if n == 3 {
		relation.catalog = identifiers[0]
	}

	return relation, nil
}

func parseIdentifier(part string) (string, error) {
	part = strings.TrimSpace(part)
	if len(part) >= 2 && strings.HasPrefix(part, "`") && strings.HasSuffix(part, "`") {
		quoted := part[1 : len(part)-1]
		if quoted == "" || strings.Contains(strings.ReplaceAll(quoted, "``", ""), "`") {
			return "", fmt.Errorf("invalid quoted identifier %s", part)
		}
		// A path such as delta.`/mnt/table` names a table by its location, which
		// SHOW TABLES does not list.
		if strings.Contains(quoted, "/") {
			return "", fmt.Errorf("path-based table %s cannot be checked", part)
		}
		return strings.ReplaceAll(quoted, "``", "`"), nil
	}

	if !plainIdentifier.MatchString(part) {
		return "", fmt.Errorf("identifier %q has to be quoted with backticks", part)
	}

	return part, nil
}

// RelationExists reports whether a table, view or any other relation exists
// under tableName, so an incremental asset can create its table on the first
// run.
//
// The materializer writes to the asset name as written, so a two-part name
// resolves against the session's current catalog: the connection's catalog, or
// the workspace default when it has none. The lookup runs the unqualified name
// on the same connection to resolve it the same way. Databricks compares
// identifiers case-insensitively, and so does the lookup.
//
// SHOW TABLES lists tables, views, materialized views and streaming tables, in
// Unity Catalog and in hive_metastore. It fails when the schema or its catalog
// does not exist, but also on any other error, so a missing schema or catalog
// is only reported when SHOW SCHEMAS or SHOW CATALOGS succeeds without it. All
// three are metadata commands that run on the connection's SQL warehouse or
// cluster.
func (db *DB) RelationExists(ctx context.Context, tableName string) (bool, error) {
	relation, err := parseRelationName(tableName)
	if err != nil {
		return false, err
	}

	exists, tablesErr := db.listingContains(ctx, "SHOW TABLES IN "+relation.schemaRef(), relation.table, "tableName")
	if tablesErr == nil {
		return exists, nil
	}
	tablesErr = fmt.Errorf("failed to list the tables in %s: %w", relation.schemaRef(), tablesErr)

	showSchemas := "SHOW SCHEMAS"
	if relation.catalog != "" {
		showSchemas += " IN " + quoteIdentifier(relation.catalog)
	}
	schemaExists, err := db.listingContains(ctx, showSchemas, relation.schema, "")
	if err == nil {
		if schemaExists {
			return false, tablesErr
		}
		return false, nil
	}
	// spark_catalog is an alias of the session catalog that SHOW CATALOGS does
	// not have to list, so it never counts as a missing catalog.
	if relation.catalog == "" || strings.EqualFold(relation.catalog, "spark_catalog") {
		return false, tablesErr
	}

	catalogExists, err := db.listingContains(ctx, "SHOW CATALOGS", relation.catalog, "")
	if err != nil || catalogExists {
		return false, tablesErr
	}

	return false, nil
}

// listingContains runs a SHOW statement and reports whether it lists name,
// compared case-insensitively. column is the output column that holds the
// names; when it is empty or missing from the output, every column is
// compared, which can only report a name that is not there.
func (db *DB) listingContains(ctx context.Context, statement, name, column string) (bool, error) {
	if likeSafeName.MatchString(name) {
		// LIKE only narrows the listing down; names are compared exactly below.
		// Databricks stores identifiers in lower case.
		statement += " LIKE '" + strings.ToLower(name) + "'"
	}

	result, err := db.SelectWithSchema(ctx, &query.Query{Query: statement})
	if err != nil {
		return false, err
	}

	nameColumn := -1
	for i, c := range result.Columns {
		if column != "" && strings.EqualFold(c, column) {
			nameColumn = i
			break
		}
	}

	for _, row := range result.Rows {
		hasName := false
		for i, cell := range row {
			if nameColumn >= 0 && i != nameColumn {
				continue
			}

			var value string
			switch v := cell.(type) {
			case string:
				value = v
			case []byte:
				value = string(v)
			default:
				continue
			}

			hasName = true
			if strings.EqualFold(value, name) {
				return true, nil
			}
		}

		if !hasName {
			return false, fmt.Errorf("unexpected output from %s: %v", statement, row)
		}
	}

	return false, nil
}

func (db *DB) BuildTableExistsQuery(tableName string) (string, error) {
	cb, ok := tablename.For("databricks")
	if !ok {
		return "", errors.New("databricks table-name capability not found")
	}
	tn, err := cb.Parse(tableName, tablename.Defaults{})
	if err != nil {
		return "", err
	}

	// information_schema is catalog-scoped in Unity Catalog; qualify the lookup
	// with the catalog when the name carries one.
	infoSchema := "information_schema"
	if tn.Catalog != "" {
		infoSchema = tn.Catalog + ".information_schema"
	}

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.tables WHERE table_schema = '%s' AND table_name = '%s'",
		infoSchema,
		tn.Schema,
		tn.Table,
	)

	return strings.TrimSpace(query), nil
}
