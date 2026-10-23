package sqlparser

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// SQLParser analyzes SQL in-process with pkg/sqlengine. It is safe for concurrent use.
type SQLParser struct {
	// MaxQueryLength is the longest query, in bytes, ColumnLineage analyzes.
	MaxQueryLength int
}

func NewSQLParser() (*SQLParser, error) {
	return &SQLParser{MaxQueryLength: 10000}, nil
}

// Start is a no-op; the parser needs no setup.
func (s *SQLParser) Start() error {
	return nil
}

type Schema map[string]map[string]string

type UpstreamColumn struct {
	Column string `json:"column"`
	Table  string `json:"table"`
}

type ColumnLineage struct {
	Name     string           `json:"name"`
	Upstream []UpstreamColumn `json:"upstream"`
	Type     string           `json:"type"`
}
type Lineage struct {
	Columns            []ColumnLineage `json:"columns"`
	NonSelectedColumns []ColumnLineage `json:"non_selected_columns"`
	Errors             []string        `json:"errors"`
}

// ColumnLineage returns the upstream columns of each column the query selects. A nil schema is
// the same as an empty one.
func (s *SQLParser) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
	if len(sql) > s.MaxQueryLength {
		return &Lineage{
			Columns:            []ColumnLineage{},
			NonSelectedColumns: []ColumnLineage{},
			Errors:             []string{"query is too long skipping column lineage analysis"},
		}, nil
	}

	sql, dialect, schema = validUTF8(sql), validUTF8(dialect), validUTF8Schema(schema)
	var lineage Lineage
	if err := run(&lineage, func() (any, error) {
		raw, err := toRawSchema(schema)
		if err != nil {
			return nil, err
		}
		return getColumnLineage(sql, raw, dialect)
	}); err != nil {
		return nil, err
	}
	return &lineage, nil
}

func (s *SQLParser) UsedTables(sql, dialect string) ([]string, error) {
	var tables tablesResponse
	if err := run(&tables, func() (any, error) { return getTables(validUTF8(sql), validUTF8(dialect)), nil }); err != nil {
		return nil, err
	}
	if tables.Error != "" {
		return nil, errors.New(tables.Error)
	}

	sort.Strings(tables.Tables)

	return tables.Tables, nil
}

// queryCommand runs a command whose response is a {query, error} object and returns the resulting
// query string. It is the shared body for the SQL-rewriting methods.
func queryCommand(cmd func() (any, error)) (string, error) {
	var resp queryResponse
	if err := run(&resp, cmd); err != nil {
		return "", err
	}
	if resp.Error != "" {
		return "", errors.New(resp.Error)
	}
	return resp.Query, nil
}

func (s *SQLParser) RenameTables(sql string, dialect string, tableMapping map[string]string) (string, error) {
	sql, dialect, tableMapping = validUTF8(sql), validUTF8(dialect), validUTF8Map(tableMapping)
	return queryCommand(func() (any, error) { return replaceTableReferences(sql, dialect, tableMapping) })
}

// HoistDeclares moves top-level declarations ahead of other statements without
// regenerating their SQL. On failure it returns the original input.
func (s *SQLParser) HoistDeclares(sql string, assetType pipeline.AssetType) (string, error) {
	dialect, err := AssetTypeToDialect(assetType)
	if err != nil {
		return sql, err
	}
	query, err := queryCommand(func() (any, error) { return hoistDeclares(validUTF8(sql), dialect), nil })
	if err != nil {
		return sql, err
	}
	return query, nil
}

// HoistDeclaresList preserves whole query entries, including their formatting.
func (s *SQLParser) HoistDeclaresList(queries []string, assetType pipeline.AssetType) ([]string, error) {
	dialect, err := AssetTypeToDialect(assetType)
	if err != nil {
		return queries, err
	}
	if len(queries) == 0 {
		return queries, nil
	}
	var resp queriesResponse
	if err := run(&resp, func() (any, error) { return hoistDeclaresList(validUTF8Slice(queries), dialect), nil }); err != nil {
		return queries, err
	}
	if resp.Error != "" {
		return queries, errors.New(resp.Error)
	}
	return resp.Queries, nil
}

// Close is a no-op; the parser holds no resources.
func (s *SQLParser) Close() error {
	return nil
}

type QueryConfig struct {
	Name   string `json:"name"`
	Query  string `json:"query"`
	Schema Schema `json:"schema"`
}

var assetTypeDialectMap = map[pipeline.AssetType]string{
	pipeline.AssetTypeBigqueryQuery:     "bigquery",
	pipeline.AssetTypeSnowflakeQuery:    "snowflake",
	pipeline.AssetTypePostgresQuery:     "postgres",
	pipeline.AssetTypeMySQLQuery:        "mysql",
	pipeline.AssetTypeDorisQuery:        "doris",
	pipeline.AssetTypeStarRocksQuery:    "starrocks",
	pipeline.AssetTypeRedshiftQuery:     "redshift",
	pipeline.AssetTypeAthenaQuery:       "athena",
	pipeline.AssetTypeTrinoQuery:        "trino",
	pipeline.AssetTypeDremioQuery:       "trino",
	pipeline.AssetTypeSailQuery:         "trino",
	pipeline.AssetTypeSparkQuery:        "spark",
	pipeline.AssetTypeFabricSparkQuery:  "spark",
	pipeline.AssetTypeClickHouse:        "clickhouse",
	pipeline.AssetTypeDatabricksQuery:   "databricks",
	pipeline.AssetTypeMsSQLQuery:        "tsql",
	pipeline.AssetTypeSynapseQuery:      "tsql",
	pipeline.AssetTypeDuckDBQuery:       "duckdb",
	pipeline.AssetTypeMotherduckQuery:   "duckdb",
	pipeline.AssetTypeOracleQuery:       "oracle",
	pipeline.AssetTypeFabricQuery:       "fabric",
	pipeline.AssetTypeFabricQueryLegacy: "fabric",
	pipeline.AssetTypeVerticaQuery:      "postgres",
}

func AssetTypeToDialect(assetType pipeline.AssetType) (string, error) {
	dialect, ok := assetTypeDialectMap[assetType]
	if !ok {
		return "", fmt.Errorf("unsupported asset type %s", assetType)
	}
	return dialect, nil
}

// connectionTypeDialectMap maps the connection type identifier used in the
// connection manager (the yaml tag of each connection field in
// config.Connections) to the dialect string the SQL parser understands.
// This is used to pick a dialect for queries that are run directly against a
// connection without going through a Bruin asset (where we'd otherwise know
// the asset type).
var connectionTypeDialectMap = map[string]string{
	"google_cloud_platform": "bigquery",
	"snowflake":             "snowflake",
	"postgres":              "postgres",
	"mysql":                 "mysql",
	"doris":                 "doris",
	"starrocks":             "starrocks",
	"redshift":              "redshift",
	"athena":                "athena",
	"trino":                 "trino",
	"dremio":                "trino",
	"sail":                  "trino",
	"spark":                 "spark",
	"clickhouse":            "clickhouse",
	"databricks":            "databricks",
	"mssql":                 "tsql",
	"synapse":               "tsql",
	"duckdb":                "duckdb",
	"motherduck":            "duckdb",
	"oracle":                "oracle",
	"fabric":                "fabric",
	"vertica":               "postgres",
}

// ConnectionTypeToDialect maps a connection type identifier (e.g. "clickhouse")
// to the dialect string used by the SQL parser. Returns the empty string
// when no dialect is registered for the type.
func ConnectionTypeToDialect(connectionType string) string {
	return connectionTypeDialectMap[connectionType]
}

func (s *SQLParser) AddLimit(sql string, limit int, dialect string) (string, error) {
	sql, dialect = validUTF8(sql), validUTF8(dialect)
	return queryCommand(func() (any, error) { return addLimit(sql, limit, dialect) })
}

// ExtractSelect reduces an asset statement to the SELECT that produces its
// rows. A CREATE OR REPLACE VIEW / CTAS / INSERT ... SELECT is unwrapped to its
// inner SELECT, and a statement that is already a SELECT (with or without a
// WITH clause) is returned unchanged. This lets a unit test exercise the read
// logic of a `materialization: none` (full-DDL) asset without issuing the DDL,
// keeping the test a single read-only SELECT. It errors when the statement has
// no SELECT to test (e.g. a CREATE TABLE with a column list).
func (s *SQLParser) ExtractSelect(sql string, dialect string) (string, error) {
	sql, dialect = validUTF8(sql), validUTF8(dialect)
	return queryCommand(func() (any, error) { return extractSelect(sql, dialect), nil })
}

// SelectFromCTE rewrites a query so it returns all rows of one of its own named
// CTEs (WITH … SELECT * FROM <cteName>), keeping the other CTEs in place. It
// lets a unit test assert the output of an intermediate CTE. Errors if the
// query has no WITH clause or no CTE with that name.
func (s *SQLParser) SelectFromCTE(sql string, dialect string, cteName string) (string, error) {
	sql, dialect, cteName = validUTF8(sql), validUTF8(dialect), validUTF8(cteName)
	return queryCommand(func() (any, error) { return selectCTE(sql, dialect, cteName), nil })
}

// FreezeTime replaces CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME in a query
// with literals fixed at executionTime, so a unit test of a time-dependent
// asset is deterministic instead of reading the warehouse clock.
func (s *SQLParser) FreezeTime(sql string, dialect string, executionTime string) (string, error) {
	sql, dialect, executionTime = validUTF8(sql), validUTF8(dialect), validUTF8(executionTime)
	return queryCommand(func() (any, error) { return freezeTime(sql, dialect, executionTime), nil })
}

// CTE is a named common table expression to prepend to a query.
type CTE struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// PrependCTEs adds the given CTEs to the front of a query's WITH clause (merging
// with any existing one, so existing CTEs can reference the prepended ones) and
// returns the rewritten SQL in the same dialect. It underpins connection-mode
// unit tests, which substitute the tables a query reads with inline fixture CTEs
// so the test runs as a single read-only SELECT with no DDL.
func (s *SQLParser) PrependCTEs(sql string, dialect string, ctes []CTE) (string, error) {
	sql, dialect = validUTF8(sql), validUTF8(dialect)
	var valid []CTE
	if ctes != nil {
		valid = make([]CTE, len(ctes))
		for i, c := range ctes {
			valid[i] = CTE{Name: validUTF8(c.Name), Query: validUTF8(c.Query)}
		}
	}
	return queryCommand(func() (any, error) { return addCTEs(sql, dialect, valid), nil })
}

func (s *SQLParser) IsSingleSelectQuery(sql string, dialect string) (bool, error) {
	var resp singleSelectResponse
	if err := run(&resp, func() (any, error) { return isSingleSelectQuery(validUTF8(sql), validUTF8(dialect)), nil }); err != nil {
		return false, err
	}
	if resp.Error != "" {
		return false, errors.New(resp.Error)
	}

	return resp.IsSingleSelect, nil
}

func (s *SQLParser) GetMissingDependenciesForAsset(asset *pipeline.Asset, pipeline *pipeline.Pipeline, renderer jinja.RendererInterface) ([]string, error) {
	return getMissingDependenciesForAsset(s, asset, pipeline, renderer)
}

func getMissingDependenciesForAsset(p Parser, asset *pipeline.Asset, pl *pipeline.Pipeline, renderer jinja.RendererInterface) ([]string, error) {
	if err := p.Start(); err != nil {
		return []string{}, errors.Wrap(err, "failed to start sql parser")
	}

	dialect, err := AssetTypeToDialect(asset.Type)
	if err != nil {
		return []string{}, nil //nolint:nilerr
	}

	renderedQ, err := renderer.Render(mergeMacrosWithQuery(asset.ExecutableFile.Content, pl.Macros))
	if err != nil {
		return []string{}, errors.New("failed to render the query before parsing the SQL")
	}

	tables, err := p.UsedTables(renderedQ, dialect)
	if err != nil {
		return []string{}, errors.Wrap(err, "failed to get used tables")
	}

	if len(tables) == 0 && len(asset.Upstreams) == 0 {
		return []string{}, nil
	}

	pipelineAssetNames := make(map[string]bool, len(pl.Assets))
	for _, a := range pl.Assets {
		pipelineAssetNames[strings.ToLower(a.Name)] = true
	}

	usedTableNameMap := make(map[string]string, len(tables))
	for _, table := range tables {
		usedTableNameMap[strings.ToLower(table)] = table
	}

	depsNameMap := make(map[string]string, len(asset.Upstreams))
	for _, upstream := range asset.Upstreams {
		if upstream.Type != "asset" {
			continue
		}

		depsNameMap[strings.ToLower(upstream.Value)] = upstream.Value
	}

	missingDependencies := make([]string, 0)
	for usedTable, actualReferenceName := range usedTableNameMap {
		if usedTable == asset.Name || actualReferenceName == asset.Name {
			continue
		}

		if _, ok := depsNameMap[usedTable]; ok {
			continue
		}

		if _, ok := pipelineAssetNames[usedTable]; !ok {
			continue
		}

		missingDependencies = append(missingDependencies, actualReferenceName)
	}

	return missingDependencies, nil
}

func mergeMacrosWithQuery(query string, macros []pipeline.Macro) string {
	if len(macros) == 0 {
		return query
	}

	var b strings.Builder
	for _, macro := range macros {
		b.WriteString(string(macro))
		b.WriteString("\n")
	}
	b.WriteString("\n")
	b.WriteString(query)

	return b.String()
}
