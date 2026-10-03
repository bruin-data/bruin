package sqlparser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// SQLParser analyzes SQL with an in-process Go port of SQLGlot (see the sqlglot package in
// the gosqlx fork). It used to drive an embedded Python interpreter; the API is unchanged.
type SQLParser struct {
	tmpDir         string
	started        bool
	randomize      bool
	MaxQueryLength int

	// mutex serializes commands per instance, like the single-threaded Python process did.
	mutex      sync.Mutex
	startMutex sync.Mutex
}

func NewSQLParser(randomize bool) (*SQLParser, error) {
	return NewSQLParserWithConfig(randomize, 10000)
}

// NewSQLParserCached creates a SQLParser. It is kept for API compatibility: parsing no longer
// needs any extracted files, so cached and uncached parsers are equivalent.
func NewSQLParserCached() (*SQLParser, error) {
	return newSQLParserInternal("bruin-cli-embedded-cached", false, 10000), nil
}

func NewSQLParserWithConfig(randomize bool, maxQueryLength int) (*SQLParser, error) {
	tmpDirName := "bruin-cli-embedded_0"
	return newSQLParserInternal(tmpDirName, randomize, maxQueryLength), nil
}

func newSQLParserInternal(tmpDirName string, randomize bool, maxQueryLength int) *SQLParser {
	return &SQLParser{
		tmpDir:         tmpDirName,
		randomize:      randomize,
		MaxQueryLength: maxQueryLength,
	}
}

// Start marks the parser as started. There is no subprocess to launch anymore.
func (s *SQLParser) Start() error {
	s.startMutex.Lock()
	defer s.startMutex.Unlock()
	s.started = true
	return nil
}

type parserCommand struct {
	Command  string                 `json:"command"`
	Contents map[string]interface{} `json:"contents"`
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

func (s *SQLParser) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
	if len(sql) > s.MaxQueryLength {
		return &Lineage{
			Columns:            []ColumnLineage{},
			NonSelectedColumns: []ColumnLineage{},
			Errors:             []string{"query is too long skipping column lineage analysis"},
		}, nil
	}

	command := parserCommand{
		Command: "lineage",
		Contents: map[string]interface{}{
			"query":   sql,
			"dialect": dialect,
			"schema":  schema,
		},
	}

	resp, err := s.sendCommand(&command)
	if err != nil {
		return nil, err
	}

	var lineage Lineage
	err = json.Unmarshal([]byte(resp), &lineage)
	if err != nil {
		return nil, err
	}

	return &lineage, nil
}

func (s *SQLParser) UsedTables(sql, dialect string) ([]string, error) {
	err := s.Start()
	if err != nil {
		return nil, errors.Wrap(err, "failed to start sql parser")
	}

	command := parserCommand{
		Command: "get-tables",
		Contents: map[string]interface{}{
			"query":   sql,
			"dialect": dialect,
		},
	}

	resp, err := s.sendCommand(&command)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send command")
	}

	var tables struct {
		Tables []string `json:"tables"`
		Error  string   `json:"error"`
	}
	err = json.Unmarshal([]byte(resp), &tables)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response")
	}

	if tables.Error != "" {
		return nil, errors.New(tables.Error)
	}

	sort.Strings(tables.Tables)

	return tables.Tables, nil
}

// sendQueryCommand runs a parser command whose response is a {query, error}
// object and returns the resulting query string. It is the shared body for the
// SQL-rewriting verbs (rename/limit/transpile).
func (s *SQLParser) sendQueryCommand(command string, contents map[string]interface{}) (string, error) {
	if err := s.Start(); err != nil {
		return "", errors.Wrap(err, "failed to start sql parser")
	}

	responsePayload, err := s.sendCommand(&parserCommand{Command: command, Contents: contents})
	if err != nil {
		return "", errors.Wrap(err, "failed to send command")
	}

	var resp struct {
		Query string `json:"query"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(responsePayload), &resp); err != nil {
		return "", errors.Wrap(err, "failed to unmarshal response")
	}
	if resp.Error != "" {
		return "", errors.New(resp.Error)
	}
	return resp.Query, nil
}

func (s *SQLParser) RenameTables(sql string, dialect string, tableMapping map[string]string) (string, error) {
	return s.sendQueryCommand("replace-table-references", map[string]interface{}{
		"query":         sql,
		"dialect":       dialect,
		"table_mapping": tableMapping,
	})
}

// HoistDeclares moves top-level declarations ahead of other statements without
// regenerating their SQL. On failure it returns the original input.
func (s *SQLParser) HoistDeclares(sql string, assetType pipeline.AssetType) (string, error) {
	dialect, err := AssetTypeToDialect(assetType)
	if err != nil {
		return sql, err
	}
	query, err := s.sendQueryCommand("hoist-declares", map[string]interface{}{
		"query":   sql,
		"dialect": dialect,
	})
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
	if err := s.Start(); err != nil {
		return queries, errors.Wrap(err, "failed to start sql parser")
	}
	payload, err := s.sendCommand(&parserCommand{
		Command:  "hoist-declares-list",
		Contents: map[string]interface{}{"queries": queries, "dialect": dialect},
	})
	if err != nil {
		return queries, errors.Wrap(err, "failed to hoist declares list")
	}
	var resp struct {
		Queries []string `json:"queries"`
		Error   string   `json:"error"`
	}
	if err := json.Unmarshal([]byte(payload), &resp); err != nil {
		return queries, errors.Wrap(err, "failed to unmarshal response")
	}
	if resp.Error != "" {
		return queries, errors.New(resp.Error)
	}
	return resp.Queries, nil
}

func (s *SQLParser) sendCommand(pc *parserCommand) (string, error) {
	// Round-trip through JSON so that handlers observe exactly what the Python process received
	// (map key order, null for nil maps/slices, numbers as float64).
	payload, err := json.Marshal(pc)
	if err != nil {
		return "", err
	}
	var decoded parserCommand
	dec := json.NewDecoder(bytes.NewReader(payload))
	if err := dec.Decode(&decoded); err != nil {
		return "", err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return dispatch(&decoded) + "\n", nil
}

func (s *SQLParser) Close() error {
	s.startMutex.Lock()
	defer s.startMutex.Unlock()
	s.started = false
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
	return s.sendQueryCommand("add-limit", map[string]interface{}{
		"query":   sql,
		"limit":   limit,
		"dialect": dialect,
	})
}

// ExtractSelect reduces an asset statement to the SELECT that produces its
// rows. A CREATE OR REPLACE VIEW / CTAS / INSERT ... SELECT is unwrapped to its
// inner SELECT, and a statement that is already a SELECT (with or without a
// WITH clause) is returned unchanged. This lets a unit test exercise the read
// logic of a `materialization: none` (full-DDL) asset without issuing the DDL,
// keeping the test a single read-only SELECT. It errors when the statement has
// no SELECT to test (e.g. a CREATE TABLE with a column list).
func (s *SQLParser) ExtractSelect(sql string, dialect string) (string, error) {
	return s.sendQueryCommand("extract-select", map[string]interface{}{
		"query":   sql,
		"dialect": dialect,
	})
}

// SelectFromCTE rewrites a query so it returns all rows of one of its own named
// CTEs (WITH … SELECT * FROM <cteName>), keeping the other CTEs in place. It
// lets a unit test assert the output of an intermediate CTE. Errors if the
// query has no WITH clause or no CTE with that name.
func (s *SQLParser) SelectFromCTE(sql string, dialect string, cteName string) (string, error) {
	return s.sendQueryCommand("select-cte", map[string]interface{}{
		"query":    sql,
		"dialect":  dialect,
		"cte_name": cteName,
	})
}

// FreezeTime replaces CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME in a query
// with literals fixed at executionTime, so a unit test of a time-dependent
// asset is deterministic instead of reading the warehouse clock.
func (s *SQLParser) FreezeTime(sql string, dialect string, executionTime string) (string, error) {
	return s.sendQueryCommand("freeze-time", map[string]interface{}{
		"query":          sql,
		"dialect":        dialect,
		"execution_time": executionTime,
	})
}

// CTE is a named common table expression to prepend to a query. The JSON tags
// match the keys the parser's add-ctes verb reads, so a []CTE serializes
// directly onto the wire.
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
	return s.sendQueryCommand("add-ctes", map[string]interface{}{
		"query":   sql,
		"dialect": dialect,
		"ctes":    ctes,
	})
}

func (s *SQLParser) IsSingleSelectQuery(sql string, dialect string) (bool, error) {
	err := s.Start()
	if err != nil {
		return false, errors.Wrap(err, "failed to start sql parser")
	}

	command := parserCommand{
		Command: "is-single-select",
		Contents: map[string]interface{}{
			"query":   sql,
			"dialect": dialect,
		},
	}

	responsePayload, err := s.sendCommand(&command)
	if err != nil {
		return false, errors.Wrap(err, "failed to send command")
	}

	var resp struct {
		IsSingleSelect bool   `json:"is_single_select"`
		Error          string `json:"error"`
	}
	err = json.Unmarshal([]byte(responsePayload), &resp)
	if err != nil {
		return false, errors.Wrap(err, "failed to unmarshal response")
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
