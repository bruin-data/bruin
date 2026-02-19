// Package rustparser provides a Go wrapper around the polyglot-sql Rust library
// for SQL parsing, table extraction, column lineage, and query transformation.
// It communicates with the Rust library via CGO using JSON as the interchange format.
package rustparser

/*
#include "polyglot_ffi.h"
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unsafe"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// Schema maps fully-qualified table names to their column→type mappings.
type Schema map[string]map[string]string

// UpstreamColumn represents a column reference from an upstream table.
type UpstreamColumn struct {
	Column string `json:"column"`
	Table  string `json:"table"`
}

// ColumnLineage represents the lineage of a single output column.
type ColumnLineage struct {
	Name     string           `json:"name"`
	Upstream []UpstreamColumn `json:"upstream"`
	Type     string           `json:"type"`
}

// Lineage represents the full lineage result for a query.
type Lineage struct {
	Columns            []ColumnLineage `json:"columns"`
	NonSelectedColumns []ColumnLineage `json:"non_selected_columns"`
	Errors             []string        `json:"errors"`
}

// RustSQLParser wraps the polyglot-sql Rust library via FFI.
// It is stateless and thread-safe — no Start()/Close() needed.
type RustSQLParser struct {
	MaxQueryLength int
}

// NewRustSQLParser creates a new Rust-based SQL parser.
func NewRustSQLParser() *RustSQLParser {
	return &RustSQLParser{MaxQueryLength: 10000}
}

// NewRustSQLParserWithConfig creates a new Rust-based SQL parser with custom config.
func NewRustSQLParserWithConfig(maxQueryLength int) *RustSQLParser {
	return &RustSQLParser{MaxQueryLength: maxQueryLength}
}

// callFFI is a helper to call a C function that returns a JSON string.
func callFFIResult(cStr *C.char) (string, error) {
	if cStr == nil {
		return "", fmt.Errorf("polyglot FFI returned null")
	}
	defer C.polyglot_free_string(cStr)
	return C.GoString(cStr), nil
}

// UsedTables extracts table names referenced in the SQL query.
func (p *RustSQLParser) UsedTables(sql, dialect string) ([]string, error) {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	cDialect := C.CString(dialect)
	defer C.free(unsafe.Pointer(cDialect))

	result, err := callFFIResult(C.polyglot_get_tables(cSQL, cDialect))
	if err != nil {
		return nil, err
	}

	var resp struct {
		Tables []string `json:"tables"`
		Error  string   `json:"error"`
	}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	sort.Strings(resp.Tables)
	return resp.Tables, nil
}

// RenameTables renames table references in the SQL query according to the mapping.
func (p *RustSQLParser) RenameTables(sql, dialect string, tableMapping map[string]string) (string, error) {
	reqJSON, err := json.Marshal(map[string]interface{}{
		"query":         sql,
		"dialect":       dialect,
		"table_mapping": tableMapping,
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	cReq := C.CString(string(reqJSON))
	defer C.free(unsafe.Pointer(cReq))

	result, err := callFFIResult(C.polyglot_rename_tables(cReq))
	if err != nil {
		return "", err
	}

	var resp struct {
		Query string `json:"query"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}
	if resp.Error != "" {
		return "", fmt.Errorf("%s", resp.Error)
	}

	return resp.Query, nil
}

// AddLimit adds a LIMIT clause to the SQL query.
func (p *RustSQLParser) AddLimit(sql string, limit int, dialect string) (string, error) {
	reqJSON, err := json.Marshal(map[string]interface{}{
		"query":   sql,
		"limit":   limit,
		"dialect": dialect,
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	cReq := C.CString(string(reqJSON))
	defer C.free(unsafe.Pointer(cReq))

	result, err := callFFIResult(C.polyglot_add_limit(cReq))
	if err != nil {
		return "", err
	}

	var resp struct {
		Query string `json:"query"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}
	if resp.Error != "" {
		return "", fmt.Errorf("%s", resp.Error)
	}

	return resp.Query, nil
}

// IsSingleSelectQuery checks if the SQL is a single SELECT statement.
func (p *RustSQLParser) IsSingleSelectQuery(sql, dialect string) (bool, error) {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	cDialect := C.CString(dialect)
	defer C.free(unsafe.Pointer(cDialect))

	result, err := callFFIResult(C.polyglot_is_single_select(cSQL, cDialect))
	if err != nil {
		return false, err
	}

	var resp struct {
		IsSingleSelect bool   `json:"is_single_select"`
		Error          string `json:"error"`
	}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	if resp.Error != "" {
		return false, fmt.Errorf("%s", resp.Error)
	}

	return resp.IsSingleSelect, nil
}

// ColumnLineage extracts column-level lineage from the SQL query.
func (p *RustSQLParser) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
	if len(sql) > p.MaxQueryLength {
		return &Lineage{
			Columns:            []ColumnLineage{},
			NonSelectedColumns: []ColumnLineage{},
			Errors:             []string{"query is too long skipping column lineage analysis"},
		}, nil
	}

	reqJSON, err := json.Marshal(map[string]interface{}{
		"query":   sql,
		"dialect": dialect,
		"schema":  schema,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	cReq := C.CString(string(reqJSON))
	defer C.free(unsafe.Pointer(cReq))

	result, err := callFFIResult(C.polyglot_column_lineage(cReq))
	if err != nil {
		return nil, err
	}

	var lineage Lineage
	if err := json.Unmarshal([]byte(result), &lineage); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &lineage, nil
}

// AssetTypeDialectMap maps Bruin asset types to SQL dialect names.
var AssetTypeDialectMap = map[pipeline.AssetType]string{
	pipeline.AssetTypeBigqueryQuery:   "bigquery",
	pipeline.AssetTypeSnowflakeQuery:  "snowflake",
	pipeline.AssetTypePostgresQuery:   "postgres",
	pipeline.AssetTypeMySQLQuery:      "mysql",
	pipeline.AssetTypeRedshiftQuery:   "redshift",
	pipeline.AssetTypeAthenaQuery:     "athena",
	pipeline.AssetTypeClickHouse:      "clickhouse",
	pipeline.AssetTypeDatabricksQuery: "databricks",
	pipeline.AssetTypeMsSQLQuery:      "tsql",
	pipeline.AssetTypeSynapseQuery:    "tsql",
	pipeline.AssetTypeDuckDBQuery:     "duckdb",
}

// AssetTypeToDialect converts a pipeline asset type to a SQL dialect string.
func AssetTypeToDialect(assetType pipeline.AssetType) (string, error) {
	dialect, ok := AssetTypeDialectMap[assetType]
	if !ok {
		return "", fmt.Errorf("unsupported asset type %s", assetType)
	}
	return dialect, nil
}

// GetMissingDependenciesForAsset finds tables used in the asset's SQL that are not
// declared as upstream dependencies but exist as assets in the pipeline.
func (p *RustSQLParser) GetMissingDependenciesForAsset(asset *pipeline.Asset, pl *pipeline.Pipeline, renderedQuery string) ([]string, error) {
	dialect, err := AssetTypeToDialect(asset.Type)
	if err != nil {
		return []string{}, nil //nolint:nilerr
	}

	tables, err := p.UsedTables(renderedQuery, dialect)
	if err != nil {
		return []string{}, fmt.Errorf("failed to get used tables: %w", err)
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
