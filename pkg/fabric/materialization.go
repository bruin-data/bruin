package fabric

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

var matMap = pipeline.AssetMaterializationMap{
	pipeline.MaterializationTypeView: {
		pipeline.MaterializationStrategyNone:          viewMaterializer,
		pipeline.MaterializationStrategyAppend:        errorMaterializer,
		pipeline.MaterializationStrategyCreateReplace: viewMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:  errorMaterializer,
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildDeleteInsertQuery,
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
	},
}

func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        fullRefresh,
	}
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "CREATE OR ALTER VIEW " + QuoteIdentifier(asset.Name) + " AS\n" + strings.TrimSuffix(query, ";"), nil
}

// buildCreateReplaceQuery uses a temp table swap pattern because Fabric doesn't support CREATE OR REPLACE TABLE.
func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	_, baseName := splitSchemaName(asset.Name)
	targetFullName := asset.Name
	tempFullName := asset.Name + "__bruin_tmp"
	backupFullName := asset.Name + "__bruin_backup"

	backupBase := baseName + "__bruin_backup"
	targetBase := baseName

	tempName := QuoteIdentifier(tempFullName)
	backupName := QuoteIdentifier(backupFullName)

	query = strings.TrimSuffix(query, ";")

	queries := []string{
		"DROP TABLE IF EXISTS " + tempName,
		"DROP TABLE IF EXISTS " + backupName,
		"CREATE TABLE " + tempName + " AS\n" + query,
		"IF OBJECT_ID('" + targetFullName + "', 'U') IS NOT NULL BEGIN EXEC sp_rename '" + targetFullName + "', '" + backupBase + "' END",
		"EXEC sp_rename '" + tempFullName + "', '" + targetBase + "'",
		"DROP TABLE IF EXISTS " + backupName,
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return "INSERT INTO " + QuoteIdentifier(asset.Name) + "\n" + strings.TrimSuffix(query, ";"), nil
}

func buildDeleteInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", errors.New("delete+insert strategy requires columns to be defined")
	}

	pkCols := getPrimaryKeyColumns(asset)
	if len(pkCols) == 0 {
		return "", errors.New("delete+insert strategy requires primary key columns")
	}

	tableName := QuoteIdentifier(asset.Name)
	tempName := QuoteIdentifier(asset.Name + "__bruin_tmp")

	deleteCondition := buildDeleteCondition(pkCols, tableName, tempName)
	query = strings.TrimSuffix(query, ";")

	queries := []string{
		"DROP TABLE IF EXISTS " + tempName,
		"CREATE TABLE " + tempName + " AS\n" + query,
		"DELETE FROM " + tableName + " WHERE EXISTS (\n  SELECT 1 FROM " + tempName + " WHERE " + deleteCondition + "\n)",
		"INSERT INTO " + tableName + " SELECT * FROM " + tempName,
		"DROP TABLE IF EXISTS " + tempName,
	}

	return strings.Join(queries, ";\n") + ";", nil
}

// NativeMergeParameter opts a merge asset into a native T-SQL MERGE. Without it,
// merge keeps its original delete+insert behavior, which replaces matched rows
// entirely. The default stays delete+insert because switching it silently would
// stop updating every non-key column that lacks update_on_merge or merge_sql.
const NativeMergeParameter = "fabric_native_merge"

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	native, err := nativeMergeEnabled(asset)
	if err != nil {
		return "", err
	}
	if native {
		return buildNativeMergeQuery(asset, query)
	}

	// The delete+insert fallback has no merge match condition to attach the predicate to.
	if strings.TrimSpace(asset.Materialization.IncrementalPredicate) != "" {
		return "", fmt.Errorf("incremental_predicate on Fabric merge requires `parameters.%s: true`", NativeMergeParameter)
	}

	return buildDeleteInsertQuery(asset, query)
}

func nativeMergeEnabled(asset *pipeline.Asset) (bool, error) {
	raw, exists := asset.Parameters[NativeMergeParameter]
	if !exists {
		return false, nil
	}

	// GetString also fails for nil, list, and map values, which must not quietly
	// fall back to delete+insert.
	value, ok := asset.Parameters.GetString(NativeMergeParameter)
	if !ok {
		return false, fmt.Errorf("parameters.%s must be true or false, got %v", NativeMergeParameter, raw)
	}

	enabled, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("parameters.%s must be true or false, got %q", NativeMergeParameter, value)
	}
	return enabled, nil
}

// buildNativeMergeQuery stages the query in a table and merges it into the
// target. Staging keeps CTE-bodied queries working, since Fabric rejects a WITH
// clause inside MERGE ... USING (...).
func buildNativeMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	strategy := asset.Materialization.Strategy
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", strategy)
	}

	pkCols := getPrimaryKeyColumns(asset)
	if len(pkCols) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", strategy)
	}

	mergeColumns := ansisql.GetColumnsWithMergeLogic(asset)
	predicate := strings.TrimSpace(asset.Materialization.IncrementalPredicate)
	if predicate != "" && len(mergeColumns) == 0 {
		return "", errors.New("incremental_predicate on Fabric merge limits which matched rows are updated, so it requires at least one column with `update_on_merge` or `merge_sql`")
	}

	tableName := QuoteIdentifier(asset.Name)
	// Unique per run so parallel backfill partitions cannot replace each other's staged rows.
	tempName := QuoteIdentifier(asset.Name + "__bruin_tmp_" + helpers.PrefixGenerator())

	mergeLines := []string{
		"MERGE INTO " + tableName + " target",
		"USING " + tempName + " source",
		"ON " + buildDeleteCondition(pkCols, "target", "source"),
	}

	if len(mergeColumns) > 0 {
		updates := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			value := "source." + QuoteIdentifier(col.Name)
			if col.MergeSQL != "" {
				value = col.MergeSQL
			}
			updates = append(updates, fmt.Sprintf("target.%s = %s", QuoteIdentifier(col.Name), value))
		}

		whenMatched := "WHEN MATCHED"
		if predicate != "" {
			whenMatched += " AND (" + predicate + ")"
		}
		mergeLines = append(mergeLines, whenMatched+" THEN UPDATE SET "+strings.Join(updates, ", "))
	}

	columns := make([]string, 0, len(asset.Columns))
	values := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		columns = append(columns, QuoteIdentifier(col.Name))
		values = append(values, "source."+QuoteIdentifier(col.Name))
	}
	mergeLines = append(mergeLines, fmt.Sprintf("WHEN NOT MATCHED BY TARGET THEN INSERT (%s) VALUES (%s)", strings.Join(columns, ", "), strings.Join(values, ", ")))

	queries := make([]string, 0, 5)
	if createsMissingTable(asset) {
		// A MERGE into the new, empty table inserts every row by column name.
		queries = append(queries, fmt.Sprintf(
			"IF OBJECT_ID('%s', 'U') IS NULL\nBEGIN\n%s;\nEND",
			strings.ReplaceAll(asset.Name, "'", "''"),
			buildCreateTableStatement(asset),
		))
	}
	queries = append(
		queries,
		"CREATE TABLE "+tempName+" AS\n"+strings.TrimSuffix(strings.TrimSpace(query), ";"),
		buildDuplicateKeyGuard(asset.Name, pkCols, tempName),
		strings.Join(mergeLines, "\n"),
		"DROP TABLE IF EXISTS "+tempName,
	)

	return strings.Join(queries, ";\n") + ";", nil
}

// buildDuplicateKeyGuard fails the run when the staged rows repeat a primary
// key. MERGE would otherwise either fail with a generic "same row more than
// once" error, when the key already exists, or insert every copy, when it doesn't.
func buildDuplicateKeyGuard(assetName string, pkCols []string, tempName string) string {
	quotedKeys := make([]string, len(pkCols))
	for i, col := range pkCols {
		quotedKeys[i] = QuoteIdentifier(col)
	}

	message := fmt.Sprintf(
		"Fabric merge into %s failed: the query returned more than one row for the same primary key (%s). Deduplicate the query so that each key appears once.",
		assetName,
		strings.Join(pkCols, ", "),
	)
	// THROW treats % as a format character and the message is a string literal.
	message = strings.ReplaceAll(strings.ReplaceAll(message, "%", "%%"), "'", "''")

	return fmt.Sprintf(
		"IF EXISTS (SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*) > 1)\nBEGIN\nDROP TABLE IF EXISTS %s;\nTHROW 50000, N'%s', 1;\nEND",
		tempName,
		strings.Join(quotedKeys, ", "),
		tempName,
		message,
	)
}

// buildCreateTableStatement creates the target from the declared columns.
// Fabric rejects key constraints inside CREATE TABLE, so unlike the ddl
// strategy it leaves out the PRIMARY KEY clause.
func buildCreateTableStatement(asset *pipeline.Asset) string {
	columnDefs, _ := buildColumnDefinitions(asset)
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)", QuoteIdentifier(asset.Name), strings.Join(columnDefs, ",\n"))
}

// createsMissingTable reports whether a native merge creates its missing target
// from the declared columns. That applies only to assets with
// refresh_restricted, which a full refresh never creates, and a type on every
// declared column. The target is created before the query runs, so a query
// that reads its own target sees an empty table, and MERGE inserts by column
// name, so the declared order may differ from the query's. Strategies that copy
// staged rows by position would put values in the wrong columns, so they don't
// create missing targets.
func createsMissingTable(asset *pipeline.Asset) bool {
	return asset.RefreshRestricted != nil && *asset.RefreshRestricted && allColumnsTyped(asset)
}

func allColumnsTyped(asset *pipeline.Asset) bool {
	if len(asset.Columns) == 0 {
		return false
	}
	for _, col := range asset.Columns {
		if strings.TrimSpace(col.Type) == "" {
			return false
		}
	}
	return true
}

func buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	tableName := QuoteIdentifier(asset.Name)
	schemaName, _ := splitSchemaName(asset.Name)
	tempFullName := "__bruin_tmp_" + helpers.PrefixGenerator()
	if schemaName != "" {
		tempFullName = schemaName + "." + tempFullName
	}
	tempName := QuoteIdentifier(tempFullName)
	query = strings.TrimSuffix(query, ";")

	queries := []string{
		"CREATE TABLE " + tempName + " AS\n" + query,
		"TRUNCATE TABLE " + tableName,
		"INSERT INTO " + tableName + " SELECT * FROM " + tempName,
		"DROP TABLE IF EXISTS " + tempName,
	}
	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, _ string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	for _, col := range asset.Columns {
		if col.Type == "" {
			return "", fmt.Errorf("materialization strategy %s requires column %q to have a type", asset.Materialization.Strategy, col.Name)
		}
	}

	columnDefs, primaryKeys := buildColumnDefinitions(asset)
	if len(primaryKeys) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	return fmt.Sprintf(
		"IF OBJECT_ID('%s', 'U') IS NULL\nBEGIN\nCREATE TABLE %s (\n%s\n)\nEND;",
		strings.ReplaceAll(asset.Name, "'", "''"),
		QuoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	), nil
}

// buildColumnDefinitions returns a definition per column and the quoted names
// of the primary key columns. Callers must check that every column has a type.
func buildColumnDefinitions(asset *pipeline.Asset) ([]string, []string) {
	columnDefs := make([]string, 0, len(asset.Columns)+1)
	primaryKeys := make([]string, 0)
	for _, col := range asset.Columns {
		definition := fmt.Sprintf("    %s %s", QuoteIdentifier(col.Name), col.SQLType())
		if col.PrimaryKey || !col.Nullable.Bool() {
			definition += " NOT NULL"
		}
		columnDefs = append(columnDefs, definition)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, QuoteIdentifier(col.Name))
		}
	}

	return columnDefs, primaryKeys
}

func getPrimaryKeyColumns(asset *pipeline.Asset) []string {
	var pkCols []string
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}
	return pkCols
}

func buildDeleteCondition(pkCols []string, targetAlias, sourceAlias string) string {
	conditions := make([]string, len(pkCols))
	for i, col := range pkCols {
		conditions[i] = fmt.Sprintf("%s.%s = %s.%s", targetAlias, QuoteIdentifier(col), sourceAlias, QuoteIdentifier(col))
	}
	return strings.Join(conditions, " AND ")
}

func splitSchemaName(name string) (string, string) {
	if idx := strings.LastIndex(name, "."); idx != -1 {
		return name[:idx], name[idx+1:]
	}

	return "", name
}
