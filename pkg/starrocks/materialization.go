package starrocks

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

const (
	starRocksTableModelDuplicateKey = "duplicate_key"
	starRocksTableModelUniqueKey    = "unique_key"
	starRocksTableModelPrimaryKey   = "primary_key"
)

func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        fullRefresh,
	}
}

var matMap = pipeline.AssetMaterializationMap{
	pipeline.MaterializationTypeView: {
		pipeline.MaterializationStrategyNone:          viewMaterializer,
		pipeline.MaterializationStrategyAppend:        errorMaterializer,
		pipeline.MaterializationStrategyCreateReplace: errorMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:  errorMaterializer,
		pipeline.MaterializationStrategyMerge:         errorMaterializer,
		pipeline.MaterializationStrategyDDL:           errorMaterializer,
		pipeline.MaterializationStrategySCD2ByColumn:  errorMaterializer,
		pipeline.MaterializationStrategySCD2ByTime:    errorMaterializer,
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildIncrementalQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   errorMaterializer,
		pipeline.MaterializationStrategySCD2ByTime:     errorMaterializer,
	},
}

func errorMaterializer(asset *pipeline.Asset, _ string) (string, error) {
	return "", fmt.Errorf(
		"materialization strategy %s is not supported for materialization type %s and asset type %s",
		asset.Materialization.Strategy,
		asset.Materialization.Type,
		asset.Type,
	)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf(
		"DROP VIEW IF EXISTS %s;\nCREATE VIEW %s AS\n%s;",
		quoteIdentifier(asset.Name),
		quoteIdentifier(asset.Name),
		strings.TrimSuffix(strings.TrimSpace(query), ";"),
	), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf(
		"INSERT INTO %s %s;",
		quoteIdentifier(asset.Name),
		strings.TrimSuffix(strings.TrimSpace(query), ";"),
	), nil
}

func buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", pipeline.MaterializationStrategyDeleteInsert)
	}

	trimmedQuery := strings.TrimSuffix(strings.TrimSpace(query), ";")
	runID := temporaryTableRunID()
	newRowsTable := temporaryTableName(asset.Name, runID, "new")
	replacementTable := temporaryTableName(asset.Name, runID, "replacement")
	incrementalKey := quoteColumnName(asset.Materialization.IncrementalKey)
	targetAlias := quoteColumnName("target")
	newRowsAlias := quoteColumnName("new_rows")
	nullSafeJoin := fmt.Sprintf(
		"(%s.%s = %s.%s OR (%s.%s IS NULL AND %s.%s IS NULL))",
		targetAlias,
		incrementalKey,
		newRowsAlias,
		incrementalKey,
		targetAlias,
		incrementalKey,
		newRowsAlias,
		incrementalKey,
	)

	queries := []string{
		"DROP TABLE IF EXISTS " + quoteIdentifier(newRowsTable),
		fmt.Sprintf(`CREATE TABLE %s
PROPERTIES ("replication_num" = "1")
AS
%s`, quoteIdentifier(newRowsTable), trimmedQuery),
		"DROP TABLE IF EXISTS " + quoteIdentifier(replacementTable),
		fmt.Sprintf(
			`CREATE TABLE %s
PROPERTIES ("replication_num" = "1")
AS
SELECT %s.*
FROM %s AS %s
WHERE NOT EXISTS (
  SELECT 1
  FROM %s AS %s
  WHERE %s
)
UNION ALL
SELECT * FROM %s`,
			quoteIdentifier(replacementTable),
			targetAlias,
			quoteIdentifier(asset.Name),
			targetAlias,
			quoteIdentifier(newRowsTable),
			newRowsAlias,
			nullSafeJoin,
			quoteIdentifier(newRowsTable),
		),
		replaceTableQuery(asset.Name, replacementTable),
		"DROP TABLE IF EXISTS " + quoteIdentifier(newRowsTable),
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	runID := temporaryTableRunID()
	replacementTable := temporaryTableName(asset.Name, runID, "replacement")
	queries := []string{
		"DROP TABLE IF EXISTS " + quoteIdentifier(replacementTable),
		fmt.Sprintf(`CREATE TABLE %s
PROPERTIES (%s)
AS
%s`, quoteIdentifier(replacementTable), buildStarRocksProperties(asset), strings.TrimSuffix(strings.TrimSpace(query), ";")),
		createTableLikeQuery(asset.Name, replacementTable),
		replaceTableQuery(asset.Name, replacementTable),
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	trimmedQuery := strings.TrimSuffix(strings.TrimSpace(query), ";")
	if requiresTypedCreateTable(asset) {
		createTable, err := buildCreateTableStatement(asset, false)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf(
			`DROP TABLE IF EXISTS %s;
%s;
INSERT INTO %s (%s)
%s;`,
			quoteIdentifier(asset.Name),
			createTable,
			quoteIdentifier(asset.Name),
			strings.Join(quoteColumnNames(asset.ColumnNames()), ", "),
			trimmedQuery,
		), nil
	}

	return fmt.Sprintf(
		`DROP TABLE IF EXISTS %s;
CREATE TABLE %s
PROPERTIES (%s)
AS
%s;`,
		quoteIdentifier(asset.Name),
		quoteIdentifier(asset.Name),
		buildStarRocksProperties(asset),
		trimmedQuery,
	), nil
}

// buildMergeQuery upserts into a StarRocks PRIMARY KEY table. StarRocks has no
// MERGE INTO statement; instead its primary-key model replaces rows whose key
// matches an incoming row and inserts the rest, so a plain INSERT is the
// idiomatic merge. The target table is created if missing so first runs work.
func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if err := validateMergeAsset(asset); err != nil {
		return "", err
	}

	for _, col := range ansisql.GetColumnsWithMergeLogic(asset) {
		if col.MergeSQL != "" {
			return "", fmt.Errorf(
				"StarRocks merge uses primary-key upsert and does not support per-column merge expressions (column %q); encode the logic in the asset query instead",
				col.Name,
			)
		}
	}

	createTable, err := buildCreateTableStatement(asset, true)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		`%s;
INSERT INTO %s (%s)
%s;`,
		createTable,
		quoteIdentifier(asset.Name),
		strings.Join(quoteColumnNames(asset.ColumnNames()), ", "),
		strings.TrimSuffix(strings.TrimSpace(query), ";"),
	), nil
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp &&
		asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return "", errors.New("time_granularity must be either 'date' or 'timestamp'")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	queries := []string{
		fmt.Sprintf(
			"DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'",
			quoteIdentifier(asset.Name),
			quoteColumnName(asset.Materialization.IncrementalKey),
			startVar,
			endVar,
		),
		fmt.Sprintf("INSERT INTO %s %s", quoteIdentifier(asset.Name), strings.TrimSuffix(strings.TrimSpace(query), ";")),
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, _ string) (string, error) {
	statement, err := buildCreateTableStatement(asset, true)
	if err != nil {
		return "", err
	}

	return statement + ";", nil
}

func requiresTypedCreateTable(asset *pipeline.Asset) bool {
	return asset.Materialization.Strategy == pipeline.MaterializationStrategyMerge ||
		strings.TrimSpace(asset.StarRocks.TableModel) != "" ||
		len(asset.StarRocks.DistributedBy) > 0 ||
		len(asset.StarRocks.PartitionBy) > 0 ||
		asset.StarRocks.Buckets != 0
}

func validateMergeAsset(asset *pipeline.Asset) error {
	if _, err := starRocksTableModel(asset); err != nil {
		return err
	}
	if len(asset.Columns) == 0 {
		return fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}
	if len(asset.ColumnNamesWithPrimaryKey()) == 0 {
		return fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}
	return validateColumnDefinitions(asset, starRocksTableModelPrimaryKey)
}

func buildCreateTableStatement(asset *pipeline.Asset, ifNotExists bool) (string, error) {
	model, err := starRocksTableModel(asset)
	if err != nil {
		return "", err
	}
	if len(asset.Columns) == 0 {
		return "", errors.New("DDL strategy requires `columns` to be specified")
	}
	if err := validateColumnDefinitions(asset, model); err != nil {
		return "", err
	}

	orderedColumns := starRocksColumnOrder(asset, model)
	keyColumns, err := starRocksKeyColumns(asset, model)
	if err != nil {
		return "", err
	}
	distributedBy, err := starRocksDistributedByColumns(asset, keyColumns)
	if err != nil {
		return "", err
	}
	buckets, err := starRocksBuckets(asset)
	if err != nil {
		return "", err
	}
	partitionBy, err := starRocksPartitionByColumns(asset)
	if err != nil {
		return "", err
	}

	columnDefs := make([]string, 0, len(orderedColumns))
	keyColumnSet := columnSet(keyColumns)
	for _, col := range orderedColumns {
		columnDefs = append(columnDefs, starRocksColumnDefinition(col, model, keyColumnSet[col.Name]))
	}

	createPrefix := "CREATE TABLE "
	if ifNotExists {
		createPrefix += "IF NOT EXISTS "
	}

	partitionClause := ""
	if len(partitionBy) > 0 {
		partitionClause = fmt.Sprintf("PARTITION BY (%s)\n", strings.Join(quoteColumnNames(partitionBy), ", "))
	}

	return fmt.Sprintf(
		`%s%s (
%s
)
%s(%s)
%sDISTRIBUTED BY HASH(%s) BUCKETS %d
PROPERTIES (%s)`,
		createPrefix,
		quoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
		starRocksKeyClause(model),
		strings.Join(quoteColumnNames(keyColumns), ", "),
		partitionClause,
		strings.Join(quoteColumnNames(distributedBy), ", "),
		buckets,
		buildStarRocksProperties(asset),
	), nil
}

func starRocksKeyClause(model string) string {
	switch model {
	case starRocksTableModelUniqueKey:
		return "UNIQUE KEY"
	case starRocksTableModelPrimaryKey:
		return "PRIMARY KEY"
	default:
		return "DUPLICATE KEY"
	}
}

func starRocksTableModel(asset *pipeline.Asset) (string, error) {
	configured := normalizeStarRocksTableModel(asset.StarRocks.TableModel)
	if configured == "" {
		if asset.Materialization.Strategy == pipeline.MaterializationStrategyMerge {
			return starRocksTableModelPrimaryKey, nil
		}
		return starRocksTableModelDuplicateKey, nil
	}

	switch configured {
	case starRocksTableModelDuplicateKey, starRocksTableModelUniqueKey, starRocksTableModelPrimaryKey:
	default:
		return "", fmt.Errorf("unsupported StarRocks table_model %q: expected %q, %q or %q", asset.StarRocks.TableModel, starRocksTableModelDuplicateKey, starRocksTableModelUniqueKey, starRocksTableModelPrimaryKey)
	}

	if asset.Materialization.Strategy == pipeline.MaterializationStrategyMerge && configured != starRocksTableModelPrimaryKey {
		return "", fmt.Errorf("materialization strategy %s requires StarRocks table_model %q", pipeline.MaterializationStrategyMerge, starRocksTableModelPrimaryKey)
	}

	return configured, nil
}

func normalizeStarRocksTableModel(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case "duplicate":
		return starRocksTableModelDuplicateKey
	case "unique":
		return starRocksTableModelUniqueKey
	case "primary":
		return starRocksTableModelPrimaryKey
	default:
		return normalized
	}
}

// modelUsesPrimaryKeyColumns reports whether the model keys off the columns
// flagged with `primary_key: true` (unique and primary key models) rather than
// defaulting to the first column (duplicate key model).
func modelUsesPrimaryKeyColumns(model string) bool {
	return model == starRocksTableModelUniqueKey || model == starRocksTableModelPrimaryKey
}

func validateColumnDefinitions(asset *pipeline.Asset, model string) error {
	for _, col := range asset.Columns {
		if strings.TrimSpace(col.Name) == "" {
			return errors.New("starrocks table columns require a non-empty `name`")
		}
		if strings.TrimSpace(col.Type) == "" {
			return fmt.Errorf("starrocks table column %q requires the `type` field to be set", col.Name)
		}
	}

	if modelUsesPrimaryKeyColumns(model) && len(asset.ColumnNamesWithPrimaryKey()) == 0 {
		return fmt.Errorf("starrocks table_model %q requires at least one column with `primary_key: true`", model)
	}

	return nil
}

func starRocksColumnOrder(asset *pipeline.Asset, model string) []pipeline.Column {
	if !modelUsesPrimaryKeyColumns(model) {
		return asset.Columns
	}

	ordered := make([]pipeline.Column, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			ordered = append(ordered, col)
		}
	}
	for _, col := range asset.Columns {
		if !col.PrimaryKey {
			ordered = append(ordered, col)
		}
	}
	return ordered
}

func starRocksKeyColumns(asset *pipeline.Asset, model string) ([]string, error) {
	if modelUsesPrimaryKeyColumns(model) {
		primaryKeys := asset.ColumnNamesWithPrimaryKey()
		if len(primaryKeys) == 0 {
			return nil, fmt.Errorf("starrocks table_model %q requires at least one column with `primary_key: true`", model)
		}
		return primaryKeys, nil
	}

	if len(asset.Columns) == 0 {
		return nil, errors.New("DDL strategy requires `columns` to be specified")
	}
	return []string{asset.Columns[0].Name}, nil
}

func starRocksDistributedByColumns(asset *pipeline.Asset, keyColumns []string) ([]string, error) {
	if len(asset.StarRocks.DistributedBy) == 0 {
		return keyColumns, nil
	}

	for _, column := range asset.StarRocks.DistributedBy {
		if strings.TrimSpace(column) == "" {
			return nil, errors.New("starrocks distributed_by columns cannot be empty")
		}
	}

	return asset.StarRocks.DistributedBy, nil
}

func starRocksPartitionByColumns(asset *pipeline.Asset) ([]string, error) {
	if len(asset.StarRocks.PartitionBy) == 0 {
		return nil, nil
	}

	for _, column := range asset.StarRocks.PartitionBy {
		if strings.TrimSpace(column) == "" {
			return nil, errors.New("starrocks partition_by columns cannot be empty")
		}
	}

	return asset.StarRocks.PartitionBy, nil
}

func starRocksBuckets(asset *pipeline.Asset) (int, error) {
	if asset.StarRocks.Buckets < 0 {
		return 0, errors.New("starrocks buckets must be greater than zero")
	}
	if asset.StarRocks.Buckets == 0 {
		return 1, nil
	}
	return asset.StarRocks.Buckets, nil
}

func starRocksColumnDefinition(col pipeline.Column, model string, isKeyColumn bool) string {
	columnType := col.Type
	if isKeyColumn && strings.EqualFold(strings.TrimSpace(columnType), "STRING") {
		columnType = "VARCHAR(65533)"
	}

	definition := fmt.Sprintf("%s %s", quoteColumnName(col.Name), columnType)
	if isKeyColumn && modelUsesPrimaryKeyColumns(model) {
		definition += " NOT NULL"
	} else if col.Nullable.Value != nil && !*col.Nullable.Value {
		definition += " NOT NULL"
	}

	if col.Description != "" {
		comment := strings.ReplaceAll(col.Description, `'`, `''`)
		definition += fmt.Sprintf(" COMMENT '%s'", comment)
	}

	return definition
}

func buildStarRocksProperties(asset *pipeline.Asset) string {
	properties := map[string]string{
		"replication_num": "1",
	}
	for key, value := range asset.StarRocks.Properties {
		properties[key] = value
	}

	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	clauses := make([]string, 0, len(keys))
	for _, key := range keys {
		clauses = append(clauses, fmt.Sprintf("\"%s\" = \"%s\"", escapeStarRocksProperty(key), escapeStarRocksProperty(properties[key])))
	}
	return strings.Join(clauses, ", ")
}

func escapeStarRocksProperty(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func columnSet(columns []string) map[string]bool {
	set := make(map[string]bool, len(columns))
	for _, column := range columns {
		set[column] = true
	}
	return set
}

func quoteColumnNames(names []string) []string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, quoteColumnName(name))
	}
	return quoted
}

func quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		quoted = append(quoted, quoteColumnName(part))
	}

	return strings.Join(quoted, ".")
}

func quoteColumnName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// replaceTableQuery atomically swaps the target with a staged replacement.
// StarRocks exposes this via `ALTER TABLE ... SWAP WITH ...` (unlike Doris,
// which uses `REPLACE WITH TABLE ... PROPERTIES('swap'='false')`).
func replaceTableQuery(targetTable, replacementTable string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s SWAP WITH %s",
		quoteIdentifier(targetTable),
		quoteColumnName(lastIdentifierPart(replacementTable)),
	)
}

func createTableLikeQuery(targetTable, sourceTable string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s LIKE %s",
		quoteIdentifier(targetTable),
		quoteIdentifier(sourceTable),
	)
}

func renameTableQuery(sourceTable, targetTable string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s RENAME %s",
		quoteIdentifier(sourceTable),
		quoteColumnName(lastIdentifierPart(targetTable)),
	)
}

func lastIdentifierPart(identifier string) string {
	parts := strings.Split(identifier, ".")
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.TrimSpace(parts[i])
		if part != "" {
			return part
		}
	}
	return identifier
}

func temporaryTableRunID() string {
	if flag.Lookup("test.v") != nil {
		return "abcefghi"
	}

	var bytes [4]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return hex.EncodeToString(bytes[:])
	}

	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func temporaryTableName(assetName, runID, suffix string) string {
	parts := strings.Split(assetName, ".")
	tableName := "asset"
	if len(parts) > 0 && strings.TrimSpace(parts[len(parts)-1]) != "" {
		tableName = parts[len(parts)-1]
	}

	var sanitized strings.Builder
	for _, r := range tableName {
		switch {
		case r >= 'a' && r <= 'z':
			sanitized.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			sanitized.WriteRune(r)
		case r >= '0' && r <= '9':
			sanitized.WriteRune(r)
		default:
			sanitized.WriteRune('_')
		}
	}

	base := strings.Trim(sanitized.String(), "_")
	if base == "" {
		base = "asset"
	}

	prefix := "__bruin_tmp_"
	maxBaseLength := 64 - len(prefix) - len(runID) - len(suffix) - 2
	if len(base) > maxBaseLength {
		base = base[:maxBaseLength]
	}

	parts[len(parts)-1] = prefix + base + "_" + runID + "_" + suffix
	return strings.Join(parts, ".")
}
