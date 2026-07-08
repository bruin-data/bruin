package doris

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
	dorisTableModelDuplicateKey = "duplicate_key"
	dorisTableModelUniqueKey    = "unique_key"
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
%s`, quoteIdentifier(replacementTable), buildDorisProperties(asset), strings.TrimSuffix(strings.TrimSpace(query), ";")),
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
		buildDorisProperties(asset),
		trimmedQuery,
	), nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if err := validateMergeAsset(asset); err != nil {
		return "", err
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	mergeColumns := ansisql.GetColumnsWithMergeLogic(asset)
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", quoteColumnName(key), quoteColumnName(key)))
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", quoteIdentifier(asset.Name)),
		fmt.Sprintf("USING (%s) source", strings.TrimSuffix(strings.TrimSpace(query), ";")),
		"ON " + strings.Join(on, " AND "),
	}

	if len(mergeColumns) > 0 {
		matchedUpdateStatements := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			if col.MergeSQL != "" {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = %s", quoteColumnName(col.Name), col.MergeSQL))
			} else {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", quoteColumnName(col.Name), quoteColumnName(col.Name)))
			}
		}

		mergeLines = append(mergeLines, "WHEN MATCHED THEN UPDATE SET "+strings.Join(matchedUpdateStatements, ", "))
	}

	sourceColumnValues := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		sourceColumnValues = append(sourceColumnValues, "source."+quoteColumnName(columnName))
	}

	mergeLines = append(
		mergeLines,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)", strings.Join(quoteColumnNames(columnNames), ", "), strings.Join(sourceColumnValues, ", ")),
	)

	return strings.Join(mergeLines, "\n") + ";", nil
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
		strings.TrimSpace(asset.Doris.TableModel) != "" ||
		len(asset.Doris.DistributedBy) > 0 ||
		asset.Doris.Buckets != 0
}

func validateMergeAsset(asset *pipeline.Asset) error {
	if _, err := dorisTableModel(asset); err != nil {
		return err
	}
	if len(asset.Columns) == 0 {
		return fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}
	if len(asset.ColumnNamesWithPrimaryKey()) == 0 {
		return fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}
	return validateColumnDefinitions(asset, dorisTableModelUniqueKey)
}

func buildCreateTableStatement(asset *pipeline.Asset, ifNotExists bool) (string, error) {
	model, err := dorisTableModel(asset)
	if err != nil {
		return "", err
	}
	if len(asset.Columns) == 0 {
		return "", errors.New("DDL strategy requires `columns` to be specified")
	}
	if err := validateColumnDefinitions(asset, model); err != nil {
		return "", err
	}

	orderedColumns := dorisColumnOrder(asset, model)
	keyColumns, err := dorisKeyColumns(asset, model)
	if err != nil {
		return "", err
	}
	distributedBy, err := dorisDistributedByColumns(asset, keyColumns)
	if err != nil {
		return "", err
	}
	buckets, err := dorisBuckets(asset)
	if err != nil {
		return "", err
	}

	columnDefs := make([]string, 0, len(orderedColumns))
	keyColumnSet := columnSet(keyColumns)
	for _, col := range orderedColumns {
		columnDefs = append(columnDefs, dorisColumnDefinition(col, model, keyColumnSet[col.Name]))
	}

	createPrefix := "CREATE TABLE "
	if ifNotExists {
		createPrefix += "IF NOT EXISTS "
	}

	keyClause := "DUPLICATE KEY"
	if model == dorisTableModelUniqueKey {
		keyClause = "UNIQUE KEY"
	}

	return fmt.Sprintf(
		`%s%s (
%s
)
%s(%s)
DISTRIBUTED BY HASH(%s) BUCKETS %d
PROPERTIES (%s)`,
		createPrefix,
		quoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
		keyClause,
		strings.Join(quoteColumnNames(keyColumns), ", "),
		strings.Join(quoteColumnNames(distributedBy), ", "),
		buckets,
		buildDorisProperties(asset),
	), nil
}

func dorisTableModel(asset *pipeline.Asset) (string, error) {
	configured := normalizeDorisTableModel(asset.Doris.TableModel)
	if configured == "" {
		if asset.Materialization.Strategy == pipeline.MaterializationStrategyMerge {
			return dorisTableModelUniqueKey, nil
		}
		return dorisTableModelDuplicateKey, nil
	}

	switch configured {
	case dorisTableModelDuplicateKey, dorisTableModelUniqueKey:
	default:
		return "", fmt.Errorf("unsupported Doris table_model %q: expected %q or %q", asset.Doris.TableModel, dorisTableModelDuplicateKey, dorisTableModelUniqueKey)
	}

	if asset.Materialization.Strategy == pipeline.MaterializationStrategyMerge && configured != dorisTableModelUniqueKey {
		return "", fmt.Errorf("materialization strategy %s requires Doris table_model %q", pipeline.MaterializationStrategyMerge, dorisTableModelUniqueKey)
	}

	return configured, nil
}

func normalizeDorisTableModel(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case "duplicate":
		return dorisTableModelDuplicateKey
	case "unique":
		return dorisTableModelUniqueKey
	default:
		return normalized
	}
}

func validateColumnDefinitions(asset *pipeline.Asset, model string) error {
	for _, col := range asset.Columns {
		if strings.TrimSpace(col.Name) == "" {
			return errors.New("doris table columns require a non-empty `name`")
		}
		if strings.TrimSpace(col.Type) == "" {
			return fmt.Errorf("doris table column %q requires the `type` field to be set", col.Name)
		}
	}

	if model == dorisTableModelUniqueKey && len(asset.ColumnNamesWithPrimaryKey()) == 0 {
		return fmt.Errorf("doris table_model %q requires at least one column with `primary_key: true`", dorisTableModelUniqueKey)
	}

	return nil
}

func dorisColumnOrder(asset *pipeline.Asset, model string) []pipeline.Column {
	if model != dorisTableModelUniqueKey {
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

func dorisKeyColumns(asset *pipeline.Asset, model string) ([]string, error) {
	if model == dorisTableModelUniqueKey {
		primaryKeys := asset.ColumnNamesWithPrimaryKey()
		if len(primaryKeys) == 0 {
			return nil, fmt.Errorf("doris table_model %q requires at least one column with `primary_key: true`", dorisTableModelUniqueKey)
		}
		return primaryKeys, nil
	}

	if len(asset.Columns) == 0 {
		return nil, errors.New("DDL strategy requires `columns` to be specified")
	}
	return []string{asset.Columns[0].Name}, nil
}

func dorisDistributedByColumns(asset *pipeline.Asset, keyColumns []string) ([]string, error) {
	if len(asset.Doris.DistributedBy) == 0 {
		return keyColumns, nil
	}

	for _, column := range asset.Doris.DistributedBy {
		if strings.TrimSpace(column) == "" {
			return nil, errors.New("doris distributed_by columns cannot be empty")
		}
	}

	return asset.Doris.DistributedBy, nil
}

func dorisBuckets(asset *pipeline.Asset) (int, error) {
	if asset.Doris.Buckets < 0 {
		return 0, errors.New("doris buckets must be greater than zero")
	}
	if asset.Doris.Buckets == 0 {
		return 1, nil
	}
	return asset.Doris.Buckets, nil
}

func dorisColumnDefinition(col pipeline.Column, model string, isKeyColumn bool) string {
	columnType := col.Type
	if isKeyColumn && strings.EqualFold(strings.TrimSpace(columnType), "STRING") {
		columnType = "VARCHAR(65533)"
	}

	definition := fmt.Sprintf("%s %s", quoteColumnName(col.Name), columnType)
	if isKeyColumn && model == dorisTableModelUniqueKey {
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

func buildDorisProperties(asset *pipeline.Asset) string {
	properties := map[string]string{
		"replication_num": "1",
	}
	for key, value := range asset.Doris.Properties {
		properties[key] = value
	}

	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	clauses := make([]string, 0, len(keys))
	for _, key := range keys {
		clauses = append(clauses, fmt.Sprintf("\"%s\" = \"%s\"", escapeDorisProperty(key), escapeDorisProperty(properties[key])))
	}
	return strings.Join(clauses, ", ")
}

func escapeDorisProperty(value string) string {
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

func replaceTableQuery(targetTable, replacementTable string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s REPLACE WITH TABLE %s PROPERTIES('swap' = 'false')",
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
