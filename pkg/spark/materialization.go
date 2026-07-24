package spark

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sail"
)

const scd2ValidUntil = "TIMESTAMP '9999-12-31 00:00:00'"

// NewMaterializer uses the Spark SQL materialization implementation shared
// with Sail and replaces the strategies that need Spark table-provider
// features, such as Iceberg partition specs, sort orders, and MERGE INTO.
func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	materializer := sail.NewMaterializer(fullRefresh)
	tableMaterializers := materializer.MaterializationMap[pipeline.MaterializationTypeTable]
	tableMaterializers[pipeline.MaterializationStrategyNone] = buildCreateReplaceQuery
	tableMaterializers[pipeline.MaterializationStrategyCreateReplace] = buildCreateReplaceQuery
	tableMaterializers[pipeline.MaterializationStrategyDeleteInsert] = quoteIncrementalKey(
		tableMaterializers[pipeline.MaterializationStrategyDeleteInsert],
	)
	tableMaterializers[pipeline.MaterializationStrategyTimeInterval] = quoteIncrementalKey(
		tableMaterializers[pipeline.MaterializationStrategyTimeInterval],
	)
	tableMaterializers[pipeline.MaterializationStrategyMerge] = buildMergeQuery
	tableMaterializers[pipeline.MaterializationStrategyDDL] = buildDDLQuery
	tableMaterializers[pipeline.MaterializationStrategySCD2ByColumn] = buildSCD2ByColumnQuery
	tableMaterializers[pipeline.MaterializationStrategySCD2ByTime] = buildSCD2ByTimeQuery
	return materializer
}

func quoteIncrementalKey(materializer pipeline.MaterializerFunc) pipeline.MaterializerFunc {
	return func(asset *pipeline.Asset, query string) (string, error) {
		if asset.Materialization.IncrementalKey == "" {
			return materializer(asset, query)
		}
		assetCopy := *asset
		assetCopy.Materialization = asset.Materialization
		assetCopy.Materialization.IncrementalKey = quoteIdentifier(asset.Materialization.IncrementalKey)
		return materializer(&assetCopy, query)
	}
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	switch asset.Materialization.Strategy {
	case pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnFullRefresh(asset, query)
	case pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimeFullRefresh(asset, query)
	default:
	}

	query = strings.TrimSuffix(query, ";")
	return buildReplaceTableQuery(
		asset,
		query,
		asset.Materialization.PartitionBy,
		asset.Materialization.ClusterBy,
	), nil
}

func buildReplaceTableQuery(asset *pipeline.Asset, selectQuery, partitionBy string, clusterBy []string) string {
	name := quoteIdentifier(asset.Name)
	lines := []string{
		"DROP TABLE IF EXISTS " + name + ";",
		"CREATE TABLE " + name,
	}
	if partitionBy != "" {
		lines = append(lines, "PARTITIONED BY ("+partitionBy+")")
	}
	lines = append(lines, "AS", strings.TrimSpace(selectQuery)+";")
	if len(clusterBy) > 0 {
		lines = append(lines, buildClusterQuery(asset.Name, clusterBy))
	}
	return strings.Join(lines, "\n")
}

func buildClusterQuery(tableName string, clusterBy []string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s WRITE ORDERED BY %s;",
		quoteIdentifier(tableName),
		strings.Join(clusterBy, ", "),
	)
}

func buildDDLQuery(asset *pipeline.Asset, _ string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	columnDefinitions := make([]string, 0, len(asset.Columns))
	for _, column := range asset.Columns {
		definition := fmt.Sprintf("    %s %s", quoteIdentifier(column.Name), column.SQLType())
		if column.Description != "" {
			description := strings.ReplaceAll(column.Description, "'", "''")
			definition += fmt.Sprintf(" COMMENT '%s'", description)
		}
		columnDefinitions = append(columnDefinitions, definition)
	}

	lines := []string{
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
			quoteIdentifier(asset.Name),
			strings.Join(columnDefinitions, ",\n"),
		),
	}
	if asset.Materialization.PartitionBy != "" {
		lines = append(lines, "PARTITIONED BY ("+asset.Materialization.PartitionBy+")")
	}
	lines[len(lines)-1] += ";"
	if len(asset.Materialization.ClusterBy) > 0 {
		lines = append(lines, buildClusterQuery(asset.Name, asset.Materialization.ClusterBy))
	}
	return strings.Join(lines, "\n"), nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	on := make([]string, 0, len(primaryKeys)+1)
	for _, key := range primaryKeys {
		quotedKey := quoteIdentifier(key)
		on = append(on, fmt.Sprintf("source.%s <=> target.%s", quotedKey, quotedKey))
	}
	on = ansisql.AddIncrementalPredicate(on, asset.Materialization.IncrementalPredicate)

	columns := asset.ColumnNames()
	quotedColumns := make([]string, len(columns))
	sourceColumns := make([]string, len(columns))
	for index, column := range columns {
		quotedColumns[index] = quoteIdentifier(column)
		sourceColumns[index] = "source." + quoteIdentifier(column)
	}

	lines := []string{
		fmt.Sprintf("MERGE INTO %s target", quoteIdentifier(asset.Name)),
		fmt.Sprintf("USING (%s) source", strings.TrimSuffix(query, ";")),
		"ON " + strings.Join(on, " AND "),
	}

	mergeColumns := ansisql.GetColumnsWithMergeLogic(asset)
	if len(mergeColumns) > 0 {
		updates := make([]string, 0, len(mergeColumns))
		for _, column := range mergeColumns {
			expression := "source." + quoteIdentifier(column.Name)
			if column.MergeSQL != "" {
				expression = column.MergeSQL
			}
			updates = append(updates, fmt.Sprintf("target.%s = %s", quoteIdentifier(column.Name), expression))
		}
		lines = append(lines, "WHEN MATCHED THEN UPDATE SET "+strings.Join(updates, ", "))
	}

	lines = append(
		lines,
		fmt.Sprintf(
			"WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
			strings.Join(quotedColumns, ", "),
			strings.Join(sourceColumns, ", "),
		),
	)

	return strings.Join(lines, "\n") + ";", nil
}

func buildSCD2ByColumnFullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys, err := validateSCD2Asset(asset, false)
	if err != nil {
		return "", err
	}

	validFrom := "CURRENT_TIMESTAMP()"
	if asset.Materialization.IncrementalKey != "" {
		validFrom = fmt.Sprintf(
			"CAST(src.%s AS TIMESTAMP)",
			quoteIdentifier(asset.Materialization.IncrementalKey),
		)
	}
	selectQuery := fmt.Sprintf(
		`SELECT
  %s AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		validFrom,
		scd2ValidUntil,
		strings.TrimSpace(strings.TrimSuffix(query, ";")),
	)
	partitionBy, clusterBy := scd2Layout(asset, primaryKeys)
	return buildReplaceTableQuery(asset, selectQuery, partitionBy, clusterBy), nil
}

func buildSCD2ByTimeFullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys, err := validateSCD2Asset(asset, true)
	if err != nil {
		return "", err
	}

	incrementalKey := quoteIdentifier(asset.Materialization.IncrementalKey)
	selectQuery := fmt.Sprintf(
		`SELECT
  CAST(src.%s AS TIMESTAMP) AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		incrementalKey,
		scd2ValidUntil,
		strings.TrimSpace(strings.TrimSuffix(query, ";")),
	)
	partitionBy, clusterBy := scd2Layout(asset, primaryKeys)
	return buildReplaceTableQuery(asset, selectQuery, partitionBy, clusterBy), nil
}

func scd2Layout(asset *pipeline.Asset, primaryKeys []string) (string, []string) {
	partitionBy := asset.Materialization.PartitionBy
	if partitionBy == "" {
		partitionBy = "days(_valid_from)"
	}

	clusterBy := asset.Materialization.ClusterBy
	if len(clusterBy) == 0 {
		clusterBy = append([]string{"_is_current"}, primaryKeys...)
	}
	return partitionBy, clusterBy
}

func validateSCD2Asset(asset *pipeline.Asset, requireIncrementalKey bool) ([]string, error) {
	if requireIncrementalKey && asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for scd2_by_time strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf(
			"materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}

	incrementalKeyFound := false
	for _, column := range asset.Columns {
		switch {
		case strings.EqualFold(column.Name, "_valid_from"),
			strings.EqualFold(column.Name, "_valid_until"),
			strings.EqualFold(column.Name, "_is_current"):
			return nil, fmt.Errorf("column name %s is reserved for SCD2 and cannot be used", column.Name)
		case requireIncrementalKey && strings.EqualFold(column.Name, asset.Materialization.IncrementalKey):
			incrementalKeyFound = true
			columnType := strings.ToLower(strings.TrimSpace(column.Type))
			if columnType != "timestamp" && columnType != "date" {
				return nil, errors.New("incremental_key must be TIMESTAMP or DATE in scd2_by_time strategy")
			}
		}
	}
	if requireIncrementalKey && !incrementalKeyFound {
		return nil, errors.New("incremental_key must reference a declared column in scd2_by_time strategy")
	}
	return primaryKeys, nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys, err := validateSCD2Asset(asset, false)
	if err != nil {
		return "", err
	}

	insertColumns := make([]string, 0, len(asset.Columns)+3)
	insertValues := make([]string, 0, len(asset.Columns)+3)
	sourceChanges := make([]string, 0, len(asset.Columns)-len(primaryKeys))
	targetChanges := make([]string, 0, len(asset.Columns)-len(primaryKeys))
	for _, column := range asset.Columns {
		quotedColumn := quoteIdentifier(column.Name)
		insertColumns = append(insertColumns, quotedColumn)
		insertValues = append(insertValues, "source."+quotedColumn)
		if !column.PrimaryKey {
			sourceChanges = append(
				sourceChanges,
				fmt.Sprintf("NOT (t1.%s <=> s1.%s)", quotedColumn, quotedColumn),
			)
			targetChanges = append(
				targetChanges,
				fmt.Sprintf("NOT (target.%s <=> source.%s)", quotedColumn, quotedColumn),
			)
		}
	}

	validFrom := "CURRENT_TIMESTAMP()"
	validUntil := "CURRENT_TIMESTAMP()"
	if asset.Materialization.IncrementalKey != "" {
		incrementalKey := quoteIdentifier(asset.Materialization.IncrementalKey)
		validFrom = "CAST(source." + incrementalKey + " AS TIMESTAMP)"
		validUntil = validFrom
	}
	insertColumns = append(insertColumns, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, validFrom, scd2ValidUntil, "TRUE")

	sourceChangeCondition := "FALSE"
	targetChangeCondition := "FALSE"
	if len(sourceChanges) > 0 {
		sourceChangeCondition = strings.Join(sourceChanges, " OR ")
		targetChangeCondition = strings.Join(targetChanges, " OR ")
	}

	return buildSCD2MergeQuery(
		asset,
		query,
		primaryKeys,
		sourceChangeCondition,
		targetChangeCondition,
		validUntil,
		insertColumns,
		insertValues,
	), nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys, err := validateSCD2Asset(asset, true)
	if err != nil {
		return "", err
	}

	insertColumns := make([]string, 0, len(asset.Columns)+3)
	insertValues := make([]string, 0, len(asset.Columns)+3)
	for _, column := range asset.Columns {
		quotedColumn := quoteIdentifier(column.Name)
		insertColumns = append(insertColumns, quotedColumn)
		insertValues = append(insertValues, "source."+quotedColumn)
	}

	incrementalKey := quoteIdentifier(asset.Materialization.IncrementalKey)
	sourceTimestamp := "CAST(s1." + incrementalKey + " AS TIMESTAMP)"
	targetTimestamp := "CAST(source." + incrementalKey + " AS TIMESTAMP)"
	insertColumns = append(insertColumns, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, targetTimestamp, scd2ValidUntil, "TRUE")

	return buildSCD2MergeQuery(
		asset,
		query,
		primaryKeys,
		"t1._valid_from < "+sourceTimestamp,
		"target._valid_from < "+targetTimestamp,
		targetTimestamp,
		insertColumns,
		insertValues,
	), nil
}

func buildSCD2MergeQuery(
	asset *pipeline.Asset,
	sourceQuery string,
	primaryKeys []string,
	sourceChangeCondition,
	targetChangeCondition,
	validUntil string,
	insertColumns,
	insertValues []string,
) string {
	primaryKeyJoin := make([]string, 0, len(primaryKeys))
	targetJoin := make([]string, 0, len(primaryKeys)+2)
	for _, primaryKey := range primaryKeys {
		quotedKey := quoteIdentifier(primaryKey)
		primaryKeyJoin = append(
			primaryKeyJoin,
			fmt.Sprintf("t1.%s <=> s1.%s", quotedKey, quotedKey),
		)
		targetJoin = append(
			targetJoin,
			fmt.Sprintf("target.%s <=> source.%s", quotedKey, quotedKey),
		)
	}
	targetJoin = append(targetJoin, "target._is_current", "source._is_current")

	return fmt.Sprintf(
		`MERGE INTO %s AS target
USING (
  WITH s1 AS (
    %s
  )
  SELECT s1.*, TRUE AS _is_current
  FROM s1
  UNION ALL
  SELECT s1.*, FALSE AS _is_current
  FROM s1
  JOIN %s AS t1
    ON %s
  WHERE t1._is_current AND (%s)
) AS source
ON %s
WHEN MATCHED AND (%s) THEN
  UPDATE SET
    target._valid_until = %s,
    target._is_current = FALSE
WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)
WHEN NOT MATCHED BY SOURCE AND target._is_current THEN
  UPDATE SET
    target._valid_until = CURRENT_TIMESTAMP(),
    target._is_current = FALSE;`,
		quoteIdentifier(asset.Name),
		strings.TrimSpace(strings.TrimSuffix(sourceQuery, ";")),
		quoteIdentifier(asset.Name),
		strings.Join(primaryKeyJoin, " AND "),
		sourceChangeCondition,
		strings.Join(targetJoin, " AND "),
		targetChangeCondition,
		validUntil,
		strings.Join(insertColumns, ", "),
		strings.Join(insertValues, ", "),
	)
}
