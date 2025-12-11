package bigquery

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

var matMap = pipeline.AssetMaterializationMap{
	pipeline.MaterializationTypeView: {
		pipeline.MaterializationStrategyNone:          viewMaterializer,
		pipeline.MaterializationStrategyAppend:        errorMaterializer,
		pipeline.MaterializationStrategyCreateReplace: errorMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:  errorMaterializer,
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildIncrementalQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          mergeMaterializer,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2QueryByTime,
	},
}

func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        fullRefresh,
		ForceDDL:           false,
	}
}

func NewDDLMaterializer() *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        false,
		ForceDDL:           true,
	}
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query), nil
}

func buildTruncateInsertQuery(task *pipeline.Asset, query string) (string, error) {
	// BigQuery doesn't support transactions, so we execute as separate statements
	queries := []string{
		"TRUNCATE TABLE " + task.Name,
		fmt.Sprintf("INSERT INTO %s %s", task.Name, strings.TrimSuffix(query, ";")),
	}
	return strings.Join(queries, ";\n"), nil
}

func mergeMaterializer(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	mergeColumns := ansisql.GetColumnsWithMergeLogic(asset)
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("(source.%s = target.%s OR (source.%s IS NULL and target.%s IS NULL))", key, key, key, key))
	}
	onQuery := strings.Join(on, " AND ")

	allColumnValues := strings.Join(columnNames, ", ")

	whenMatchedThenQuery := ""

	if len(mergeColumns) > 0 {
		matchedUpdateStatements := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			if col.MergeSQL != "" {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("target.%s = %s", col.Name, col.MergeSQL))
			} else {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("target.%s = source.%s", col.Name, col.Name))
			}
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE %s target", asset.Name),
		fmt.Sprintf("USING (%s) source", strings.TrimSuffix(query, ";")),
		fmt.Sprintf("ON (%s)", onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, allColumnValues),
	}

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", asset.Name, query), nil
}

func buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", mat.Strategy)
	}

	foundCol := asset.GetColumnWithName(mat.IncrementalKey)
	if foundCol == nil || foundCol.Type == "" || foundCol.Type == "UNKNOWN" {
		return buildIncrementalQueryWithoutTempVariable(asset, query)
	}

	randPrefix := helpers.PrefixGenerator()
	tempTableName := "__bruin_tmp_" + randPrefix

	declaredVarName := "distinct_keys_" + randPrefix
	queries := []string{
		fmt.Sprintf("DECLARE %s array<%s>", declaredVarName, foundCol.Type),
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, strings.TrimSuffix(query, ";")),
		fmt.Sprintf("SET %s = (SELECT array_agg(distinct %s) FROM %s)", declaredVarName, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("DELETE FROM %s WHERE %s in unnest(%s)", asset.Name, mat.IncrementalKey, declaredVarName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", asset.Name, tempTableName),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildIncrementalQueryWithoutTempVariable(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, strings.TrimSuffix(query, ";")),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", asset.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", asset.Name, tempTableName),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	switch {
	case asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimefullRefresh(asset, query)
	case asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnfullRefresh(asset, query)
	default:
		partitionClause := ""

		if mat.PartitionBy != "" {
			partitionClause = "PARTITION BY " + mat.PartitionBy
		}

		clusterByClause := ""
		if len(mat.ClusterBy) > 0 {
			clusterByClause = "CLUSTER BY " + strings.Join(mat.ClusterBy, ", ")
		}
		return fmt.Sprintf("CREATE OR REPLACE TABLE %s %s %s AS\n%s", asset.Name, partitionClause, clusterByClause, query), nil
	}
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}

	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp || asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return "", errors.New("time_granularity must be either 'date', or 'timestamp'")
	}
	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'`,
			asset.Name,
			asset.Materialization.IncrementalKey,
			startVar,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			asset.Name,
			strings.TrimSuffix(query, ";")),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	primaryKeys := []string{}

	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)

		if col.Description != "" {
			def += fmt.Sprintf(` OPTIONS(description=%q)`, col.Description)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		columnDefs = append(columnDefs, def)
	}

	if len(primaryKeys) > 0 {
		primaryKeyClause := fmt.Sprintf("PRIMARY KEY (%s) NOT ENFORCED", strings.Join(primaryKeys, ", "))
		columnDefs = append(columnDefs, primaryKeyClause)
	}

	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		asset.Name,
		strings.Join(columnDefs, ",\n  "),
	)

	if asset.Materialization.PartitionBy != "" {
		q += "\nPARTITION BY " + asset.Materialization.PartitionBy
	}
	if len(asset.Materialization.ClusterBy) > 0 {
		q += "\nCLUSTER BY " + strings.Join(asset.Materialization.ClusterBy, ", ")
	}

	return q, nil
}

func buildSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys  = make([]string, 0, 4)
		joinConds    = make([]string, 0, 5)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)
	for _, col := range asset.Columns {
		switch col.Name {
		case "_valid_from", "_valid_until", "_is_current":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return "", errors.New("incremental_key must be TIMESTAMP or DATE in SCD2_by_time strategy")
			}
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}
	pkList := strings.Join(primaryKeys, ", ")
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues,
		"CAST(source."+asset.Materialization.IncrementalKey+" AS TIMESTAMP)",
		"TIMESTAMP('9999-12-31')",
		"TRUE",
	)

	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf("target.%[1]s = source.%[1]s", pk))
	}
	joinConds = append(joinConds, "target._is_current AND source._is_current")
	onCondition := strings.Join(joinConds, " AND ")
	tbl := fmt.Sprintf("`%s`", asset.Name)

	queryStr := fmt.Sprintf(`
MERGE INTO %s AS target
USING (
  WITH s1 AS (
    %s
  )
  SELECT s1.*, TRUE AS _is_current
  FROM   s1
  UNION ALL
  SELECT s1.*, FALSE AS _is_current
  FROM s1
  JOIN   %s AS t1 USING (%s)
  WHERE  t1._valid_from < CAST (s1.%s AS TIMESTAMP) AND t1._is_current
) AS source
ON  %s

WHEN MATCHED AND (
  target._valid_from < CAST (source.%s AS TIMESTAMP)
) THEN
  UPDATE SET
    target._valid_until = CAST (source.%s AS TIMESTAMP),
    target._is_current  = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    target._valid_until = CURRENT_TIMESTAMP(),
    target._is_current  = FALSE

WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		tbl,
		strings.TrimSpace(query),
		tbl,
		pkList,
		asset.Materialization.IncrementalKey,
		onCondition,
		asset.Materialization.IncrementalKey,
		asset.Materialization.IncrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")
	var (
		primaryKeys      = make([]string, 0, 4)
		compareConds     = make([]string, 0, 12)
		compareCondsS1T1 = make([]string, 0, 4)
		insertCols       = make([]string, 0, 12)
		insertValues     = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%[1]s != source.%[1]s", col.Name))
			compareCondsS1T1 = append(compareCondsS1T1,
				fmt.Sprintf("t1.%[1]s != s1.%[1]s", col.Name))
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, "CURRENT_TIMESTAMP()", "TIMESTAMP('9999-12-31')", "TRUE")
	pkList := strings.Join(primaryKeys, ", ")
	for i, pk := range primaryKeys {
		primaryKeys[i] = fmt.Sprintf("target.%[1]s = source.%[1]s", pk)
	}
	onCondition := strings.Join(primaryKeys, " AND ")
	onCondition += " AND target._is_current AND source._is_current"

	tbl := fmt.Sprintf("`%s`", asset.Name)
	whereCondition := strings.Join(compareCondsS1T1, " OR ")
	whereCondition = "(" + whereCondition + ")" + " AND t1._is_current"
	queryStr := fmt.Sprintf(`
MERGE INTO %s AS target
USING (
  WITH s1 AS (
    %s
  )
  SELECT *, TRUE AS _is_current
  FROM   s1
  UNION ALL
  SELECT s1.*, FALSE AS _is_current
  FROM   s1
  JOIN   %s AS t1 USING (%s)
  WHERE  %s
) AS source
ON  %s

WHEN MATCHED AND (
    %s
) THEN
  UPDATE SET
    target._valid_until = CURRENT_TIMESTAMP(),
    target._is_current  = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    target._valid_until = CURRENT_TIMESTAMP(),
    target._is_current  = FALSE


WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		tbl,
		strings.TrimSpace(query),
		tbl,
		pkList,
		whereCondition,
		onCondition,
		strings.Join(compareConds, " OR "),
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}
	tbl := fmt.Sprintf("`%s`", asset.Name)
	cluster := strings.Join(primaryKeys, ", ")

	// Build partition clause - use user-specified partition or default to DATE(_valid_from)
	var partitionClause string
	if asset.Materialization.PartitionBy != "" {
		partitionClause = "PARTITION BY " + asset.Materialization.PartitionBy
	} else {
		partitionClause = "PARTITION BY DATE(_valid_from)"
	}

	// Build cluster clause - use user-specified cluster or default to _is_current + primary keys
	var clusterClause string
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterClause = "CLUSTER BY " + strings.Join(asset.Materialization.ClusterBy, ", ")
	} else {
		clusterClause = "CLUSTER BY _is_current, " + cluster
	}

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s
%s
%s AS
SELECT
  CAST (%s AS TIMESTAMP) AS _valid_from,
  src.*,
  TIMESTAMP('9999-12-31') AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;`,
		tbl,
		partitionClause,
		clusterClause,
		asset.Materialization.IncrementalKey,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}
	tbl := fmt.Sprintf("`%s`", asset.Name)
	cluster := strings.Join(primaryKeys, ", ")

	// Build partition clause - use user-specified partition or default to DATE(_valid_from)
	var partitionClause string
	if asset.Materialization.PartitionBy != "" {
		partitionClause = "PARTITION BY " + asset.Materialization.PartitionBy
	} else {
		partitionClause = "PARTITION BY DATE(_valid_from)"
	}

	// Build cluster clause - use user-specified cluster or default to _is_current + primary keys
	var clusterClause string
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterClause = "CLUSTER BY " + strings.Join(asset.Materialization.ClusterBy, ", ")
	} else {
		clusterClause = "CLUSTER BY _is_current, " + cluster
	}

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s
%s
%s AS
SELECT
  CURRENT_TIMESTAMP() AS _valid_from,
  src.*,
  TIMESTAMP('9999-12-31') AS _valid_until,
  TRUE                    AS _is_current
FROM (
%s
) AS src;`,
		tbl,
		partitionClause,
		clusterClause,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}
