package databricks

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type (
	MaterializerFunc        func(task *pipeline.Asset, query string) ([]string, error)
	AssetMaterializationMap map[pipeline.MaterializationType]map[pipeline.MaterializationStrategy]MaterializerFunc
)

var matMap = AssetMaterializationMap{
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
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2QueryByTime,
	},
}

func errorMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return nil, fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", asset.Name),
		fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", asset.Name, query),
	}, nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{fmt.Sprintf("INSERT INTO %s %s", asset.Name, query)}, nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) ([]string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return []string{}, fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		fmt.Sprintf("CREATE TEMPORARY VIEW %s AS %s\n", tempTableName, query),
		fmt.Sprintf("\nDELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP VIEW IF EXISTS " + tempTableName,
	}

	return queries, nil
}

func buildTruncateInsertQuery(task *pipeline.Asset, query string) ([]string, error) {
	queries := []string{
		"TRUNCATE TABLE " + task.Name,
		fmt.Sprintf("INSERT INTO %s %s", task.Name, strings.TrimSuffix(query, ";")),
	}
	return queries, nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) ([]string, error) {
	if len(asset.Columns) == 0 {
		return []string{}, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return []string{}, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	nonPrimaryKeys := asset.ColumnNamesWithUpdateOnMerge()
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", key, key))
	}
	onQuery := strings.Join(on, " AND ")

	allColumnValues := strings.Join(columnNames, ", ")

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", asset.Name),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
	}

	if len(nonPrimaryKeys) > 0 {
		matchedUpdateStatements := make([]string, 0, len(nonPrimaryKeys))
		for _, col := range nonPrimaryKeys {
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", col, col))
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		mergeLines = append(mergeLines, "WHEN MATCHED THEN UPDATE SET "+matchedUpdateQuery)
	}

	mergeLines = append(mergeLines, fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, allColumnValues))

	// Join all lines into a single MERGE statement
	mergeQuery := strings.Join(mergeLines, "\n")

	return []string{mergeQuery}, nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) ([]string, error) {
	mat := task.Materialization

	// Handle SCD2 strategies with full refresh
	//nolint:exhaustive
	switch mat.Strategy {
	case pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimefullRefresh(task, query)
	case pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnfullRefresh(task, query)
	}

	assetNameParts := strings.Split(task.Name, ".")
	if len(assetNameParts) != 2 {
		return []string{}, errors.New("databricks asset names must be in the format `database.table`")
	}
	databaseName := assetNameParts[0]

	if len(mat.ClusterBy) > 0 {
		return []string{}, errors.New("databricks assets do not support `cluster_by`")
	}

	tempTableName := databaseName + ".__bruin_tmp_" + helpers.PrefixGenerator()

	query = strings.TrimSuffix(query, ";")

	return []string{
		fmt.Sprintf(`CREATE TABLE %s AS %s;`, tempTableName, query),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, task.Name),
		fmt.Sprintf(`ALTER TABLE %s RENAME TO %s;`, tempTableName, task.Name),
	}, nil
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return nil, errors.New("time_granularity is required for time_interval strategy")
	}

	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp || asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return nil, errors.New("time_granularity must be either 'date', or 'timestamp'")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	queries := []string{
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'`,
			asset.Name,
			asset.Materialization.IncrementalKey,
			startVar,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			asset.Name, query),
	}

	return queries, nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) ([]string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))

	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)
		if col.PrimaryKey {
			def += " PRIMARY KEY"
		}
		if col.Description != "" {
			def += fmt.Sprintf(" COMMENT '%s'", col.Description)
		}
		columnDefs = append(columnDefs, def)
	}

	partitionBy := ""
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf("\nPARTITIONED BY (%s)", asset.Materialization.PartitionBy)
	}

	clusterByClause := ""
	if asset.Materialization.ClusterBy != nil {
		clusterByClause = "\nCLUSTER BY (" + strings.Join(asset.Materialization.ClusterBy, ", ") + ")"
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n"+
		"%s\n"+
		")%s"+
		"%s",
		asset.Name,
		strings.Join(columnDefs, ",\n"),
		partitionBy,
		clusterByClause,
	)

	return []string{ddl}, nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query string) ([]string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, errors.New("materialization strategy 'scd2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	query = strings.TrimSuffix(query, ";")

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s AS
SELECT
  CURRENT_TIMESTAMP() AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31 00:00:00' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		asset.Name,
		strings.TrimSpace(query),
	)

	return []string{stmt}, nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for scd2_by_time strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, errors.New("materialization strategy 'scd2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	query = strings.TrimSuffix(query, ";")
	incrementalKey := asset.Materialization.IncrementalKey

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s AS
SELECT
  %s AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31 00:00:00' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		asset.Name,
		incrementalKey,
		strings.TrimSpace(query),
	)

	return []string{stmt}, nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) ([]string, error) {
	query = strings.TrimRight(query, ";")
	var (
		primaryKeys      = make([]string, 0, 4)
		compareConds     = make([]string, 0, 12)
		compareCondsS1T1 = make([]string, 0, 4)
		insertCols       = make([]string, 0, 12)
		insertValues     = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		colName := col.Name
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, colName)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		insertCols = append(insertCols, colName)
		insertValues = append(insertValues, "source."+colName)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%s != source.%s", colName, colName))
			compareCondsS1T1 = append(compareCondsS1T1,
				fmt.Sprintf("t1.%s != s1.%s", colName, colName))
		}
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, "CURRENT_TIMESTAMP()", "TIMESTAMP '9999-12-31 00:00:00'", "TRUE")

	// Build USING clause for join
	pkListUsing := strings.Join(primaryKeys, ", ")

	// Build ON condition for MERGE
	onConditions := make([]string, 0, len(primaryKeys)+1)
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onConditions = append(onConditions, "target._is_current AND source._is_current")
	onCondition := strings.Join(onConditions, " AND ")

	var whereCondition string
	var matchedCondition string
	if len(compareCondsS1T1) > 0 {
		whereCondition = "(" + strings.Join(compareCondsS1T1, " OR ") + ")" + " AND t1._is_current"
		matchedCondition = strings.Join(compareConds, " OR ")
	} else {
		whereCondition = "FALSE AND t1._is_current"
		matchedCondition = "FALSE"
	}

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
    _valid_until = CURRENT_TIMESTAMP(),
    _is_current  = FALSE

WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP(),
    _is_current  = FALSE`,
		asset.Name,
		strings.TrimSpace(query),
		asset.Name,
		pkListUsing,
		whereCondition,
		onCondition,
		matchedCondition,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return []string{strings.TrimSpace(queryStr)}, nil
}

func buildSCD2QueryByTime(asset *pipeline.Asset, query string) ([]string, error) {
	query = strings.TrimRight(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for scd2_by_time strategy")
	}

	var (
		primaryKeys  = make([]string, 0, 4)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)
	for _, col := range asset.Columns {
		colName := col.Name
		switch col.Name {
		case "_valid_from", "_valid_until", "_is_current":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return nil, errors.New("incremental_key must be TIMESTAMP or DATE in scd2_by_time strategy")
			}
		}
		insertCols = append(insertCols, colName)
		insertValues = append(insertValues, "source."+colName)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, colName)
		}
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}

	// Build USING clause for join
	pkListUsing := strings.Join(primaryKeys, ", ")

	incrementalKey := asset.Materialization.IncrementalKey
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues,
		"source."+incrementalKey,
		"TIMESTAMP '9999-12-31 00:00:00'",
		"TRUE",
	)

	// Build join conditions
	joinConds := make([]string, 0, len(primaryKeys)+1)
	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	joinConds = append(joinConds, "target._is_current AND source._is_current")
	onCondition := strings.Join(joinConds, " AND ")

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
  WHERE  t1._valid_from < s1.%s AND t1._is_current
) AS source
ON  %s

WHEN MATCHED AND (
  target._valid_from < source.%s
) THEN
  UPDATE SET
    _valid_until = source.%s,
    _is_current  = FALSE

WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP(),
    _is_current  = FALSE`,
		asset.Name,
		strings.TrimSpace(query),
		asset.Name,
		pkListUsing,
		incrementalKey,
		onCondition,
		incrementalKey,
		incrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return []string{strings.TrimSpace(queryStr)}, nil
}
