package athena

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type (
	MaterializerFunc        func(task *pipeline.Asset, query, location string) ([]string, error)
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
		pipeline.MaterializationStrategyNone:          buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:        buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace: buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:  buildIncrementalQuery,
		pipeline.MaterializationStrategyMerge:         buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:  buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:           buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:  buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:    buildSCD2ByTimeQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query, location string) ([]string, error) {
	return nil, fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query, location string) ([]string, error) {
	return []string{fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query)}, nil
}

func buildAppendQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	return []string{fmt.Sprintf("INSERT INTO %s %s", asset.Name, query)}, nil
}

func buildIncrementalQuery(task *pipeline.Asset, query, location string) ([]string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return []string{}, fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		fmt.Sprintf("CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s') AS %s\n", tempTableName, location, tempTableName, query),
		fmt.Sprintf("\nDELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
	}

	return queries, nil
}

func buildMergeQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
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

	columnNamesWithSource := make([]string, len(columnNames))
	for i, col := range columnNames {
		columnNamesWithSource[i] = "source." + col
	}

	allColumnValues := strings.Join(columnNames, ", ")

	whenMatchedThenQuery := ""

	if len(nonPrimaryKeys) > 0 {
		matchedUpdateStatements := make([]string, 0, len(nonPrimaryKeys))
		for _, col := range nonPrimaryKeys {
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", col, col))
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	queries := []string{
		fmt.Sprintf("MERGE INTO %s target", asset.Name),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, strings.Join(columnNamesWithSource, ", ")),
	}

	return []string{strings.Join(queries, " ")}, nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query, location string) ([]string, error) {
	switch {
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimefullRefresh(task, query, location)
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnfullRefresh(task, query, location)
	default:
		query = strings.TrimSuffix(query, ";")

		tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

		var partitionBy string
		if task.Materialization.PartitionBy != "" {
			partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", task.Materialization.PartitionBy)
		}

		return []string{
			fmt.Sprintf(
				"CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS %s",
				tempTableName,
				location,
				tempTableName,
				partitionBy,
				query,
			),
			"DROP TABLE IF EXISTS " + task.Name,
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempTableName, task.Name),
		}, nil
	}
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string, location string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for time_interval strategy")
	}
	if asset.Materialization.TimeGranularity == "" {
		return nil, errors.New("time_granularity is required for time_interval strategy")
	}
	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp || asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return nil, errors.New("time_granularity must be either 'date', or 'timestamp'")
	}

	timePrefix := "timestamp"
	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
		timePrefix = "date"
	}

	queries := []string{
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN %s '%s' AND %s '%s'`,
			asset.Name,
			asset.Materialization.IncrementalKey,
			timePrefix,
			startVar,
			timePrefix,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			asset.Name, query),
	}

	return queries, nil
}

func buildDDLQuery(asset *pipeline.Asset, query string, location string) ([]string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)
		if col.Description != "" {
			desc := strings.ReplaceAll(col.Description, `'`, `''`)
			def += fmt.Sprintf(" COMMENT '%s'", desc)
		}
		columnDefs = append(columnDefs, def)
	}

	partitionedBy := ""
	if asset.Materialization.PartitionBy != "" {
		partitionedBy = fmt.Sprintf("\nPARTITIONED BY (%s)", asset.Materialization.PartitionBy)
	}

	ddlQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n"+
			"%s\n"+
			")"+
			"%s"+
			"\nLOCATION '%s/%s'"+
			"\nTBLPROPERTIES('table_type'='ICEBERG')",
		asset.Name,
		strings.Join(columnDefs, ",\n"),
		partitionedBy,
		location,
		asset.Name,
	)

	return []string{ddlQuery}, nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	query = strings.TrimSuffix(query, ";")

	var (
		primaryKeys       = make([]string, 0, 4)
		compareConditions = make([]string, 0, 4)
		userCols          = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		} else {
			compareConditions = append(compareConditions, fmt.Sprintf("t.%s != s.%s", col.Name, col.Name))
		}
		userCols = append(userCols, col.Name)
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	// Build join conditions
	onConds := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", pk, pk)
	}
	joinCondition := strings.Join(onConds, " AND ")

	// Build primary key checks
	targetPrimaryKeys := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		targetPrimaryKeys[i] = fmt.Sprintf("t.%s", pk)
	}
	targetPKIsNullCheck := strings.Join(targetPrimaryKeys, " IS NULL AND ")
	
	sourcePrimaryKeys := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		sourcePrimaryKeys[i] = fmt.Sprintf("s_%s", pk)
	}
	sourcePKIsNullCheck := strings.Join(sourcePrimaryKeys, " IS NULL AND ")
	sourcePKNotNullCheck := strings.Join(sourcePrimaryKeys, " IS NOT NULL AND ")

	// Build CASE condition for change detection
	changeCondition := strings.Join(compareConditions, " OR ")

	// Build column lists
	allCols := append([]string{}, userCols...)
	allCols = append(allCols, "_valid_from", "_valid_until", "_is_current")

	// Build joined CTE SELECT with explicit aliases
	joinedSelectCols := make([]string, 0, len(userCols)*2+3)
	for _, col := range userCols {
		joinedSelectCols = append(joinedSelectCols, fmt.Sprintf("t.%s AS t_%s", col, col))
	}
	joinedSelectCols = append(joinedSelectCols, "t._valid_from")
	joinedSelectCols = append(joinedSelectCols, "t._valid_until")
	joinedSelectCols = append(joinedSelectCols, "t._is_current")
	for _, col := range userCols {
		joinedSelectCols = append(joinedSelectCols, fmt.Sprintf("s.%s AS s_%s", col, col))
	}

	// Build unchanged CTE SELECT
	unchangedSelectCols := make([]string, 0, len(allCols))
	for _, col := range userCols {
		unchangedSelectCols = append(unchangedSelectCols, fmt.Sprintf("t_%s AS %s", col, col))
	}

	// Build to_expire CTE SELECT
	expireSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		expireSelectCols = append(expireSelectCols, fmt.Sprintf("t_%s AS %s", col, col))
	}

	// Build to_insert CTE SELECT
	insertSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		insertSelectCols = append(insertSelectCols, fmt.Sprintf("s.%s AS %s", col, col))
	}

	// Build change condition for joined CTE (using aliased column names)
	joinedChangeConds := make([]string, len(compareConditions))
	for i, cond := range compareConditions {
		// Replace t.col and s.col with t_col and s_col
		cond = strings.ReplaceAll(cond, "t.", "t_")
		cond = strings.ReplaceAll(cond, "s.", "s_")
		joinedChangeConds[i] = cond
	}
	joinedChangeCondition := strings.Join(joinedChangeConds, " OR ")

	// Build unchanged condition (equality check)
	unchangedConds := make([]string, len(compareConditions))
	for i, cond := range compareConditions {
		// Replace t.col != s.col with t_col = s_col for unchanged check
		cond = strings.ReplaceAll(cond, "t.", "t_")
		cond = strings.ReplaceAll(cond, "s.", "s_")
		cond = strings.ReplaceAll(cond, "!=", "=")
		unchangedConds[i] = cond
	}
	unchangedCondition := strings.Join(unchangedConds, " AND ")

	createQuery := fmt.Sprintf(`
CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS
WITH
time_now AS (
  SELECT CURRENT_TIMESTAMP AS now
),
source AS (
  %s
),
target AS (
  SELECT %s 	
  FROM %s 
  WHERE _is_current = TRUE
),
joined AS (
  SELECT %s
  FROM target t
  LEFT JOIN source s ON %s
),
-- Rows that are unchanged
unchanged AS (
  SELECT %s,
  _valid_from,
  _valid_until,
  _is_current
  FROM joined
  WHERE %s IS NOT NULL AND %s
),
-- Rows that need to be expired (changed or missing in source)
to_expire AS (
  SELECT %s,
  _valid_from,
  (SELECT now FROM time_now) AS _valid_until,
  FALSE AS _is_current
  FROM joined
  WHERE %s IS NULL OR %s
),
-- New/changed inserts from source
to_insert AS (
  SELECT %s,
  (SELECT now FROM time_now) AS _valid_from,
  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
  TRUE AS _is_current
  FROM source s
  LEFT JOIN target t ON %s
  WHERE %s IS NULL OR %s
),
-- Already expired historical rows (untouched)
historical AS (
  SELECT %s
  FROM %s
  WHERE _is_current = FALSE
)
SELECT %s FROM unchanged
UNION ALL
SELECT %s FROM to_expire
UNION ALL
SELECT %s FROM to_insert
UNION ALL
SELECT %s FROM historical`,
		// Create table
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		// Source data
		strings.TrimSpace(query),
		// Target data
		strings.Join(allCols, ", "),
		asset.Name,
		// Joined data
		strings.Join(joinedSelectCols, ",\n    "),
		joinCondition,
		// Unchanged data
		strings.Join(unchangedSelectCols, ", "),
		sourcePKNotNullCheck,
		unchangedCondition,
		// Expired data
		strings.Join(expireSelectCols, ", "),
		sourcePKIsNullCheck,
		joinedChangeCondition,
		// Insert data
		strings.Join(insertSelectCols, ", "),
		joinCondition,
		targetPKIsNullCheck,
		changeCondition,
		// Historical data
		strings.Join(allCols, ", "),
		asset.Name,
		// Unions
		strings.Join(allCols, ", "),
		strings.Join(allCols, ", "),
		strings.Join(allCols, ", "),
		strings.Join(allCols, ", "),
	)

	return []string{
		strings.TrimSpace(createQuery),
		"\nDROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("\nALTER TABLE %s RENAME TO %s;", tempTableName, asset.Name),
	}, nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	query = strings.TrimSuffix(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys = make([]string, 0, 4)
		userCols    = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_valid_from", "_valid_until", "_is_current":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return nil, errors.New("incremental_key must be TIMESTAMP or DATE in SCD2_by_time strategy")
			}
		}
		userCols = append(userCols, col.Name)
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}

	// Build join conditions for primary keys
	onConds := make([]string, len(primaryKeys))
	sourcePKs := make([]string, 0, len(primaryKeys))
	targetPKs := make([]string, 0, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", pk, pk)
		sourcePKs = append(sourcePKs, fmt.Sprintf("s.%s IS NULL", pk))
		targetPKs = append(targetPKs, fmt.Sprintf("t.%s IS NULL", pk))
	}
	joinCondition := strings.Join(onConds, " AND ")
	sourcePrimaryKeyIsNull := strings.Join(sourcePKs, " AND ")
	targetPrimaryKeyIsNull := strings.Join(targetPKs, " AND ")

	// Build column lists for different CTEs
	tNewSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		tNewSelectCols = append(tNewSelectCols, "t."+col)
	}
	tNewSelectCols = append(tNewSelectCols, "t._valid_from")
	// Only compare the timestamp (incremental key) for changes
	tNewSelectCols = append(tNewSelectCols, fmt.Sprintf("CASE WHEN %s OR (s.%s IS NOT NULL AND CAST(s.%s AS TIMESTAMP) > t._valid_from) THEN CAST(s.%s AS TIMESTAMP) ELSE t._valid_until END AS _valid_until", sourcePrimaryKeyIsNull, asset.Materialization.IncrementalKey, asset.Materialization.IncrementalKey, asset.Materialization.IncrementalKey))
	tNewSelectCols = append(tNewSelectCols, fmt.Sprintf("CASE WHEN %s OR (s.%s IS NOT NULL AND CAST(s.%s AS TIMESTAMP) > t._valid_from) THEN FALSE ELSE t._is_current END AS _is_current", sourcePrimaryKeyIsNull, asset.Materialization.IncrementalKey, asset.Materialization.IncrementalKey))

	insertsSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		insertsSelectCols = append(insertsSelectCols, "s."+col)
	}
	insertsSelectCols = append(insertsSelectCols, fmt.Sprintf("CAST(s.%s AS TIMESTAMP) AS _valid_from", asset.Materialization.IncrementalKey))
	insertsSelectCols = append(insertsSelectCols, "TIMESTAMP '9999-12-31' AS _valid_until")
	insertsSelectCols = append(insertsSelectCols, "TRUE AS _is_current")

	// Historical data columns
	allCols := append([]string{}, userCols...)
	allCols = append(allCols, "_valid_from", "_valid_until", "_is_current")
	histCols := make([]string, 0, len(allCols))
	for _, col := range allCols {
		histCols = append(histCols, "h."+col)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	createQuery := fmt.Sprintf(`
CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS
WITH
source AS (
  %s
),
current_data AS (
  SELECT * FROM %s WHERE _is_current = TRUE
),
historical_data AS (
  SELECT %s FROM %s h WHERE h._is_current = FALSE
),
t_new AS (
  SELECT 
    %s
  FROM current_data t
  LEFT JOIN source s ON %s
),
insert_rows AS (
  SELECT 
    %s
  FROM source s
  LEFT JOIN current_data t ON %s
  WHERE %s OR (%s AND CAST(s.%s AS TIMESTAMP) > t._valid_from)
)
SELECT %s FROM t_new
UNION ALL
SELECT %s FROM insert_rows
UNION ALL
SELECT %s FROM historical_data`,
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		strings.TrimSpace(query),
		asset.Name,
		strings.Join(histCols, ", "),
		asset.Name,
		strings.Join(tNewSelectCols, ",\n    "),
		joinCondition,
		strings.Join(insertsSelectCols, ",\n    "),
		joinCondition,
		targetPrimaryKeyIsNull,
		joinCondition,
		asset.Materialization.IncrementalKey,
		strings.Join(allCols, ", "),
		strings.Join(allCols, ", "),
		strings.Join(allCols, ", "),
	)

	return []string{
		strings.TrimSpace(createQuery),
		"DROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempTableName, asset.Name),
	}, nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, errors.New("materialization strategy 'SCD2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	createQuery := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS
SELECT
  CAST(%s AS TIMESTAMP) AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		asset.Materialization.IncrementalKey,
		strings.TrimSpace(query),
	)

	return []string{
		strings.TrimSpace(createQuery),
		"DROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempTableName, asset.Name),
	}, nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	srcCols := make([]string, len(asset.Columns))
	for i, col := range asset.Columns {
		srcCols[i] = fmt.Sprintf("src.%s", col.Name)
	}

	createQuery := fmt.Sprintf(
		`CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS
SELECT
  %s,
  CURRENT_TIMESTAMP AS _valid_from,
  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		strings.Join(srcCols, ", "),
		strings.TrimSpace(query),
	)

	return []string{
		strings.TrimSpace(createQuery),
		"\nDROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("\nALTER TABLE %s RENAME TO %s;", tempTableName, asset.Name),
	}, nil
}
