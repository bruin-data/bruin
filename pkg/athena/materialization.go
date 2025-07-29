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
		primaryKeys = make([]string, 0, 4)
		userCols    = make([]string, 0, 12)
		nonPKCols   = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		} else {
			nonPKCols = append(nonPKCols, col.Name)
		}
		userCols = append(userCols, col.Name)
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the primary_key field to be set on at least one column", asset.Materialization.Strategy)
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	// Build join conditions for primary keys
	onConds := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", pk, pk)
	}
	joinCondition := strings.Join(onConds, " AND ")

	sourcePKCols := make([]string, len(primaryKeys))
	targetPKCols := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		sourcePKCols[i] = fmt.Sprintf("s.%s", pk)
		targetPKCols[i] = fmt.Sprintf("t.%s", pk)
	}

	changeConditions := make([]string, 0, len(nonPKCols))
	for _, col := range nonPKCols {
		changeConditions = append(changeConditions, fmt.Sprintf("t.%s != s.%s", col, col))
	}
	changeCondition := ""
	if len(changeConditions) > 0 {
		changeCondition = strings.Join(changeConditions, " OR ")
	}

	// Build user column list for SELECTs
	userColList := strings.Join(userCols, ", ")
	allCols := append([]string{}, userCols...)
	allCols = append(allCols, "_valid_from", "_valid_until", "_is_current")
	allColList := strings.Join(allCols, ", ")

	// to_keep SELECT
	toKeepSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toKeepSelectCols = append(toKeepSelectCols, fmt.Sprintf("t.%s", col))
	}

	// to_insert SELECT
	toInsertSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toInsertSelectCols = append(toInsertSelectCols, fmt.Sprintf("s.%s AS %s", col, col))
	}

	createQuery := fmt.Sprintf(`
CREATE TABLE %[1]s WITH (table_type='ICEBERG', is_external=false, location='%[2]s/%[1]s'%[3]s) AS
WITH
	time_now AS (
    	SELECT CURRENT_TIMESTAMP AS now
	),
	source AS (
		SELECT %[4]s,
		TRUE as _matched_by_source 
		FROM (%[5]s
		)
	),
	target AS (
    	SELECT %[6]s,
		TRUE as _matched_by_target FROM %[7]s
	),
	current_data AS (
    	SELECT %[6]s
    	FROM target as t
    	WHERE _is_current = TRUE
	),
	--current or updated (expired) existing rows from target
	to_keep AS (
    	SELECT %[4]s,
			t._valid_from,
			CASE 
				WHEN _matched_by_source IS NOT NULL AND (%[9]s) THEN (SELECT now FROM time_now)
				WHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)
				ELSE t._valid_until
			END AS _valid_until,
			CASE
				WHEN _matched_by_source IS NOT NULL AND (%[9]s) THEN FALSE
				WHEN _matched_by_source IS NULL THEN FALSE
				ELSE t._is_current
			END AS _is_current
    	FROM target t
    	LEFT JOIN source s ON (%[10]s) AND t._is_current = TRUE
	),
	--new/updated rows from source
	to_insert AS (
    	SELECT %[11]s,
			(SELECT now FROM time_now) AS _valid_from,
			TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
			TRUE AS _is_current
    	FROM source s
    	LEFT JOIN current_data t ON (%[10]s)
    	WHERE (_matched_by_target IS NULL) OR (%[9]s)
	)
SELECT %[6]s FROM to_keep
UNION ALL
SELECT %[6]s FROM to_insert;`,
		tempTableName, //1
		location, //2
		partitionBy, //3
		// Source data
		userColList, //4
		strings.TrimSpace(query), //5
		// Target data + current_data
		allColList, //6
		asset.Name, //7
		// to_keep data
		strings.Join(toKeepSelectCols, ", \n    "), //8
		changeCondition, //9
		joinCondition, //10
		// to_insert data
		strings.Join(toInsertSelectCols, ",\n    "), //11
		//unions
		allColList, //6
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

	incrementalKey := asset.Materialization.IncrementalKey

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

	insertsSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		insertsSelectCols = append(insertsSelectCols, "s."+col)
	}

	// Historical data columns
	allCols := append([]string{}, userCols...)
	allCols = append(allCols, "_valid_from", "_valid_until", "_is_current")

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
  SELECT %s FROM %s WHERE _is_current = TRUE
),
historical_data AS (
  SELECT %s FROM %s WHERE _is_current = FALSE
),
t_new AS (
  SELECT 
    %s,
    t._valid_from,
    CASE WHEN %s OR (s.%s IS NOT NULL AND CAST(s.%s AS TIMESTAMP) > t._valid_from)
	THEN CAST(s.%s AS TIMESTAMP) 
	ELSE t._valid_until 
	END AS _valid_until,
    CASE WHEN %s OR (s.%s IS NOT NULL AND CAST(s.%s AS TIMESTAMP) > t._valid_from)
	THEN FALSE 
	ELSE t._is_current 
	END AS _is_current
  FROM current_data t
  LEFT JOIN source s ON %s
),
insert_rows AS (
  SELECT 
    %s,
    CAST(s.%s AS TIMESTAMP) AS _valid_from,
    TIMESTAMP '9999-12-31' AS _valid_until,
    TRUE AS _is_current
  FROM source s
  LEFT JOIN current_data t ON %s
  WHERE %s OR (%s AND CAST(s.%s AS TIMESTAMP) > t._valid_from)
)
SELECT %s FROM t_new
UNION ALL
SELECT %s FROM insert_rows
UNION ALL
SELECT %s FROM historical_data`,
		// Create table
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		// Source data
		strings.TrimSpace(query),
		// current data
		strings.Join(allCols, ", "),
		asset.Name,
		// Historical data
		strings.Join(allCols, ", "),
		asset.Name,
		// t_new data
		strings.Join(tNewSelectCols, ",\n    "),
		sourcePrimaryKeyIsNull,
		incrementalKey,
		incrementalKey,
		incrementalKey,
		sourcePrimaryKeyIsNull,
		incrementalKey,
		incrementalKey,
		joinCondition,
		// insert_rows data
		strings.Join(insertsSelectCols, ",\n    "),
		incrementalKey,
		joinCondition,
		targetPrimaryKeyIsNull,
		joinCondition,
		// historical_data data
		asset.Materialization.IncrementalKey,
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

	srcCols := make([]string, len(asset.Columns))
	for i, col := range asset.Columns {
		srcCols[i] = "src." + col.Name
	}

	createQuery := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS
SELECT
  %s,		
  CAST(%s AS TIMESTAMP) AS _valid_from,
  TIMESTAMP '9999-12-31' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		strings.Join(srcCols, ", "),
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
		srcCols[i] = "src." + col.Name
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


