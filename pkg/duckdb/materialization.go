package duck

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
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
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:          buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:        buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace: buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:  buildIncrementalQuery,
		pipeline.MaterializationStrategyMerge:         buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:  buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:           buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByTime:    buildSCD2ByTimeQuery,
		pipeline.MaterializationStrategySCD2ByColumn:  buildSCD2ByColumnQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", asset.Name, query), nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s\n", tempTableName, query),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	query = strings.TrimSuffix(query, ";")
	usingClause := strings.Join(primaryKeys, ", ")
	whereClause := primaryKeys[0]

	mergeQuery := fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s AS WITH source_data AS (%s) SELECT * FROM source_data UNION ALL SELECT dt.* FROM %s AS dt LEFT JOIN source_data AS sd USING(%s) WHERE sd.%s IS NULL",
		asset.Name,
		query,
		asset.Name,
		usingClause,
		whereClause,
	)

	queries := []string{
		"BEGIN TRANSACTION",
		mergeQuery,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) (string, error) {
	if task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime {
		return buildSCD2ByTimefullRefresh(task, query)
	}
	if task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn {
		return buildSCD2ByColumnfullRefresh(task, query)
	}
	query = strings.TrimSuffix(query, ";")
	return fmt.Sprintf(
		`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s; 
CREATE TABLE %s AS %s;
COMMIT;`, task.Name, task.Name, query), nil
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
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	primaryKeys := []string{}
	columnComments := []string{}

	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}

		columnDefs = append(columnDefs, def)

		if col.Description != "" {
			comment := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", asset.Name, col.Name, strings.ReplaceAll(col.Description, "'", "''"))
			columnComments = append(columnComments, comment)
		}
	}

	if len(primaryKeys) > 0 {
		primaryKeyClause := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
		columnDefs = append(columnDefs, primaryKeyClause)
	}

	createTableStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)", asset.Name, strings.Join(columnDefs, ",\n  "))

	if len(columnComments) > 0 {
		createTableStmt += ";\n" + strings.Join(columnComments, "\n")
	}

	return createTableStmt, nil
}

func buildSCD2QueryByTime2(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys        = make([]string, 0, 4)
		nonIncrementalCols = make([]string, 0, 12)
		insertValues       = make([]string, 0, 12)
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
		if col.Name != asset.Materialization.IncrementalKey {
			insertValues = append(insertValues, "s."+col.Name)
			nonIncrementalCols = append(nonIncrementalCols, "t."+col.Name)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}

	nonIncColsList := strings.Join(nonIncrementalCols, ", ")

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}

	pkJoin := make([]string, 0, len(primaryKeys))
	sourcePKs := make([]string, 0, len(primaryKeys))
	sourceNPKs := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		pkJoin = append(pkJoin, fmt.Sprintf("t.%[1]s = s.%[1]s", pk))
		sourcePKs = append(sourcePKs, fmt.Sprintf("s.%[1]s IS NULL", pk))
		sourceNPKs = append(sourceNPKs, fmt.Sprintf("s.%[1]s IS NOT NULL", pk))
	}
	sourcePrimaryKeyIsNull := strings.Join(sourcePKs, " AND ")
	sourcePrimaryKeyIsNotNull := strings.Join(sourceNPKs, " AND ")
	joinCondition := strings.Join(pkJoin, " AND ")
	incrementalKey := asset.Materialization.IncrementalKey

	// Build final SQL
	finalQuery := fmt.Sprintf(`
CREATE OR REPLACE TABLE %s AS
WITH 
source AS (
  %s
),
current_data AS (
  SELECT * FROM %s
),
t_new AS (
  SELECT
    %s, t._valid_from,
    CASE 
      WHEN %s THEN CURRENT_TIMESTAMP
      WHEN %s 
           AND %s 
           AND t._valid_from < CAST(s.%s AS TIMESTAMP)
      THEN CAST(s.dt AS TIMESTAMP)
      ELSE t._valid_until
    END AS _valid_until,
	CASE
      WHEN %s THEN FALSE
      WHEN %s
           AND %s
           AND t._valid_from < CAST(s.%s AS TIMESTAMP)
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
    TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
    TRUE AS _is_current
  FROM source s
)
SELECT * FROM t_new 
UNION
SELECT * FROM insert_rows;`,
		asset.Name,
		query,
		asset.Name,
		nonIncColsList,
		sourcePrimaryKeyIsNull,
		sourcePrimaryKeyIsNotNull,
		joinCondition,
		incrementalKey,
		sourcePrimaryKeyIsNull,
		sourcePrimaryKeyIsNotNull,
		joinCondition,
		incrementalKey,
		joinCondition,
		strings.Join(insertValues, ", "),
		incrementalKey,
	)
	return strings.TrimSpace(finalQuery), nil
}

func buildSCD2ByColumnQuery2(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")
	var (
		primaryKeys       = make([]string, 0, 4)
		compareCondsS1T1  = make([]string, 0, 4)
		tNewSelectCols    = make([]string, 0, 12)
		insertsSelectCols = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}

		if !col.PrimaryKey {
			compareCondsS1T1 = append(compareCondsS1T1, fmt.Sprintf("t.%[1]s != s.%[1]s", col.Name))
		}

		// For t_new SELECT
		tNewSelectCols = append(tNewSelectCols, "t."+col.Name)
		// For inserts SELECT
		insertsSelectCols = append(insertsSelectCols, "s."+col.Name)
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	onConds := make([]string, len(primaryKeys))
	sourcePKs := make([]string, 0, len(primaryKeys))
	targetPKs := make([]string, 0, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%[1]s = s.%[1]s", pk)
		sourcePKs = append(sourcePKs, fmt.Sprintf("s.%[1]s IS NULL", pk))
		targetPKs = append(targetPKs, fmt.Sprintf("t.%[1]s IS NULL", pk))
	}
	joinCondition := strings.Join(onConds, " AND ")
	fullCompareCondition := strings.Join(compareCondsS1T1, " OR ")
	soucePrimaryKeyIsNull := strings.Join(sourcePKs, " AND ")
	targetPrimaryKeyIsNull := strings.Join(targetPKs, " AND ")

	tbl := asset.Name

	queryStr := fmt.Sprintf(`
CREATE OR REPLACE TABLE %s AS
WITH
source AS (
  %s
),
current_data AS (
  SELECT * FROM %s WHERE _is_current = TRUE
),
t_new AS (
  SELECT 
    %s,
    t._valid_from,
    CASE
	  WHEN %s OR %s THEN CURRENT_TIMESTAMP
      ELSE t._valid_until 
    END AS _valid_until,
    CASE 
	  WHEN %s OR %s THEN FALSE
      ELSE t._is_current
    END AS _is_current
  FROM current_data t
  LEFT JOIN source s ON %s
),
insert_rows AS (
  SELECT 
    %s,
    CURRENT_TIMESTAMP AS _valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
    TRUE AS _is_current
  FROM source s
  LEFT JOIN current_data t ON %s
  WHERE %s
	OR %s
)
SELECT * FROM t_new
UNION
SELECT * FROM insert_rows;
`,
		tbl,
		strings.TrimSpace(query),
		tbl,
		strings.Join(tNewSelectCols, ",\n    "),
		soucePrimaryKeyIsNull,
		fullCompareCondition,
		soucePrimaryKeyIsNull,
		fullCompareCondition,
		joinCondition,
		strings.Join(insertsSelectCols, ",\n    "),
		joinCondition,
		targetPrimaryKeyIsNull,
		fullCompareCondition,
	)
	return strings.TrimSpace(queryStr), nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")

	var (
		primaryKeys = make([]string, 0, 4)
		userCols    = make([]string, 0, 12)
		nonPKCols   = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		} else {
			nonPKCols = append(nonPKCols, col.Name)
		}
		userCols = append(userCols, col.Name)
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the primary_key field to be set on at least one column", asset.Materialization.Strategy)
	}

	// Build join conditions for primary keys
	onConds := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", pk, pk)
	}
	joinCondition := strings.Join(onConds, " AND ")

	sourcePKCols := make([]string, len(primaryKeys))
	targetPKCols := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		sourcePKCols[i] = "s." + pk
		targetPKCols[i] = "t." + pk
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
		toKeepSelectCols = append(toKeepSelectCols, "t."+col)
	}

	// to_insert SELECT
	toInsertSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toInsertSelectCols = append(toInsertSelectCols, fmt.Sprintf("s.%s AS %s", col, col))
	}

	// Build the SQL using array formatting
	sqlLines := []string{
		"CREATE OR REPLACE TABLE " + asset.Name + " AS",
		"WITH",
		"time_now AS (",
		"\tSELECT CURRENT_TIMESTAMP AS now",
		"),",
		"source AS (",
		fmt.Sprintf("\tSELECT %s,", userColList),
		"\tTRUE as _matched_by_source",
		"\tFROM (" + strings.TrimSpace(query),
		"\t)",
		"),",
		"target AS (",
		fmt.Sprintf("\tSELECT %s,", allColList),
		"\tTRUE as _matched_by_target FROM " + asset.Name,
		"),",
		"current_data AS (",
		fmt.Sprintf("\tSELECT %s, _matched_by_target", allColList),
		"\tFROM target as t",
		"\tWHERE _is_current = TRUE",
		"),",
		"--current or updated (expired) existing rows from target",
		"to_keep AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toKeepSelectCols, ", ")),
		"\tt._valid_from,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN (SELECT now FROM time_now)", changeCondition),
		"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)",
		"\t\t\tELSE t._valid_until",
		"\t\tEND AS _valid_until,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN FALSE", changeCondition),
		"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE",
		"\t\t\tELSE t._is_current",
		"\t\tEND AS _is_current",
		"\tFROM target t",
		fmt.Sprintf("\tLEFT JOIN source s ON (%s) AND t._is_current = TRUE", joinCondition),
		"),",
		"--new/updated rows from source",
		"to_insert AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toInsertSelectCols, ", ")),
		"\t(SELECT now FROM time_now) AS _valid_from,",
		"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,",
		"\tTRUE AS _is_current",
		"\tFROM source s",
		fmt.Sprintf("\tLEFT JOIN current_data t ON (%s)", joinCondition),
		fmt.Sprintf("\tWHERE (_matched_by_target IS NULL) OR (%s)", changeCondition),
		")",
		fmt.Sprintf("SELECT %s FROM to_keep", allColList),
		"UNION ALL",
		fmt.Sprintf("SELECT %s FROM to_insert;", allColList),
	}

	return strings.Join(sqlLines, "\n"), nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	incrementalKey := asset.Materialization.IncrementalKey

	var (
		primaryKeys = make([]string, 0, 4)
		userCols    = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return "", errors.New("incremental_key must be TIMESTAMP or DATE in SCD2_by_time strategy")
			}
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		userCols = append(userCols, col.Name)
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the primary_key field to be set on at least one column", asset.Materialization.Strategy)
	}

	// Build join conditions for primary keys
	onConds := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", pk, pk)
	}
	joinCondition := strings.Join(onConds, " AND ")

	changeCondition := fmt.Sprintf("CAST(s.%s AS TIMESTAMP) > t._valid_from", incrementalKey)

	// Build user column list for SELECTs
	userColList := strings.Join(userCols, ", ")
	allCols := append([]string{}, userCols...)
	allCols = append(allCols, "_valid_from", "_valid_until", "_is_current")
	allColList := strings.Join(allCols, ", ")

	// to_keep SELECT
	toKeepSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toKeepSelectCols = append(toKeepSelectCols, "t."+col)
	}

	// to_insert SELECT
	toInsertSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toInsertSelectCols = append(toInsertSelectCols, fmt.Sprintf("s.%s AS %s", col, col))
	}

	// Build the SQL using array formatting
	sqlLines := []string{
		"CREATE OR REPLACE TABLE " + asset.Name + " AS",
		"WITH",
		"time_now AS (",
		"\tSELECT CURRENT_TIMESTAMP AS now",
		"),",
		"source AS (",
		fmt.Sprintf("\tSELECT %s,", userColList),
		"\tTRUE as _matched_by_source",
		"\tFROM (" + strings.TrimSpace(query),
		"\t)",
		"),",
		"target AS (",
		fmt.Sprintf("\tSELECT %s,", allColList),
		"\tTRUE as _matched_by_target FROM " + asset.Name,
		"),",
		"current_data AS (",
		fmt.Sprintf("\tSELECT %s, _matched_by_target", allColList),
		"\tFROM target as t",
		"\tWHERE _is_current = TRUE",
		"),",
		"--current or updated (expired) existing rows from target",
		"to_keep AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toKeepSelectCols, ", ")),
		"\tt._valid_from,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN CAST(s.%s AS TIMESTAMP)", changeCondition, incrementalKey),
		"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)",
		"\t\t\tELSE t._valid_until",
		"\t\tEND AS _valid_until,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN FALSE", changeCondition),
		"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE",
		"\t\t\tELSE t._is_current",
		"\t\tEND AS _is_current",
		"\tFROM target t",
		fmt.Sprintf("\tLEFT JOIN source s ON (%s) AND t._is_current = TRUE", joinCondition),
		"),",
		"--new/updated rows from source",
		"to_insert AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toInsertSelectCols, ", ")),
		fmt.Sprintf("\tCAST(s.%s AS TIMESTAMP) AS _valid_from,", incrementalKey),
		"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,",
		"\tTRUE AS _is_current",
		"\tFROM source s",
		fmt.Sprintf("\tLEFT JOIN current_data t ON (%s)", joinCondition),
		fmt.Sprintf("\tWHERE (_matched_by_target IS NULL) OR (%s)", changeCondition),
		")",
		fmt.Sprintf("SELECT %s FROM to_keep", allColList),
		"UNION ALL",
		fmt.Sprintf("SELECT %s FROM to_insert;", allColList),
	}
	
	return strings.Join(sqlLines, "\n"), nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}
	tbl := asset.Name
	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s AS
SELECT
  CAST (%s AS TIMESTAMP) AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;`,
		tbl,
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
	tbl := asset.Name
	stmt := fmt.Sprintf(
		`
CREATE OR REPLACE TABLE %s AS
SELECT
  CURRENT_TIMESTAMP AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
  TRUE                    AS _is_current
FROM (
%s
) AS src;`,
		tbl,
		strings.TrimSpace(query),
	)
	return strings.TrimSpace(stmt), nil
}
