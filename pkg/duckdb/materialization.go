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
		pipeline.MaterializationStrategySCD2ByTime:    buildSCD2QueryByTime,
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

func nonPKCols(cols []pipeline.Column, primaryKeys []string) []string {
	pkMap := make(map[string]bool)
	for _, pk := range primaryKeys {
		pkMap[pk] = true
	}
	nonPK := []string{}
	for _, col := range cols {
		if !pkMap[col.Name] {
			nonPK = append(nonPK, fmt.Sprintf("t.%s", col.Name))
		}
	}
	return nonPK
}

func buildSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys  = make([]string, 0, 4)
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
		insertValues = append(insertValues, fmt.Sprintf("s.%s", col.Name))

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

	pkJoin := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		pkJoin = append(pkJoin, fmt.Sprintf("t.%[1]s = s.%[1]s", pk))
	}
	pkList := strings.Join(primaryKeys, ", ")
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
  SELECT * FROM %s WHERE _is_current = TRUE
),
t_new AS (
  SELECT 
    t.%s,
    %s,
    t._valid_from,
    CASE 
      WHEN s.%s IS NOT NULL AND t._valid_from < CAST(s.%s AS TIMESTAMP)
        THEN CAST(s.%s AS TIMESTAMP)
      WHEN s.%s IS NULL 
        THEN CURRENT_TIMESTAMP
      ELSE t._valid_until
    END AS _valid_until,
    CASE 
      WHEN s.%s IS NOT NULL AND t._valid_from < CAST(s.%s AS TIMESTAMP)
        THEN FALSE
      WHEN s.%s IS NULL 
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
  LEFT JOIN current_data t ON %s
  WHERE t.%s IS NULL OR t._valid_from < CAST(s.%s AS TIMESTAMP)
)
SELECT * FROM t_new
UNION ALL
SELECT * FROM insert_rows;`,
		asset.Name,
		query,
		asset.Name,
		pkList,
		strings.Join(nonPKCols(asset.Columns, primaryKeys), ", "),
		primaryKeys[0], incrementalKey, incrementalKey,
		primaryKeys[0],
		primaryKeys[0], incrementalKey,
		primaryKeys[0],
		joinCondition,
		strings.Join(insertValues, ", "),
		incrementalKey,
		joinCondition,
		primaryKeys[0], incrementalKey,
	)
	fmt.Printf(finalQuery + "\n")
	return strings.TrimSpace(finalQuery), nil
}

//func buildSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
//	query = strings.TrimRight(query, ";")
//
//	if asset.Materialization.IncrementalKey == "" {
//		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
//	}
//
//	var (
//		primaryKeys  = make([]string, 0, 4)
//		joinConds    = make([]string, 0, 5)
//		insertCols   = make([]string, 0, 12)
//		insertValues = make([]string, 0, 12)
//	)
//	for _, col := range asset.Columns {
//		switch col.Name {
//		case "_valid_from", "_valid_until", "_is_current":
//			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
//		}
//		if col.Name == asset.Materialization.IncrementalKey {
//			lcType := strings.ToLower(col.Type)
//			if lcType != "timestamp" && lcType != "date" {
//				return "", errors.New("incremental_key must be TIMESTAMP or DATE in SCD2_by_time strategy")
//			}
//		}
//		insertCols = append(insertCols, col.Name)
//		insertValues = append(insertValues, "source."+col.Name)
//
//		if col.PrimaryKey {
//			primaryKeys = append(primaryKeys, col.Name)
//		}
//	}
//
//	if len(primaryKeys) == 0 {
//		return "", fmt.Errorf(
//			"materialization strategy %s requires the primary_key field to be set on at least one column",
//			asset.Materialization.Strategy,
//		)
//	}
//	pkList := strings.Join(primaryKeys, ", ")
//	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
//	insertValues = append(insertValues,
//		"CAST(source."+asset.Materialization.IncrementalKey+" AS TIMESTAMP)",
//		"TIMESTAMP('9999-12-31')",
//		"TRUE",
//	)
//
//	for _, pk := range primaryKeys {
//		joinConds = append(joinConds, fmt.Sprintf("target.%[1]s = source.%[1]s", pk))
//	}
//	joinConds = append(joinConds, "target._is_current AND source._is_current")
//	onCondition := strings.Join(joinConds, " AND ")
//	tbl := fmt.Sprintf("%s", asset.Name)
//
//	queryStr := fmt.Sprintf(
//		`CREATE OR REPLACE TABLE %s AS
//WITH
//source AS (
//  %s
//),
//current_data AS (
//  SELECT * FROM %s WHERE _is_current = TRUE
//),
//t_new AS (
//  SELECT
//    t.%s,
//    %s,
//    t._valid_from,
//    CASE
//      WHEN s.%s IS NOT NULL AND t._valid_from < CAST(s.%s AS TIMESTAMP)
//        THEN CAST(s.%s AS TIMESTAMP)
//      WHEN s.%s IS NULL
//        THEN CURRENT_TIMESTAMP
//      ELSE t._valid_until
//    END AS _valid_until,
//    CASE
//      WHEN s.%s IS NOT NULL AND t._valid_from < CAST(s.%s AS TIMESTAMP)
//        THEN FALSE
//      WHEN s.%s IS NULL
//        THEN FALSE
//      ELSE t._is_current
//    END AS _is_current
//  FROM current_data t
//  LEFT JOIN source s ON %s
//),
//insert_rows AS (
//  SELECT
//    %s,
//    CAST(s.%s AS TIMESTAMP) AS _valid_from,
//    TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
//    TRUE AS _is_current
//  FROM source s
//  LEFT JOIN current_data t ON %s
//  WHERE t.%s IS NULL OR t._valid_from < CAST(s.%s AS TIMESTAMP)
//)
//SELECT * FROM t_new
//UNION ALL
//SELECT * FROM insert_rows;`,
//		tbl,
//		strings.TrimSpace(query),
//		tbl,
//		pkList,
//		asset.Materialization.IncrementalKey,
//		onCondition,
//		asset.Materialization.IncrementalKey,
//		asset.Materialization.IncrementalKey,
//		strings.Join(insertCols, ", "),
//		strings.Join(insertValues, ", "),
//	)
//	fmt.Printf(queryStr + "\n")
//	return strings.TrimSpace(queryStr), nil
//}

//func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
//	query = strings.TrimRight(query, ";")
//	var (
//		primaryKeys      = make([]string, 0, 4)
//		compareConds     = make([]string, 0, 12)
//		compareCondsS1T1 = make([]string, 0, 4)
//		insertCols       = make([]string, 0, 12)
//		insertValues     = make([]string, 0, 12)
//	)
//
//	for _, col := range asset.Columns {
//		if col.PrimaryKey {
//			primaryKeys = append(primaryKeys, col.Name)
//		}
//		switch col.Name {
//		case "_is_current", "_valid_from", "_valid_until":
//			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
//		}
//		insertCols = append(insertCols, col.Name)
//		insertValues = append(insertValues, "source."+col.Name)
//		if !col.PrimaryKey {
//			compareConds = append(compareConds,
//				fmt.Sprintf("target.%[1]s != source.%[1]s", col.Name))
//			compareCondsS1T1 = append(compareCondsS1T1,
//				fmt.Sprintf("t1.%[1]s != s1.%[1]s", col.Name))
//		}
//	}
//
//	if len(primaryKeys) == 0 {
//		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
//			asset.Materialization.Strategy)
//	}
//	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
//	insertValues = append(insertValues, "CURRENT_TIMESTAMP()", "TIMESTAMP('9999-12-31')", "TRUE")
//	//pkList := strings.Join(primaryKeys, ", ")
//	for i, pk := range primaryKeys {
//		primaryKeys[i] = fmt.Sprintf("target.%[1]s = source.%[1]s", pk)
//	}
//	onCondition := strings.Join(primaryKeys, " AND ")
//	onCondition += " AND target._is_current = source._is_current"
//
//	tbl := fmt.Sprintf("%s", asset.Name)
//
//	queryStr := fmt.Sprintf(`
//CREATE OR REPLACE TABLE %s AS
//WITH
//source AS (
//  %s
//),
//current_data AS (
//  SELECT * FROM %s WHERE _is_current = TRUE
//),
//t_new AS (
//  SELECT
//    t.ID,
//    t.Name,
//    t.Price,
//    t._valid_from,
//    CASE
//      WHEN s.ID IS NOT NULL THEN CURRENT_TIMESTAMP
//      ELSE t._valid_until
//    END AS _valid_until,
//    CASE
//      WHEN s.ID IS NOT NULL THEN FALSE
//      ELSE t._is_current
//    END AS _is_current
//  FROM current_data t
//  LEFT JOIN source s ON t.ID = s.ID AND (t.Name != s.Name OR t.Price != s.Price)
//),
//inserts AS (
//  SELECT
//    s.ID,
//    s.Name,
//    s.Price,
//    CURRENT_TIMESTAMP AS _valid_from,
//    TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
//    TRUE AS _is_current
//  FROM source s
//  LEFT JOIN current_data t ON s.ID = t.ID AND (t.Name != s.Name OR t.Price != s.Price)
//  WHERE t.ID IS NULL OR t.Name != s.Name OR t.Price != s.Price
//)
//SELECT * FROM t_new
//UNION ALL
//SELECT * FROM inserts;
//`, tbl, strings.TrimSpace(query), tbl)
//	print(queryStr + "\n")
//	return strings.TrimSpace(queryStr), nil
//}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")
	var (
		primaryKeys       = make([]string, 0, 4)
		compareConds      = make([]string, 0, 12)
		compareCondsS1T1  = make([]string, 0, 4)
		insertCols        = make([]string, 0, 12)
		insertValues      = make([]string, 0, 12)
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

		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, fmt.Sprintf("source.%s", col.Name))

		if !col.PrimaryKey {
			compareConds = append(compareConds, fmt.Sprintf("target.%[1]s != source.%[1]s", col.Name))
			compareCondsS1T1 = append(compareCondsS1T1, fmt.Sprintf("t.%[1]s != s.%[1]s", col.Name))
		}

		// For t_new SELECT
		tNewSelectCols = append(tNewSelectCols, fmt.Sprintf("t.%s", col.Name))
		// For inserts SELECT
		insertsSelectCols = append(insertsSelectCols, fmt.Sprintf("s.%s", col.Name))
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	// Add system columns
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, "CURRENT_TIMESTAMP()", "TIMESTAMP('9999-12-31 23:59:59')", "TRUE")

	// Generate ON condition for joins
	onConds := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%[1]s = s.%[1]s", pk)
	}
	joinCondition := strings.Join(onConds, " AND ")
	fullCompareCondition := strings.Join(compareCondsS1T1, " OR ")
	//whereCondition := fmt.Sprintf("s.%s IS NOT NULL", primaryKeys[0]) // Use any non-null check

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
      WHEN %s THEN CURRENT_TIMESTAMP()
      ELSE t._valid_until 
    END AS _valid_until,
    CASE 
      WHEN %s THEN FALSE 
      ELSE t._is_current 
    END AS _is_current
  FROM current_data t
  LEFT JOIN source s ON %s
),
inserts AS (
  SELECT 
    %s,
    CURRENT_TIMESTAMP() AS _valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
    TRUE AS _is_current
  FROM source s
  LEFT JOIN current_data t ON %s
  WHERE %s
)
SELECT * FROM t_new
UNION ALL
SELECT * FROM inserts;
`,
		tbl,
		strings.TrimSpace(query),
		tbl,
		strings.Join(tNewSelectCols, ",\n    "),
		fullCompareCondition,
		fullCompareCondition,
		joinCondition,
		strings.Join(insertsSelectCols, ",\n    "),
		joinCondition,
		fullCompareCondition,
	)
	print(queryStr + "\n")
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
	tbl := fmt.Sprintf("%s", asset.Name)
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
	tbl := fmt.Sprintf("%s", asset.Name)
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
