package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// QuoteIdentifier quotes a PostgreSQL identifier (table, column, etc.) to handle case-sensitive names.
// It splits the identifier on "." and quotes each part separately.
// For example, "schema.MyTable" becomes "\"schema\".\"MyTable\"".
func QuoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quotedParts := make([]string, len(parts))
	for i, part := range parts {
		quotedParts[i] = fmt.Sprintf(`"%s"`, part)
	}
	return strings.Join(quotedParts, ".")
}

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

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", QuoteIdentifier(asset.Name), query), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", QuoteIdentifier(asset.Name), query), nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()
	quotedIncrementalKey := QuoteIdentifier(mat.IncrementalKey)

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s\n", tempTableName, query),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", QuoteIdentifier(task.Name), quotedIncrementalKey, quotedIncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", QuoteIdentifier(task.Name), tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildTruncateInsertQuery(task *pipeline.Asset, query string) (string, error) {
	queries := []string{
		"BEGIN TRANSACTION",
		"TRUNCATE TABLE " + QuoteIdentifier(task.Name),
		fmt.Sprintf("INSERT INTO %s %s", QuoteIdentifier(task.Name), strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func getColumnsWithMergeLogic(asset *pipeline.Asset) []pipeline.Column {
	var columns []pipeline.Column
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			continue
		}
		if col.MergeSQL != "" || col.UpdateOnMerge {
			columns = append(columns, col)
		}
	}
	return columns
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		return buildRedshiftMergeQuery(asset, query)
	}

	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	mergeColumns := getColumnsWithMergeLogic(asset)
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", QuoteIdentifier(key), QuoteIdentifier(key)))
	}
	onQuery := strings.Join(on, " AND ")

	// Quote all column names for INSERT clause
	quotedColumnNames := make([]string, 0, len(columnNames))
	quotedColumnValues := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		quotedColumnNames = append(quotedColumnNames, QuoteIdentifier(col))
		quotedColumnValues = append(quotedColumnValues, QuoteIdentifier(col))
	}
	allColumnNamesStr := strings.Join(quotedColumnNames, ", ")
	allColumnValuesStr := strings.Join(quotedColumnValues, ", ")

	whenMatchedThenQuery := ""

	if len(mergeColumns) > 0 {
		matchedUpdateStatements := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			if col.MergeSQL != "" {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = %s", QuoteIdentifier(col.Name), col.MergeSQL))
			} else {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", QuoteIdentifier(col.Name), QuoteIdentifier(col.Name)))
			}
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", QuoteIdentifier(asset.Name)),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnNamesStr, allColumnValuesStr),
	}

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildRedshiftMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	mergeColumns := getColumnsWithMergeLogic(asset)
	columnNames := asset.ColumnNames()

	// In Redshift MERGE, target table doesn't have an alias, so we use the table name directly
	targetTableName := QuoteIdentifier(asset.Name)

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("%s.%s = source.%s", targetTableName, QuoteIdentifier(key), QuoteIdentifier(key)))
	}
	onQuery := strings.Join(on, " AND ")

	// Quote all column names for INSERT clause
	quotedColumnNames := make([]string, 0, len(columnNames))
	quotedColumnValues := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		quotedColumnNames = append(quotedColumnNames, QuoteIdentifier(col))
		quotedColumnValues = append(quotedColumnValues, "source."+QuoteIdentifier(col))
	}
	allColumnNamesStr := strings.Join(quotedColumnNames, ", ")
	allColumnValuesStr := strings.Join(quotedColumnValues, ", ")

	whenMatchedThenQuery := ""

	if len(mergeColumns) > 0 {
		matchedUpdateStatements := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			if col.MergeSQL != "" {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = %s", QuoteIdentifier(col.Name), col.MergeSQL))
			} else {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", QuoteIdentifier(col.Name), QuoteIdentifier(col.Name)))
			}
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	mergeLines := []string{
		"MERGE INTO " + targetTableName,
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnNamesStr, allColumnValuesStr),
	}

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) (string, error) {
	switch {
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimefullRefresh(task, query)
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnfullRefresh(task, query)
	default:
		query = strings.TrimSuffix(query, ";")
		return fmt.Sprintf(
			`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s; 
CREATE TABLE %s AS %s;
COMMIT;`, QuoteIdentifier(task.Name), QuoteIdentifier(task.Name), query), nil
	}
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}
	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp || asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return "", errors.New("time_granularity must be either 'date', or 'timestamp'")
	}
	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'`,
			QuoteIdentifier(asset.Name),
			quotedIncrementalKey,
			startVar,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			QuoteIdentifier(asset.Name),
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
		quotedColName := QuoteIdentifier(col.Name)
		def := fmt.Sprintf("%s %s", quotedColName, col.Type)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
		columnDefs = append(columnDefs, def)

		if col.Description != "" {
			comment := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", QuoteIdentifier(asset.Name), quotedColName, strings.ReplaceAll(col.Description, "'", "''"))
			columnComments = append(columnComments, comment)
		}
	}

	if len(primaryKeys) > 0 {
		primaryKeyClause := fmt.Sprintf("primary key (%s)", strings.Join(primaryKeys, ", "))
		columnDefs = append(columnDefs, primaryKeyClause)
	}

	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n"+
		"%s\n)",
		QuoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	)

	if len(columnComments) > 0 {
		q += ";\n" + strings.Join(columnComments, "\n")
	}

	return q, nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	var validuntil string
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		validuntil = "TIMESTAMP '9999-12-31 00:00:00'"
	} else {
		validuntil = "'9999-12-31 00:00:00'::TIMESTAMP"
	}

	stmt := fmt.Sprintf(
		`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
SELECT
  CURRENT_TIMESTAMP AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;
COMMIT;`,
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(asset.Name),
		validuntil,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	var validuntil string
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		validuntil = "TIMESTAMP '9999-12-31 00:00:00'"
	} else {
		validuntil = "'9999-12-31 00:00:00'::TIMESTAMP"
	}

	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	stmt := fmt.Sprintf(
		`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
SELECT
  %s AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;
COMMIT;`,
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(asset.Name),
		quotedIncrementalKey,
		validuntil,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		return buildRedshiftSCD2ByColumnQuery(asset, query)
	}

	query = strings.TrimRight(query, ";")
	var (
		primaryKeys      = make([]string, 0, 4)
		compareConds     = make([]string, 0, 12)
		compareCondsS1T1 = make([]string, 0, 4)
		insertCols       = make([]string, 0, 12)
		insertValues     = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		quotedColName := QuoteIdentifier(col.Name)
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		insertCols = append(insertCols, quotedColName)
		insertValues = append(insertValues, "source."+quotedColName)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%s != source.%s", quotedColName, quotedColName))
			compareCondsS1T1 = append(compareCondsS1T1,
				fmt.Sprintf("t1.%s != s1.%s", quotedColName, quotedColName))
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, "CURRENT_TIMESTAMP", "'9999-12-31 00:00:00'::TIMESTAMP", "TRUE")
	pkList := strings.Join(primaryKeys, ", ")

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
    _valid_until = CURRENT_TIMESTAMP,
    _is_current  = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP,
    _is_current  = FALSE

WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		QuoteIdentifier(asset.Name),
		strings.TrimSpace(query),
		QuoteIdentifier(asset.Name),
		pkList,
		whereCondition,
		onCondition,
		matchedCondition,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

func buildSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
	// Route to Redshift-specific implementation for Redshift assets
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		return buildRedshiftSCD2QueryByTime(asset, query)
	}

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
		quotedColName := QuoteIdentifier(col.Name)
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
		insertCols = append(insertCols, quotedColName)
		insertValues = append(insertValues, "source."+quotedColName)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}
	pkList := strings.Join(primaryKeys, ", ")
	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues,
		"source."+quotedIncrementalKey,
		"'9999-12-31 00:00:00'",
		"TRUE",
	)

	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	joinConds = append(joinConds, "target._is_current AND source._is_current")
	onCondition := strings.Join(joinConds, " AND ")
	tbl := asset.Name

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

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP,
    _is_current  = FALSE

WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		QuoteIdentifier(tbl),
		strings.TrimSpace(query),
		QuoteIdentifier(tbl),
		pkList,
		quotedIncrementalKey,
		onCondition,
		quotedIncrementalKey,
		quotedIncrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

// Redshift-specific SCD2 functions - Redshift has different SQL syntax requirements compared to PostgreSQL.
func buildRedshiftSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")
	var (
		primaryKeys  = make([]string, 0, 4)
		compareConds = make([]string, 0, 12)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		quotedColName := QuoteIdentifier(col.Name)
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		insertCols = append(insertCols, quotedColName)
		insertValues = append(insertValues, "source."+quotedColName)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%s != source.%s", quotedColName, quotedColName))
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, "(SELECT session_timestamp FROM _ts)", "TIMESTAMP '9999-12-31 00:00:00'", "TRUE")

	// Build ON condition for MERGE
	onConditions := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onCondition := strings.Join(onConditions, " AND ")

	tempTableName := "__bruin_scd2_tmp_" + helpers.PrefixGenerator()

	var matchedCondition string
	if len(compareConds) > 0 {
		matchedCondition = strings.Join(compareConds, " OR ")
	} else {
		matchedCondition = "FALSE"
	}

	// Step 1: Create temp table with new data
	// Step 2: Update existing records that changed
	// Step 3: Insert new/changed records
	// Step 4: Update records not in source (expired)
	queryStr := fmt.Sprintf(`
BEGIN TRANSACTION;

-- Capture the timestamp once for the entire transaction
CREATE TEMP TABLE _ts AS 
SELECT CURRENT_TIMESTAMP AS session_timestamp;

-- Create temp table with source data
CREATE TEMP TABLE %s AS 
SELECT *, TRUE AS _is_current FROM (%s) AS src;

-- Update existing records that have changes
UPDATE %s AS target
SET _valid_until = (SELECT session_timestamp FROM _ts), _is_current = FALSE
WHERE target._is_current = TRUE
  AND EXISTS (
    SELECT 1 FROM %s AS source
    WHERE %s AND (%s)
  );

-- Update records that are no longer in source (expired)
UPDATE %s AS target
SET _valid_until = (SELECT session_timestamp FROM _ts), _is_current = FALSE
WHERE target._is_current = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM %s AS source
    WHERE %s
  );

-- Insert new records and new versions of changed records
INSERT INTO %s (%s)
SELECT %s
FROM %s AS source
WHERE NOT EXISTS (
  SELECT 1 FROM %s AS target 
  WHERE %s AND target._is_current = TRUE
);

DROP TABLE %s;
COMMIT;`,
		tempTableName,
		strings.TrimSpace(query),
		QuoteIdentifier(asset.Name),
		tempTableName,
		onCondition,
		matchedCondition,
		QuoteIdentifier(asset.Name),
		tempTableName,
		onCondition,
		QuoteIdentifier(asset.Name),
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
		tempTableName,
		QuoteIdentifier(asset.Name),
		onCondition,
		tempTableName,
	)

	return strings.TrimSpace(queryStr), nil
}

func buildRedshiftSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
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
		quotedColName := QuoteIdentifier(col.Name)
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
		insertCols = append(insertCols, quotedColName)
		insertValues = append(insertValues, "source."+quotedColName)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}
	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues,
		"source."+quotedIncrementalKey,
		"TIMESTAMP '9999-12-31 00:00:00'",
		"TRUE",
	)

	// Build ON condition
	onConditions := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onCondition := strings.Join(onConditions, " AND ")

	tempTableName := "__bruin_scd2_time_tmp_" + helpers.PrefixGenerator()

	queryStr := fmt.Sprintf(`
BEGIN TRANSACTION;

-- Create temp table with source data
CREATE TEMP TABLE %s AS 
SELECT *, TRUE AS _is_current FROM (%s) AS src;

-- Update existing records where source timestamp is newer
UPDATE %s AS target
SET _valid_until = source.%s, _is_current = FALSE
FROM %s AS source
WHERE %s
  AND target._is_current = TRUE
  AND target._valid_from < source.%s;

-- Update records that are no longer in source (expired)
UPDATE %s AS target
SET _valid_until = CURRENT_TIMESTAMP, _is_current = FALSE
WHERE target._is_current = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM %s AS source
    WHERE %s
  );

-- Insert new records and new versions of changed records
INSERT INTO %s (%s)
SELECT %s
FROM %s AS source
WHERE NOT EXISTS (
  SELECT 1 FROM %s AS target 
  WHERE %s AND target._is_current = TRUE
)
OR EXISTS (
  SELECT 1 FROM %s AS target
  WHERE %s AND target._is_current = FALSE 
  AND target._valid_until = source.%s
);

DROP TABLE %s;
COMMIT;`,
		tempTableName,
		strings.TrimSpace(query),
		QuoteIdentifier(asset.Name),
		quotedIncrementalKey,
		tempTableName,
		onCondition,
		quotedIncrementalKey,
		QuoteIdentifier(asset.Name),
		tempTableName,
		onCondition,
		QuoteIdentifier(asset.Name),
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
		tempTableName,
		QuoteIdentifier(asset.Name),
		onCondition,
		QuoteIdentifier(asset.Name),
		onCondition,
		quotedIncrementalKey,
		tempTableName,
	)

	return strings.TrimSpace(queryStr), nil
}
