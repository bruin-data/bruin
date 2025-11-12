package mysql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildIncrementalQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          errorMaterializer,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2ByTimeQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, _ string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s",
		asset.Materialization.Strategy,
		asset.Materialization.Type,
		asset.Type,
	)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", asset.Name, query), nil
}

func buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		"START TRANSACTION",
		"DROP TEMPORARY TABLE IF EXISTS " + tempTableName,
		fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", tempTableName, strings.TrimSuffix(query, ";")),
		fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT DISTINCT %s FROM %s)", asset.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", asset.Name, tempTableName),
		"DROP TEMPORARY TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	queries := []string{
		"START TRANSACTION",
		"TRUNCATE TABLE " + asset.Name,
		fmt.Sprintf("INSERT INTO %s %s", asset.Name, strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime {
		return buildSCD2ByTimefullRefresh(asset, query)
	}
	if asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn {
		return buildSCD2ByColumnfullRefresh(asset, query)
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
%s;`,
		asset.Name,
		asset.Name,
		query,
	), nil
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}

	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp ||
		asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return "", errors.New("time_granularity must be either 'date' or 'timestamp'")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	queries := []string{
		"START TRANSACTION",
		fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'",
			asset.Name,
			asset.Materialization.IncrementalKey,
			startVar,
			endVar),
		fmt.Sprintf("INSERT INTO %s %s",
			asset.Name,
			strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, _ string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", errors.New("DDL strategy requires `columns` to be specified")
	}

	columnDefs := make([]string, 0, len(asset.Columns))
	primaryKeys := make([]string, 0)

	for _, col := range asset.Columns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}

		definition := fmt.Sprintf("%s %s", col.Name, col.Type)
		if col.Nullable.Value != nil && !*col.Nullable.Value {
			definition += " NOT NULL"
		}

		if col.Description != "" {
			comment := strings.ReplaceAll(col.Description, `'`, `''`)
			definition += fmt.Sprintf(" COMMENT '%s'", comment)
		}

		columnDefs = append(columnDefs, definition)
	}

	if len(primaryKeys) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
		asset.Name,
		strings.Join(columnDefs, ",\n"),
	), nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	incrementalKey := asset.Materialization.IncrementalKey
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	var (
		columnNames      = make([]string, 0, len(asset.Columns))
		primaryKeys      = make([]string, 0, len(asset.Columns))
		incrementalFound bool
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}

		lcType := strings.ToLower(col.Type)
		if col.Name == incrementalKey {
			incrementalFound = true
			if !strings.Contains(lcType, "timestamp") && !strings.Contains(lcType, "datetime") && lcType != "date" {
				return "", errors.New("incremental_key must be TIMESTAMP, DATETIME, or DATE in SCD2_by_time strategy")
			}
		}

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}

		columnNames = append(columnNames, col.Name)
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the primary_key field to be set on at least one column", asset.Materialization.Strategy)
	}

	if !incrementalFound {
		return "", fmt.Errorf("incremental_key %s must be present in columns", incrementalKey)
	}

	tempTableName := "__bruin_scd2_time_tmp_" + helpers.PrefixGenerator()
	timeExpr := fmt.Sprintf("CAST(source.%s AS DATETIME)", incrementalKey)

	joinConditions := make([]string, len(primaryKeys))
	currentJoinConditions := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		joinConditions[i] = fmt.Sprintf("target.%s = source.%s", pk, pk)
		currentJoinConditions[i] = fmt.Sprintf("current.%s = source.%s", pk, pk)
	}
	joinCondition := strings.Join(joinConditions, " AND ")
	currentJoinCondition := strings.Join(currentJoinConditions, " AND ")
	firstPK := primaryKeys[0]

	sourceSelectColumns := make([]string, len(columnNames))
	for i, col := range columnNames {
		sourceSelectColumns[i] = "source." + col
	}
	selectClause := strings.Join(sourceSelectColumns, ", ")
	insertColumns := append(append([]string{}, columnNames...), "_valid_from", "_valid_until", "_is_current")
	insertList := strings.Join(insertColumns, ", ")

	queries := []string{
		"START TRANSACTION",
		"DROP TEMPORARY TABLE IF EXISTS " + tempTableName,
		fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", tempTableName, query),
		fmt.Sprintf("UPDATE %s AS target JOIN %s AS source ON %s SET target._valid_until = %s, target._is_current = FALSE WHERE target._is_current = TRUE AND target._valid_from < %s",
			asset.Name, tempTableName, joinCondition, timeExpr, timeExpr),
		fmt.Sprintf("UPDATE %s AS target LEFT JOIN %s AS source ON %s SET target._valid_until = CURRENT_TIMESTAMP, target._is_current = FALSE WHERE target._is_current = TRUE AND source.%s IS NULL",
			asset.Name, tempTableName, joinCondition, firstPK),
		fmt.Sprintf("INSERT INTO %s (%s)\nSELECT %s, %s, '9999-12-31 23:59:59', TRUE\nFROM %s AS source\nLEFT JOIN %s AS current ON %s AND current._is_current = TRUE\nWHERE current.%s IS NULL OR current._valid_from < %s",
			asset.Name,
			insertList,
			selectClause,
			timeExpr,
			tempTableName,
			asset.Name,
			currentJoinCondition,
			firstPK,
			timeExpr),
		"DROP TEMPORARY TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2 strategy")
	}

	incrementalKey := asset.Materialization.IncrementalKey
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	srcCols := make([]string, 0, len(asset.Columns))
	incrementalFound := false
	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}

		lcType := strings.ToLower(col.Type)
		if col.Name == incrementalKey {
			incrementalFound = true
			if !strings.Contains(lcType, "timestamp") && !strings.Contains(lcType, "datetime") && lcType != "date" {
				return "", errors.New("incremental_key must be TIMESTAMP, DATETIME, or DATE in SCD2_by_time strategy")
			}
		}

		srcCols = append(srcCols, "src."+col.Name)
	}

	if !incrementalFound {
		return "", fmt.Errorf("incremental_key %s must be present in columns", incrementalKey)
	}

	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
SELECT
  %s,
  CAST(src.%s AS DATETIME) AS _valid_from,
  '9999-12-31 23:59:59' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;`,
		asset.Name,
		asset.Name,
		strings.Join(srcCols, ",\n  "),
		incrementalKey,
		query,
	), nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", errors.New("SCD2_by_column strategy requires `columns` to be specified")
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	var (
		columnNames             = make([]string, 0, len(asset.Columns))
		primaryKeys             = make([]string, 0, len(asset.Columns))
		changeConditionsTarget  = make([]string, 0, len(asset.Columns))
		changeConditionsCurrent = make([]string, 0, len(asset.Columns))
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}

		columnNames = append(columnNames, col.Name)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
			continue
		}

		changeConditionsTarget = append(changeConditionsTarget,
			fmt.Sprintf("NOT (target.%[1]s <=> source.%[1]s)", col.Name))
		changeConditionsCurrent = append(changeConditionsCurrent,
			fmt.Sprintf("NOT (current.%[1]s <=> source.%[1]s)", col.Name))
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	if len(changeConditionsTarget) == 0 {
		return "", errors.New("SCD2_by_column strategy requires at least one non-primary-key column")
	}

	joinConditions := make([]string, len(primaryKeys))
	currentJoinConditions := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		joinConditions[i] = fmt.Sprintf("target.%[1]s = source.%[1]s", pk)
		currentJoinConditions[i] = fmt.Sprintf("current.%[1]s = source.%[1]s", pk)
	}
	joinCondition := strings.Join(joinConditions, " AND ")
	currentJoinCondition := strings.Join(currentJoinConditions, " AND ")
	firstPK := primaryKeys[0]

	sourceSelectColumns := make([]string, len(columnNames))
	for i, col := range columnNames {
		sourceSelectColumns[i] = "source." + col
	}
	selectClause := strings.Join(sourceSelectColumns, ", ")
	insertColumns := append(append([]string{}, columnNames...), "_valid_from", "_valid_until", "_is_current")
	insertList := strings.Join(insertColumns, ", ")

	changeCondition := strings.Join(changeConditionsTarget, " OR ")
	changeConditionCurrent := strings.Join(changeConditionsCurrent, " OR ")

	tempTableName := "__bruin_scd2_col_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		"START TRANSACTION",
		"DROP TEMPORARY TABLE IF EXISTS " + tempTableName,
		fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", tempTableName, query),
		"SET @current_scd2_ts = CURRENT_TIMESTAMP",
		fmt.Sprintf("UPDATE %s AS target LEFT JOIN %s AS source ON %s SET target._valid_until = @current_scd2_ts, target._is_current = FALSE WHERE target._is_current = TRUE AND source.%s IS NULL",
			asset.Name, tempTableName, joinCondition, firstPK),
		fmt.Sprintf("UPDATE %s AS target JOIN %s AS source ON %s SET target._valid_until = @current_scd2_ts, target._is_current = FALSE WHERE target._is_current = TRUE AND (%s)",
			asset.Name, tempTableName, joinCondition, changeCondition),
		fmt.Sprintf("INSERT INTO %s (%s)\nSELECT %s, @current_scd2_ts, '9999-12-31 23:59:59', TRUE\nFROM %s AS source\nLEFT JOIN %s AS current ON %s AND current._is_current = TRUE\nWHERE current.%s IS NULL OR (%s)",
			asset.Name,
			insertList,
			selectClause,
			tempTableName,
			asset.Name,
			currentJoinCondition,
			firstPK,
			changeConditionCurrent),
		"DROP TEMPORARY TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	if len(asset.Columns) == 0 {
		return "", errors.New("SCD2_by_column strategy requires `columns` to be specified")
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	selectCols := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		selectCols = append(selectCols, "src."+col.Name)
	}

	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
SELECT
  %s,
  CURRENT_TIMESTAMP AS _valid_from,
  '9999-12-31 23:59:59' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;`,
		asset.Name,
		asset.Name,
		strings.Join(selectCols, ",\n  "),
		query,
	), nil
}
