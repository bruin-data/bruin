package mysql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
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
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   errorMaterializer,
		pipeline.MaterializationStrategySCD2ByTime:     errorMaterializer,
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
	if asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime ||
		asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn {
		return "", fmt.Errorf("materialization strategy %s is not supported during full refresh for MySQL", asset.Materialization.Strategy)
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

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	columnNames := asset.ColumnNames()
	mergeColumns := ansisql.GetColumnsWithMergeLogic(asset)

	trimmedQuery := strings.TrimSpace(query)
	trimmedQuery = strings.TrimSuffix(trimmedQuery, ";")

	selectColumns := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		selectColumns = append(selectColumns, "source."+col)
	}

	insertColumns := strings.Join(columnNames, ", ")
	selectClause := strings.Join(selectColumns, ", ")

	tempTableName := "__bruin_merge_tmp_" + helpers.PrefixGenerator()
	onClause := buildJoinConditions(primaryKeys, "target", "source")

	queries := []string{
		"START TRANSACTION",
		fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS %s", tempTableName),
		fmt.Sprintf("CREATE TEMPORARY TABLE %s AS\n%s", tempTableName, trimmedQuery),
	}

	if len(mergeColumns) > 0 {
		assignments := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			expr := "source." + col.Name
			if col.MergeSQL != "" {
				expr = col.MergeSQL
			}
			assignments = append(assignments, fmt.Sprintf("source.%s = %s", col.Name, expr))
		}

		updateStmt := fmt.Sprintf(
			"UPDATE %s AS source JOIN %s AS target ON %s SET %s",
			tempTableName,
			asset.Name,
			onClause,
			strings.Join(assignments, ", "),
		)
		queries = append(queries, updateStmt)
	}

	deleteStmt := fmt.Sprintf(
		"DELETE target FROM %s AS target JOIN %s AS source ON %s",
		asset.Name,
		tempTableName,
		onClause,
	)

	insertStmt := fmt.Sprintf(
		"INSERT INTO %s (%s)\nSELECT %s\nFROM %s AS source",
		asset.Name,
		insertColumns,
		selectClause,
		tempTableName,
	)

	queries = append(queries,
		deleteStmt,
		insertStmt,
		fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS %s", tempTableName),
		"COMMIT",
	)

	return strings.Join(queries, ";\n") + ";", nil
}

func buildJoinConditions(keys []string, leftAlias, rightAlias string) string {
	conditions := make([]string, len(keys))
	for i, key := range keys {
		conditions[i] = fmt.Sprintf("%s.%s = %s.%s", leftAlias, key, rightAlias, key)
	}
	return strings.Join(conditions, " AND ")
}
