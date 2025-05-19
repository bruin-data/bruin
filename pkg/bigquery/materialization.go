package bigquery

import (
	"fmt"
	"strings"

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
		pipeline.MaterializationStrategyNone:          buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:        buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace: buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:  buildIncrementalQuery,
		pipeline.MaterializationStrategyMerge:         mergeMaterializer,
		pipeline.MaterializationStrategyTimeInterval:  buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:           BuildDDLQuery,
	},
}

func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        fullRefresh,
	}
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query), nil
}

func mergeMaterializer(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	nonPrimaryKeys := asset.ColumnNamesWithUpdateOnMerge()
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", key, key))
	}
	onQuery := strings.Join(on, " AND ")

	allColumnValues := strings.Join(columnNames, ", ")

	whenMatchedThenQuery := ""

	if len(nonPrimaryKeys) > 0 {
		matchedUpdateStatements := make([]string, 0, len(nonPrimaryKeys))
		for _, col := range nonPrimaryKeys {
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("target.%s = source.%s", col, col))
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE %s target", asset.Name),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
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
	if foundCol == nil {
		return buildIncrementalQueryWithoutTempVariable(asset, query)
	}

	randPrefix := helpers.PrefixGenerator()
	tempTableName := "__bruin_tmp_" + randPrefix

	declaredVarName := "distinct_keys_" + randPrefix
	queries := []string{
		fmt.Sprintf("DECLARE %s array<%s>", declaredVarName, foundCol.Type),
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s\n", tempTableName, query),
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
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s\n", tempTableName, query),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", asset.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", asset.Name, tempTableName),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization

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

func BuildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)

		if col.Description != "" {
			def += fmt.Sprintf(` OPTIONS(description=%q)`, col.Description) // %q escapes "
		}
		columnDefs = append(columnDefs, def)
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
