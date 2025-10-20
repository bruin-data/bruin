package databricks

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
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
	// Use the shared ansisql implementation and split the result into individual queries
	result, err := ansisql.BuildTruncateInsertQuery(task, query)
	if err != nil {
		return nil, err
	}

	// Split the combined query into individual statements for Databricks
	// Remove the trailing semicolon and split by ";\n"
	result = strings.TrimSuffix(result, ";")
	queries := strings.Split(result, ";\n")

	// Clean up each query
	for i := range queries {
		queries[i] = strings.TrimSpace(queries[i])
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

	whenMatchedThenQuery := ""

	if len(nonPrimaryKeys) > 0 {
		matchedUpdateStatements := make([]string, 0, len(nonPrimaryKeys))
		for _, col := range nonPrimaryKeys {
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", col, col))
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", asset.Name),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, allColumnValues),
	}

	return mergeLines, nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) ([]string, error) {
	mat := task.Materialization

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
