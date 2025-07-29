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
		return buildSCD2ByTimeFullRefresh(task, query, location)
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnFullRefresh(task, query, location)
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
	return []string{}, nil
}

func buildSCD2ByColumnFullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
	return []string{}, nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	return []string{}, nil
}

func buildSCD2ByTimeFullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
	return []string{}, nil
}
