package clickhouse

import (
	"errors"
	"fmt"
	"strings"

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
		pipeline.MaterializationStrategyNone:          buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:        buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace: buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:  buildIncrementalQuery,
		pipeline.MaterializationStrategyMerge:         errorMaterializer,
		pipeline.MaterializationStrategyTimeInterval:  buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:           buildDDLQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return nil, fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query)}, nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{fmt.Sprintf("INSERT INTO %s %s", asset.Name, query)}, nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) ([]string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return nil, fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	if len(task.Columns) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", strategy)
	}

	primaryKeys := task.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) != 1 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at EXACTLY one column", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		fmt.Sprintf(
			"CREATE TABLE %s PRIMARY KEY %s AS %s",
			tempTableName,
			task.ColumnNamesWithPrimaryKey()[0],
			query,
		),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
	}

	return queries, nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) ([]string, error) {
	if len(task.Columns) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", task.Materialization.Strategy)
	}

	primaryKeys := task.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) != 1 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at EXACTLY one column", task.Materialization.Strategy)
	}

	query = strings.TrimSuffix(query, ";")

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	return []string{
		fmt.Sprintf(
			"CREATE TABLE %s PRIMARY KEY %s AS %s",
			tempTableName,
			task.ColumnNamesWithPrimaryKey()[0],
			query,
		),
		"DROP TABLE IF EXISTS " + task.Name,
		fmt.Sprintf("RENAME TABLE %s TO %s", tempTableName, task.Name),
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
	primaryKeys := ""

	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)

		if col.Description != "" {
			def += fmt.Sprintf(" COMMENT '%s'", col.Description)
		}
		if col.PrimaryKey {
			if primaryKeys != "" {
				primaryKeys += ", "
			}
			primaryKeys += col.Name
		}
		columnDefs = append(columnDefs, def)
	}

	if len(primaryKeys) > 0 {
		primaryKeys = fmt.Sprintf("\nPRIMARY KEY (%s)", primaryKeys)
	}

	partitionBy := ""
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf("\nPARTITION BY (%s)", asset.Materialization.PartitionBy)
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n"+
		"%s\n"+
		")"+
		"%s"+
		"%s",
		asset.Name,
		strings.Join(columnDefs, ",\n"),
		primaryKeys,
		partitionBy,
	)

	return []string{ddl}, nil
}
