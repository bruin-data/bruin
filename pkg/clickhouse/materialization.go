package clickhouse

import (
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
