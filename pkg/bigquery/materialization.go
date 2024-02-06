package bigquery

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

type Materializer struct{}

func (m Materializer) Render(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return query, nil
	}

	type materializerFunc func(task *pipeline.Asset, query string, mat pipeline.Materialization) (string, error)
	matMap := map[pipeline.MaterializationType]map[pipeline.MaterializationStrategy]materializerFunc{
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
		},
	}

	if matFunc, ok := matMap[mat.Type][mat.Strategy]; ok {
		return matFunc(task, query, mat)
	}

	return "", fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

func errorMaterializer(task *pipeline.Asset, query string, mat pipeline.Materialization) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s", mat.Strategy, mat.Type)
}

func viewMaterializer(task *pipeline.Asset, query string, mat pipeline.Materialization) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW `%s` AS\n%s", task.Name, query), nil
}

func mergeMaterializer(asset *pipeline.Asset, query string, mat pipeline.Materialization) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", mat.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", mat.Strategy)
	}

	nonPrimaryKeys := asset.ColumnNamesWithUpdateOnMerge()
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("T.%s = S.%s", key, key))
	}
	onQuery := strings.Join(on, " AND ")

	allColumnValues := strings.Join(columnNames, ", ")

	whenMatchedThenQuery := ""

	if len(nonPrimaryKeys) > 0 {
		matchedUpdateStatements := make([]string, 0, len(nonPrimaryKeys))
		for _, col := range nonPrimaryKeys {
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("T.%s = S.%s", col, col))
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = fmt.Sprintf("WHEN MATCHED THEN UPDATE SET %s", matchedUpdateQuery)
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE %s T", asset.Name),
		fmt.Sprintf("USING (%s) S ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, allColumnValues),
	}

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildAppendQuery(task *pipeline.Asset, query string, mat pipeline.Materialization) (string, error) {
	return fmt.Sprintf("INSERT INTO `%s` %s", task.Name, query), nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string, mat pipeline.Materialization) (string, error) {
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", mat.Strategy)
	}

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE __bruin_tmp AS %s", query),
		fmt.Sprintf("DELETE FROM `%s` WHERE `%s` in (SELECT DISTINCT `%s` FROM __bruin_tmp)", task.Name, mat.IncrementalKey, mat.IncrementalKey),
		fmt.Sprintf("INSERT INTO `%s` SELECT * FROM __bruin_tmp", task.Name),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, "\n") + ";", nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string, mat pipeline.Materialization) (string, error) {
	partitionClause := ""
	if mat.PartitionBy != "" {
		partitionClause = fmt.Sprintf("PARTITION BY `%s`", mat.PartitionBy)
	}

	clusterByClause := ""
	if len(mat.ClusterBy) > 0 {
		clusterByClause = fmt.Sprintf("CLUSTER BY `%s`", strings.Join(mat.ClusterBy, "`, `"))
	}

	return fmt.Sprintf("CREATE OR REPLACE TABLE `%s` %s %s AS\n%s", task.Name, partitionClause, clusterByClause, query), nil
}
