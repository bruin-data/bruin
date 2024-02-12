package postgres

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type randomSuffixGenerator func() string

type Materializer struct {
	prefixGenerator randomSuffixGenerator
}

func NewMaterializer() *Materializer {
	return &Materializer{
		prefixGenerator: helpers.PrefixGenerator,
	}
}

func (m Materializer) Render(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return query, nil
	}

	if mat.Type == pipeline.MaterializationTypeView {
		return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", task.Name, query), nil
	}

	if mat.Type == pipeline.MaterializationTypeTable {
		strategy := mat.Strategy
		if strategy == pipeline.MaterializationStrategyNone {
			strategy = pipeline.MaterializationStrategyCreateReplace
		}

		if strategy == pipeline.MaterializationStrategyAppend {
			return fmt.Sprintf("INSERT INTO %s %s", task.Name, query), nil
		}

		if strategy == pipeline.MaterializationStrategyCreateReplace {
			return buildCreateReplaceQuery(task, query)
		}

		if strategy == pipeline.MaterializationStrategyDeleteInsert {
			return m.buildIncrementalQuery(task, query, mat, strategy)
		}

		if strategy == pipeline.MaterializationStrategyMerge {
			return m.buildMergeQuery(task, query)
		}
	}

	return "", fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

func (m *Materializer) buildIncrementalQuery(task *pipeline.Asset, query string, mat pipeline.Materialization, strategy pipeline.MaterializationStrategy) (string, error) {
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + m.prefixGenerator()

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, query),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func (m *Materializer) buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
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
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", col, col))
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = fmt.Sprintf("WHEN MATCHED THEN UPDATE SET %s", matchedUpdateQuery)
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", asset.Name),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, allColumnValues),
	}

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	return fmt.Sprintf(
		`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s; 
CREATE TABLE %s AS %s;
COMMIT;`, task.Name, task.Name, query), nil
}
