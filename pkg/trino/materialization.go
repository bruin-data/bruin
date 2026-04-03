package trino

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func quoteIdentifier(identifier string) string {
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
	query = strings.TrimSuffix(query, ";")
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", quoteIdentifier(asset.Name), query), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", quoteIdentifier(asset.Name), query), nil
}

func buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", mat.Strategy)
	}

	query = strings.TrimSuffix(query, ";")
	key := mat.IncrementalKey

	return fmt.Sprintf(`
DELETE FROM %s 
WHERE %s IN (
    SELECT DISTINCT %s 
    FROM (%s) AS new_data
);

INSERT INTO %s
SELECT * FROM (%s) AS new_data;`,
		quoteIdentifier(asset.Name),
		key,
		key,
		query,
		quoteIdentifier(asset.Name),
		query,
	), nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	return errorMaterializer(asset, query)
}

func buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	// Trino may not support TRUNCATE for all connectors, use DELETE as fallback
	queries := []string{
		"BEGIN",
		"DELETE FROM " + asset.Name,
		fmt.Sprintf("INSERT INTO %s %s", asset.Name, strings.TrimSuffix(query, ";")),
		"COMMIT",
	}
	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	query = strings.TrimSuffix(query, ";")

	withClauses := []string{"format = 'PARQUET'"}

	if mat.PartitionBy != "" {
		withClauses = append(withClauses, fmt.Sprintf("partitioning = ARRAY['%s']", mat.PartitionBy))
	}

	withClause := ""
	if len(withClauses) > 0 {
		withClause = fmt.Sprintf("WITH (%s)", strings.Join(withClauses, ", "))
	}

	return fmt.Sprintf(`
DROP TABLE IF EXISTS %s;
CREATE TABLE %s %s AS
%s;`,
		quoteIdentifier(asset.Name),
		quoteIdentifier(asset.Name),
		withClause,
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
	if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp &&
		asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return "", errors.New("time_granularity must be either 'date', or 'timestamp'")
	}

	query = strings.TrimSuffix(query, ";")
	key := asset.Materialization.IncrementalKey

	timePrefix := "TIMESTAMP"
	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		timePrefix = "DATE"
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	return fmt.Sprintf(`
DELETE FROM %s 
WHERE %s BETWEEN %s '%s' AND %s '%s';

INSERT INTO %s
%s;`,
		quoteIdentifier(asset.Name),
		key,
		timePrefix,
		startVar,
		timePrefix,
		endVar,
		quoteIdentifier(asset.Name),
		query,
	), nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	columnDefs := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		def := fmt.Sprintf("    %s %s", col.Name, col.Type)
		if col.Description != "" {
			// Escape single quotes in descriptions
			desc := strings.ReplaceAll(col.Description, `'`, `''`)
			def += fmt.Sprintf(" COMMENT '%s'", desc)
		}
		columnDefs = append(columnDefs, def)
	}

	// Build WITH clause for table properties
	withClauses := []string{"format = 'PARQUET'"}
	if asset.Materialization.PartitionBy != "" {
		withClauses = append(withClauses, fmt.Sprintf("partitioning = ARRAY['%s']", asset.Materialization.PartitionBy))
	}

	return fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
%s
) WITH (%s)`,
		quoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
		strings.Join(withClauses, ", "),
	), nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	return errorMaterializer(asset, query)
}

func buildSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
	return errorMaterializer(asset, query)
}
