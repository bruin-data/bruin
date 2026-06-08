package dremio

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// quoteIdentifier quotes a possibly dotted identifier (schema.table) using ANSI
// double quotes. Dremio's SQL is ANSI compatible, so double-quoting identifiers
// is the correct behaviour.
func quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = fmt.Sprintf(`"%s"`, part)
	}
	return strings.Join(quoted, ".")
}

// NewMaterializer builds a materializer that renders Dremio (ANSI) SQL.
func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap(),
		FullRefresh:        fullRefresh,
	}
}

func matMap() pipeline.AssetMaterializationMap {
	return pipeline.AssetMaterializationMap{
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
			pipeline.MaterializationStrategyMerge:          errorMaterializer,
			pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
			pipeline.MaterializationStrategyDDL:            buildDDLQuery,
			pipeline.MaterializationStrategySCD2ByColumn:   errorMaterializer,
			pipeline.MaterializationStrategySCD2ByTime:     errorMaterializer,
		},
	}
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", quoteIdentifier(asset.Name), query), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	return fmt.Sprintf("INSERT INTO %s %s", quoteIdentifier(asset.Name), query), nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	name := quoteIdentifier(asset.Name)
	return fmt.Sprintf(`
DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
%s;`, name, name, query), nil
}

func buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", mat.Strategy)
	}

	query = strings.TrimSuffix(query, ";")
	key := mat.IncrementalKey
	name := quoteIdentifier(asset.Name)

	return fmt.Sprintf(`
DELETE FROM %s
WHERE %s IN (
    SELECT DISTINCT %s
    FROM (%s) AS new_data
);

INSERT INTO %s
SELECT * FROM (%s) AS new_data;`,
		name, key, key, query, name, query,
	), nil
}

func buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	name := quoteIdentifier(asset.Name)
	return fmt.Sprintf("TRUNCATE TABLE %s;\nINSERT INTO %s %s;", name, name, query), nil
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
	name := quoteIdentifier(asset.Name)

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
		name, key, timePrefix, startVar, timePrefix, endVar, name, query,
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
			desc := strings.ReplaceAll(col.Description, `'`, `''`)
			def += fmt.Sprintf(" COMMENT '%s'", desc)
		}
		columnDefs = append(columnDefs, def)
	}

	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
		quoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	), nil
}
