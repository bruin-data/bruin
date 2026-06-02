package flightsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// NewMaterializer builds a materializer for the default (Dremio) dialect. It is
// used by preview commands (render / render-ddl) that do not resolve a specific
// connection. The run path uses NewMaterializerForDialect via the operator so
// that each connection's configured dialect is honored.
func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return NewMaterializerForDialect("dremio", fullRefresh)
}

// NewMaterializerForDialect builds a materializer for a specific Flight SQL
// dialect (see dialect.go). An unknown dialect falls back to the ANSI baseline.
func NewMaterializerForDialect(dialect string, fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMapForDialect(dialectByName(dialect)),
		FullRefresh:        fullRefresh,
	}
}

func matMapForDialect(d sqlDialect) pipeline.AssetMaterializationMap {
	b := queryBuilder{dialect: d}
	return pipeline.AssetMaterializationMap{
		pipeline.MaterializationTypeView: {
			pipeline.MaterializationStrategyNone:          b.viewMaterializer,
			pipeline.MaterializationStrategyAppend:        b.errorMaterializer,
			pipeline.MaterializationStrategyCreateReplace: b.errorMaterializer,
			pipeline.MaterializationStrategyDeleteInsert:  b.errorMaterializer,
		},
		pipeline.MaterializationTypeTable: {
			pipeline.MaterializationStrategyNone:           b.buildCreateReplaceQuery,
			pipeline.MaterializationStrategyAppend:         b.buildAppendQuery,
			pipeline.MaterializationStrategyCreateReplace:  b.buildCreateReplaceQuery,
			pipeline.MaterializationStrategyDeleteInsert:   b.buildIncrementalQuery,
			pipeline.MaterializationStrategyTruncateInsert: b.buildTruncateInsertQuery,
			pipeline.MaterializationStrategyMerge:          b.errorMaterializer,
			pipeline.MaterializationStrategyTimeInterval:   b.buildTimeIntervalQuery,
			pipeline.MaterializationStrategyDDL:            b.buildDDLQuery,
			pipeline.MaterializationStrategySCD2ByColumn:   b.errorMaterializer,
			pipeline.MaterializationStrategySCD2ByTime:     b.errorMaterializer,
		},
	}
}

// queryBuilder renders materialization queries using a specific SQL dialect.
type queryBuilder struct {
	dialect sqlDialect
}

func (b queryBuilder) errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func (b queryBuilder) viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", b.dialect.quoteIdentifier(asset.Name), query), nil
}

func (b queryBuilder) buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	return fmt.Sprintf("INSERT INTO %s %s", b.dialect.quoteIdentifier(asset.Name), query), nil
}

func (b queryBuilder) buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	name := b.dialect.quoteIdentifier(asset.Name)
	return fmt.Sprintf(`
DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
%s;`, name, name, query), nil
}

func (b queryBuilder) buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", mat.Strategy)
	}

	query = strings.TrimSuffix(query, ";")
	key := mat.IncrementalKey
	name := b.dialect.quoteIdentifier(asset.Name)

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

func (b queryBuilder) buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimSuffix(query, ";")
	name := b.dialect.quoteIdentifier(asset.Name)
	return fmt.Sprintf("TRUNCATE TABLE %s;\nINSERT INTO %s %s;", name, name, query), nil
}

func (b queryBuilder) buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
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
	name := b.dialect.quoteIdentifier(asset.Name)

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

func (b queryBuilder) buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
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
		b.dialect.quoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	), nil
}
