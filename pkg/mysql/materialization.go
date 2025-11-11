package mysql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

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
		pipeline.MaterializationStrategyMerge:         errorMaterializer,
		pipeline.MaterializationStrategyDDL:           errorMaterializer,
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
		pipeline.MaterializationStrategySCD2ByColumn:   errorMaterializer,
		pipeline.MaterializationStrategySCD2ByTime:     errorMaterializer,
	},
}

func errorMaterializer(asset *pipeline.Asset, _ string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s",
		asset.Materialization.Strategy,
		asset.Materialization.Type,
		asset.Type,
	)
}

// QuoteIdentifier quotes MySQL identifiers using backticks while preserving dotted identifiers.
func QuoteIdentifier(identifier string) string {
	if identifier == "" {
		return identifier
	}

	parts := strings.Split(identifier, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		escaped := strings.ReplaceAll(part, "`", "``")
		quoted[i] = fmt.Sprintf("`%s`", escaped)
	}

	return strings.Join(quoted, ".")
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", QuoteIdentifier(asset.Name), query), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", QuoteIdentifier(asset.Name), query), nil
}

func buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()
	quotedIncrementalKey := QuoteIdentifier(mat.IncrementalKey)

	queries := []string{
		"START TRANSACTION",
		fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS %s", QuoteIdentifier(tempTableName)),
		fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", QuoteIdentifier(tempTableName), strings.TrimSuffix(query, ";")),
		fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT DISTINCT %s FROM %s)", QuoteIdentifier(asset.Name), quotedIncrementalKey, quotedIncrementalKey, QuoteIdentifier(tempTableName)),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", QuoteIdentifier(asset.Name), QuoteIdentifier(tempTableName)),
		fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS %s", QuoteIdentifier(tempTableName)),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	queries := []string{
		"START TRANSACTION",
		"TRUNCATE TABLE " + QuoteIdentifier(asset.Name),
		fmt.Sprintf("INSERT INTO %s %s", QuoteIdentifier(asset.Name), strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime ||
		asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn {
		return "", fmt.Errorf("materialization strategy %s is not supported during full refresh for MySQL", asset.Materialization.Strategy)
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
%s;`,
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(asset.Name),
		query,
	), nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	columnNames := asset.ColumnNames()
	if len(columnNames) == 0 {
		return "", errors.New("no columns defined on asset")
	}

	insertColumns := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		insertColumns = append(insertColumns, QuoteIdentifier(col))
	}

	nonPKColumns := make([]pipeline.Column, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			continue
		}
		nonPKColumns = append(nonPKColumns, col)
	}

	updateColumns := getColumnsForMerge(nonPKColumns)
	updateAssignments := make([]string, 0, len(updateColumns))
	for _, col := range updateColumns {
		target := QuoteIdentifier(col.Name)
		updateExpr := fmt.Sprintf("VALUES(%s)", target)
		if col.MergeSQL != "" {
			updateExpr = rewriteMergeExpression(col.MergeSQL, asset)
		}
		updateAssignments = append(updateAssignments, fmt.Sprintf("%s = %s", target, updateExpr))
	}

	if len(updateAssignments) == 0 {
		return "", errors.New("materialization strategy merge requires at least one non-primary key column to update")
	}

	insertClause := fmt.Sprintf("INSERT INTO %s (%s)", QuoteIdentifier(asset.Name), strings.Join(insertColumns, ", "))
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	return fmt.Sprintf(`%s
%s
ON DUPLICATE KEY UPDATE %s;`,
		insertClause,
		query,
		strings.Join(updateAssignments, ", "),
	), nil
}

func getColumnsForMerge(cols []pipeline.Column) []pipeline.Column {
	customCols := make([]pipeline.Column, 0, len(cols))
	for _, col := range cols {
		if col.MergeSQL != "" || col.UpdateOnMerge {
			customCols = append(customCols, col)
		}
	}
	if len(customCols) > 0 {
		return customCols
	}
	return cols
}

func rewriteMergeExpression(expr string, asset *pipeline.Asset) string {
	replacements := []struct {
		old string
		new string
	}{}

	for _, col := range asset.Columns {
		colName := col.Name
		quotedCol := QuoteIdentifier(colName)
		replacements = append(replacements,
			struct {
				old string
				new string
			}{fmt.Sprintf(`source."%s"`, colName), fmt.Sprintf("VALUES(%s)", quotedCol)},
			struct {
				old string
				new string
			}{fmt.Sprintf("source.%s", colName), fmt.Sprintf("VALUES(%s)", quotedCol)},
			struct {
				old string
				new string
			}{fmt.Sprintf("source.`%s`", colName), fmt.Sprintf("VALUES(%s)", quotedCol)},
			struct {
				old string
				new string
			}{fmt.Sprintf(`target."%s"`, colName), quotedCol},
			struct {
				old string
				new string
			}{fmt.Sprintf("target.%s", colName), quotedCol},
			struct {
				old string
				new string
			}{fmt.Sprintf("target.`%s`", colName), quotedCol},
		)
	}

	result := expr
	for _, repl := range replacements {
		result = strings.ReplaceAll(result, repl.old, repl.new)
	}

	return result
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}

	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp ||
		asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return "", errors.New("time_granularity must be either 'date' or 'timestamp'")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	queries := []string{
		"START TRANSACTION",
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'`,
			QuoteIdentifier(asset.Name),
			QuoteIdentifier(asset.Materialization.IncrementalKey),
			startVar,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			QuoteIdentifier(asset.Name),
			strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, _ string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", errors.New("DDL strategy requires `columns` to be specified")
	}

	columnDefs := make([]string, 0, len(asset.Columns))
	primaryKeys := make([]string, 0)

	for _, col := range asset.Columns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, QuoteIdentifier(col.Name))
		}

		definition := fmt.Sprintf("%s %s", QuoteIdentifier(col.Name), col.Type)
		if col.Nullable.Value != nil && !*col.Nullable.Value {
			definition += " NOT NULL"
		}

		if col.Description != "" {
			comment := strings.ReplaceAll(col.Description, `'`, `''`)
			definition += fmt.Sprintf(" COMMENT '%s'", comment)
		}

		columnDefs = append(columnDefs, definition)
	}

	if len(primaryKeys) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
		QuoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	), nil
}
