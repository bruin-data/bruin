package mssql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

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
		pipeline.MaterializationStrategyTruncateInsert: ansisql.BuildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
	},
}

func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        fullRefresh,
	}
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR ALTER VIEW %s AS\n%s", asset.Name, query), nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization

	if len(mat.ClusterBy) > 0 {
		return "", errors.New("MsSQL assets do not support `cluster_by`")
	}
	query = strings.TrimSuffix(query, ";")
	queries := []string{
		"BEGIN TRANSACTION",
		"DROP TABLE IF EXISTS " + task.Name,
		fmt.Sprintf("SELECT tmp.* INTO %s FROM (%s) AS tmp", task.Name, query),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", asset.Name, query), nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("SELECT alias.* INTO %s FROM (%s\n) AS alias", tempTableName, query),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	mergeColumns := ansisql.GetColumnsWithMergeLogic(asset)
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", key, key))
	}
	onQuery := strings.Join(on, " AND ")

	allColumnValues := strings.Join(columnNames, ", ")

	whenMatchedThenQuery := ""

	if len(mergeColumns) > 0 {
		matchedUpdateStatements := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			if col.MergeSQL != "" {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("target.%s = %s", col.Name, col.MergeSQL))
			} else {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("target.%s = source.%s", col.Name, col.Name))
			}
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

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp && asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return "", errors.New("time_granularity must be either 'date', or 'timestamp'")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'`,
			asset.Name,
			asset.Materialization.IncrementalKey,
			startVar,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			asset.Name,
			strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quotedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		quotedParts = append(quotedParts, "["+strings.ReplaceAll(part, "]", "]]")+"]")
	}

	return strings.Join(quotedParts, ".")
}

func sqlStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	nameParts := strings.Split(asset.Name, ".")
	queries := make([]string, 0, 2)
	if len(nameParts) == 2 {
		schemaName := nameParts[0]
		queries = append(queries, fmt.Sprintf(
			"IF SCHEMA_ID(%s) IS NULL\n    EXEC(N'CREATE SCHEMA %s')",
			sqlStringLiteral(schemaName),
			strings.ReplaceAll(quoteIdentifier(schemaName), "'", "''"),
		))
	}

	columnDefs := make([]string, 0, len(asset.Columns)+1)
	primaryKeys := make([]string, 0)
	foreignKeys := make([]string, 0)
	for _, col := range asset.Columns {
		if col.Type == "" {
			return "", fmt.Errorf("materialization strategy %s requires column %q to have a type", asset.Materialization.Strategy, col.Name)
		}

		def := fmt.Sprintf("    %s %s", quoteIdentifier(col.Name), col.SQLType())
		if col.Collation != "" {
			def += " COLLATE " + col.Collation
		}
		if col.PrimaryKey || !col.Nullable.Bool() {
			def += " NOT NULL"
		}
		if col.Default != "" {
			def += " DEFAULT " + col.Default
		}
		columnDefs = append(columnDefs, def)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quoteIdentifier(col.Name))
		}
		if col.ForeignKey != nil && col.ForeignKey.Table != "" && col.ForeignKey.Column != "" {
			foreignKeys = append(foreignKeys, fmt.Sprintf("    FOREIGN KEY (%s) REFERENCES %s (%s)",
				quoteIdentifier(col.Name), quoteIdentifier(col.ForeignKey.Table), quoteIdentifier(col.ForeignKey.Column)))
		}
	}

	if len(primaryKeys) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	columnDefs = append(columnDefs, foreignKeys...)

	createTable := fmt.Sprintf(
		"IF OBJECT_ID(%s, N'U') IS NULL\nBEGIN\nCREATE TABLE %s (\n%s\n)\nEND",
		sqlStringLiteral(asset.Name),
		quoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	)
	queries = append(queries, createTable)

	return strings.Join(queries, ";\n") + ";", nil
}
