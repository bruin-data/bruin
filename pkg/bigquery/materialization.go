package bigquery

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

var (
	integerRangePartitionRegex       = regexp.MustCompile(`(?i)\brange_bucket\s*\(`)
	simpleMergePartitionRegex        = regexp.MustCompile(`(?i)^\s*` + "`?" + `([a-z_][a-z0-9_]*)` + "`?" + `\s*$`)
	dateMergePartitionRegex          = regexp.MustCompile(`(?i)^\s*date\s*\(\s*` + "`?" + `([a-z_][a-z0-9_]*)` + "`?" + `\s*\)\s*$`)
	truncatedMergePartitionRegex     = regexp.MustCompile(`(?i)^\s*(date_trunc|datetime_trunc|timestamp_trunc)\s*\(\s*` + "`?" + `([a-z_][a-z0-9_]*)` + "`?" + `\s*,\s*(day|hour|month|year)\s*\)\s*$`)
	supportedMergePartitionDataTypes = map[string]bool{
		"DATE":      true,
		"DATETIME":  true,
		"TIMESTAMP": true,
	}
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
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          mergeMaterializer,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2QueryByTime,
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
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query), nil
}

func buildTruncateInsertQuery(task *pipeline.Asset, query string) (string, error) {
	if err := validateRequirePartitionFilter(task, task.Materialization.PartitionBy != ""); err != nil {
		return "", err
	}

	// BigQuery treats TRUNCATE TABLE as a DML statement, so the truncate and insert can
	// run inside a single transaction. This makes the full refresh atomic: readers never
	// observe the intermediate empty table, and a failed insert rolls back the truncate.
	queries := []string{
		"BEGIN TRANSACTION",
		"TRUNCATE TABLE " + task.Name,
		fmt.Sprintf("INSERT INTO %s %s", task.Name, strings.TrimSuffix(query, ";")),
		"COMMIT TRANSACTION",
	}
	return strings.Join(queries, ";\n") + ";", nil
}

func mergeMaterializer(asset *pipeline.Asset, query string) (string, error) {
	if err := validateRequirePartitionFilter(asset, asset.Materialization.PartitionBy != ""); err != nil {
		return "", err
	}

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
		on = append(on, fmt.Sprintf("(source.%s = target.%s OR (source.%s IS NULL and target.%s IS NULL))", key, key, key, key))
	}
	on = ansisql.AddIncrementalPredicate(on, asset.Materialization.IncrementalPredicate)
	onQuery := strings.Join(on, " AND ")

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

	if requirePartitionFilterEnabled(asset) {
		return buildPartitionedMergeQuery(asset, query, columnNames, on, whenMatchedThenQuery)
	}

	allColumnValues := strings.Join(columnNames, ", ")
	mergeLines := []string{
		fmt.Sprintf("MERGE %s target", asset.Name),
		fmt.Sprintf("USING (%s) source", strings.TrimSuffix(query, ";")),
		fmt.Sprintf("ON (%s)", onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, allColumnValues),
	}

	return buildCreateTableIfNotExistsQuery(asset, query) + ";\n" + strings.Join(mergeLines, "\n") + ";", nil
}

type mergePartitionExpression struct {
	columnName string
	dataType   string
	function   string
	unit       string
}

func (p mergePartitionExpression) render(alias string) string {
	column := alias + "." + p.columnName
	switch p.function {
	case "":
		return column
	case "DATE":
		return "DATE(" + column + ")"
	default:
		return fmt.Sprintf("%s(%s, %s)", p.function, column, p.unit)
	}
}

func parseMergePartitionExpression(asset *pipeline.Asset) (mergePartitionExpression, error) {
	expression := asset.Materialization.PartitionBy
	if matches := simpleMergePartitionRegex.FindStringSubmatch(expression); matches != nil {
		column := asset.GetColumnWithName(matches[1])
		if column == nil {
			return mergePartitionExpression{}, errors.Errorf("materialization.partition_by references column %q, but that column is not defined", matches[1])
		}

		dataType := strings.ToUpper(strings.TrimSpace(column.Type))
		if !supportedMergePartitionDataTypes[dataType] {
			return mergePartitionExpression{}, errors.Errorf(
				"partition-scoped BigQuery merge requires partition column %q to have type DATE, DATETIME, or TIMESTAMP, got %q",
				column.Name,
				column.Type,
			)
		}

		return mergePartitionExpression{columnName: column.Name, dataType: dataType}, nil
	}

	if matches := dateMergePartitionRegex.FindStringSubmatch(expression); matches != nil {
		column := asset.GetColumnWithName(matches[1])
		if column == nil {
			return mergePartitionExpression{}, errors.Errorf("materialization.partition_by references column %q, but that column is not defined", matches[1])
		}
		return mergePartitionExpression{columnName: column.Name, dataType: "DATE", function: "DATE"}, nil
	}

	if matches := truncatedMergePartitionRegex.FindStringSubmatch(expression); matches != nil {
		column := asset.GetColumnWithName(matches[2])
		if column == nil {
			return mergePartitionExpression{}, errors.Errorf("materialization.partition_by references column %q, but that column is not defined", matches[2])
		}

		function := strings.ToUpper(matches[1])
		dataType := strings.TrimSuffix(function, "_TRUNC")
		return mergePartitionExpression{
			columnName: column.Name,
			dataType:   dataType,
			function:   function,
			unit:       strings.ToUpper(matches[3]),
		}, nil
	}

	return mergePartitionExpression{}, errors.Errorf(
		"partition-scoped BigQuery merge does not support materialization.partition_by expression %q; use a DATE, DATETIME, or TIMESTAMP column, DATE(column), or a supported *_TRUNC(column, unit) expression",
		expression,
	)
}

func validatePartitionedMerge(asset *pipeline.Asset) error {
	partition, err := parseMergePartitionExpression(asset)
	if err != nil {
		return err
	}

	partitionColumn := asset.GetColumnWithName(partition.columnName)
	if partitionColumn.PrimaryKey {
		return nil
	}

	// When the partition is the bare column, rewriting the column rewrites the partition, so
	// partition_key_immutable cannot hold: the merge does not match the row in its old partition
	// and the insert then duplicates it into the new one. Truncated partitions are left alone
	// because there a value can change without crossing a partition boundary.
	if partition.function == "" && (partitionColumn.UpdateOnMerge || partitionColumn.MergeSQL != "") {
		return errors.Errorf(
			"partition column %q cannot use update_on_merge or merge_sql in a partition-scoped BigQuery merge because rewriting it moves the row to another partition and duplicates it; drop update_on_merge/merge_sql from the column, or disable bigquery.require_partition_filter so a normal merge can search the whole target",
			partition.columnName,
		)
	}

	if asset.BigQuery.PartitionKeyImmutable != nil && *asset.BigQuery.PartitionKeyImmutable {
		return nil
	}

	return errors.Errorf(
		"partition-scoped BigQuery merge requires partition column %q to be a primary key or bigquery.partition_key_immutable to be true; otherwise an existing row could be left behind when its partition value changes",
		partition.columnName,
	)
}

func buildPartitionedMergeQuery(
	asset *pipeline.Asset,
	query string,
	columnNames []string,
	matchConditions []string,
	whenMatchedThenQuery string,
) (string, error) {
	partition, err := parseMergePartitionExpression(asset)
	if err != nil {
		return "", err
	}

	suffix := helpers.PrefixGenerator()
	sourceTable := "__bruin_merge_source_" + suffix
	partitionVariable := "bruin_merge_partitions_" + suffix
	sourcePartitionExpression := partition.render("source")
	targetPartitionExpression := partition.render("target")
	targetPartitionFilter := fmt.Sprintf("%s IN UNNEST(%s)", targetPartitionExpression, partitionVariable)
	filteredMatchConditions := append([]string{targetPartitionFilter}, matchConditions...)
	filteredMatchQuery := strings.Join(filteredMatchConditions, " AND ")

	statements := []string{
		fmt.Sprintf("DECLARE %s ARRAY<%s>", partitionVariable, partition.dataType),
		buildCreateTableIfNotExistsQuery(asset, query),
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", sourceTable, strings.TrimSuffix(query, ";")),
		fmt.Sprintf(
			"SET %s = ARRAY(SELECT DISTINCT %s FROM %s source WHERE %s IS NOT NULL)",
			partitionVariable,
			sourcePartitionExpression,
			sourceTable,
			sourcePartitionExpression,
		),
		fmt.Sprintf(
			"ASSERT NOT EXISTS (SELECT 1 FROM %s source WHERE %s IS NULL) AS 'partition-scoped merge requires non-null partition values'",
			sourceTable,
			sourcePartitionExpression,
		),
	}

	insertTable := sourceTable
	notMatchedFilter := []string{
		"WHERE NOT EXISTS (",
		"  SELECT 1",
		fmt.Sprintf("  FROM %s target", asset.Name),
		"  WHERE " + filteredMatchQuery,
		")",
	}

	if whenMatchedThenQuery != "" {
		// The merge below mutates the target, so the rows that still need inserting have to be
		// captured beforehand. Re-evaluating the match after the update would re-select any row
		// whose incremental predicate stopped holding because the merge changed a column it
		// references, and insert a duplicate of it.
		insertTable = "__bruin_merge_new_" + suffix
		statements = append(statements, strings.Join(append([]string{
			fmt.Sprintf("CREATE TEMP TABLE %s AS", insertTable),
			fmt.Sprintf("SELECT source.* FROM %s source", sourceTable),
		}, notMatchedFilter...), "\n"))

		statements = append(statements, strings.Join([]string{
			fmt.Sprintf("MERGE %s target", asset.Name),
			fmt.Sprintf("USING %s source", sourceTable),
			fmt.Sprintf("ON (%s)", filteredMatchQuery),
			whenMatchedThenQuery,
		}, "\n"))

		notMatchedFilter = nil
	}

	sourceColumnNames := make([]string, len(columnNames))
	for i, column := range columnNames {
		sourceColumnNames[i] = "source." + column
	}

	insertLines := make([]string, 0, 3+len(notMatchedFilter))
	insertLines = append(
		insertLines,
		fmt.Sprintf("INSERT INTO %s(%s)", asset.Name, strings.Join(columnNames, ", ")),
		"SELECT "+strings.Join(sourceColumnNames, ", "),
		fmt.Sprintf("FROM %s source", insertTable),
	)
	statements = append(statements, strings.Join(append(insertLines, notMatchedFilter...), "\n"))
	statements = append(statements, "COMMIT TRANSACTION")

	return strings.Join(statements, ";\n") + ";", nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	if err := validateRequirePartitionFilter(asset, asset.Materialization.PartitionBy != ""); err != nil {
		return "", err
	}

	return fmt.Sprintf("INSERT INTO %s %s", asset.Name, query), nil
}

func buildIncrementalQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", mat.Strategy)
	}
	if err := validateRequirePartitionFilter(asset, mat.PartitionBy != ""); err != nil {
		return "", err
	}

	foundCol := asset.GetColumnWithName(mat.IncrementalKey)
	if foundCol == nil || foundCol.Type == "" || foundCol.Type == "UNKNOWN" {
		return buildIncrementalQueryWithoutTempVariable(asset, query)
	}

	randPrefix := helpers.PrefixGenerator()
	tempTableName := "__bruin_tmp_" + randPrefix

	declaredVarName := "distinct_keys_" + randPrefix
	queries := []string{
		fmt.Sprintf("DECLARE %s array<%s>", declaredVarName, foundCol.Type),
		buildCreateTableIfNotExistsQuery(asset, query),
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, strings.TrimSuffix(query, ";")),
		fmt.Sprintf("SET %s = (SELECT array_agg(distinct %s) FROM %s)", declaredVarName, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("DELETE FROM %s WHERE %s in unnest(%s)", asset.Name, mat.IncrementalKey, declaredVarName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", asset.Name, tempTableName),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildIncrementalQueryWithoutTempVariable(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		buildCreateTableIfNotExistsQuery(asset, query),
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, strings.TrimSuffix(query, ";")),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", asset.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", asset.Name, tempTableName),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

// ValidateTableOptions reports configuration errors in an asset's `bigquery` block without
// rendering a query. Linting uses it so that `bruin validate` rejects exactly the configurations
// that would later fail while materializing the asset.
func ValidateTableOptions(asset *pipeline.Asset) error {
	// The block is only applied while creating a table. Other materialization types ignore it,
	// which assets inheriting `default.bigquery` from pipeline.yml rely on.
	if asset.BigQuery.IsZero() || asset.Materialization.Type != pipeline.MaterializationTypeTable {
		return nil
	}

	_, err := buildTableOptions(asset, assetIsPartitioned(asset))
	return err
}

// assetIsPartitioned reports whether materializing the asset produces a partitioned table.
// SCD2 strategies fall back to partitioning on `DATE(_valid_from)` when partition_by is omitted.
func assetIsPartitioned(asset *pipeline.Asset) bool {
	switch asset.Materialization.Strategy {
	case pipeline.MaterializationStrategySCD2ByTime, pipeline.MaterializationStrategySCD2ByColumn:
		return true
	default:
		return asset.Materialization.PartitionBy != ""
	}
}

func validateRequirePartitionFilter(asset *pipeline.Asset, partitioned bool) error {
	if !requirePartitionFilterEnabled(asset) {
		return nil
	}
	if !partitioned {
		return errors.New("bigquery.require_partition_filter requires materialization.partition_by to be set")
	}

	switch asset.Materialization.Strategy {
	case pipeline.MaterializationStrategySCD2ByTime, pipeline.MaterializationStrategySCD2ByColumn:
		return errors.Errorf(
			"bigquery.require_partition_filter is not supported with materialization strategy %s because its incremental query scans all target partitions",
			asset.Materialization.Strategy,
		)
	case pipeline.MaterializationStrategyMerge:
		return validatePartitionedMerge(asset)
	case pipeline.MaterializationStrategyDeleteInsert:
		if !partitionExpressionReferencesColumn(asset.Materialization.PartitionBy, asset.Materialization.IncrementalKey) {
			return errors.New("bigquery.require_partition_filter with materialization strategy delete+insert requires materialization.partition_by to use materialization.incremental_key")
		}
		column := asset.GetColumnWithName(asset.Materialization.IncrementalKey)
		if column == nil || column.Type == "" || column.Type == "UNKNOWN" {
			return errors.New("bigquery.require_partition_filter with materialization strategy delete+insert requires the incremental key column type to be set")
		}
	case pipeline.MaterializationStrategyTimeInterval:
		if !partitionExpressionReferencesColumn(asset.Materialization.PartitionBy, asset.Materialization.IncrementalKey) {
			return errors.New("bigquery.require_partition_filter with materialization strategy time_interval requires materialization.partition_by to use materialization.incremental_key")
		}
	default:
		return nil
	}

	return nil
}

func requirePartitionFilterEnabled(asset *pipeline.Asset) bool {
	return asset.BigQuery.RequirePartitionFilter != nil && *asset.BigQuery.RequirePartitionFilter
}

func partitionExpressionReferencesColumn(expression, column string) bool {
	if column == "" {
		return false
	}

	candidate := strings.TrimSpace(expression)
	for {
		openParen := strings.IndexByte(candidate, '(')
		if openParen == -1 {
			break
		}

		argument := candidate[openParen+1:]
		depth := 0
		end := len(argument)
		for index, char := range argument {
			switch char {
			case '(':
				depth++
			case ')':
				if depth == 0 {
					end = index
					goto argumentFound
				}
				depth--
			case ',':
				if depth == 0 {
					end = index
					goto argumentFound
				}
			}
		}

	argumentFound:
		candidate = strings.TrimSpace(argument[:end])
	}

	candidate = strings.Trim(strings.TrimSpace(candidate), "`")
	column = strings.Trim(strings.TrimSpace(column), "`")
	return strings.EqualFold(candidate, column)
}

func buildTableOptions(asset *pipeline.Asset, partitioned bool) (string, error) {
	options := make([]string, 0, 2)

	if err := validateRequirePartitionFilter(asset, partitioned); err != nil {
		return "", err
	}
	if asset.BigQuery.RequirePartitionFilter != nil && *asset.BigQuery.RequirePartitionFilter {
		options = append(options, "require_partition_filter = TRUE")
	}

	if asset.BigQuery.PartitionExpirationDays != nil {
		// zero means "do not expire partitions", which is also how an inherited pipeline default is disabled.
		if expirationDays := *asset.BigQuery.PartitionExpirationDays; expirationDays != 0 {
			if expirationDays < 0 || math.IsNaN(expirationDays) || math.IsInf(expirationDays, 0) {
				return "", errors.Errorf("bigquery.partition_expiration_days must be a positive number, got %s", strconv.FormatFloat(expirationDays, 'f', -1, 64))
			}
			if !partitioned {
				return "", errors.New("bigquery.partition_expiration_days requires materialization.partition_by to be set")
			}
			if integerRangePartitionRegex.MatchString(asset.Materialization.PartitionBy) {
				return "", errors.New("bigquery.partition_expiration_days is not supported for integer-range partitioned tables")
			}
			options = append(options, "partition_expiration_days = "+strconv.FormatFloat(expirationDays, 'f', -1, 64))
		}
	}

	if len(options) == 0 {
		return "", nil
	}

	return "OPTIONS (" + strings.Join(options, ", ") + ")", nil
}

func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	mat := asset.Materialization
	switch asset.Materialization.Strategy {
	case pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimefullRefresh(asset, query)
	case pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnfullRefresh(asset, query)
	default:
		partitionClause := ""

		if mat.PartitionBy != "" {
			partitionClause = "PARTITION BY " + mat.PartitionBy
		}

		clusterByClause := ""
		if len(mat.ClusterBy) > 0 {
			clusterByClause = "CLUSTER BY " + strings.Join(mat.ClusterBy, ", ")
		}

		optionsClause, err := buildTableOptions(asset, mat.PartitionBy != "")
		if err != nil {
			return "", err
		}
		if optionsClause == "" {
			return fmt.Sprintf("CREATE OR REPLACE TABLE %s %s %s AS\n%s", asset.Name, partitionClause, clusterByClause, query), nil
		}

		return fmt.Sprintf("CREATE OR REPLACE TABLE %s %s %s %s AS\n%s", asset.Name, partitionClause, clusterByClause, optionsClause, query), nil
	}
}

func buildCreateTableIfNotExistsQuery(asset *pipeline.Asset, query string) string {
	options := make([]string, 0, 2)
	if asset.Materialization.PartitionBy != "" {
		options = append(options, "PARTITION BY "+asset.Materialization.PartitionBy)
	}
	if len(asset.Materialization.ClusterBy) > 0 {
		options = append(options, "CLUSTER BY "+strings.Join(asset.Materialization.ClusterBy, ", "))
	}

	return ansisql.BuildCreateTableIfNotExistsAsQuery(asset.Name, strings.Join(options, " "), query)
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
	if err := validateRequirePartitionFilter(asset, asset.Materialization.PartitionBy != ""); err != nil {
		return "", err
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	queries := []string{
		buildCreateTableIfNotExistsQuery(asset, query),
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'`,
			asset.Name,
			asset.Materialization.IncrementalKey,
			startVar,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			asset.Name,
			strings.TrimSuffix(query, ";")),
		"COMMIT TRANSACTION",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	primaryKeys := []string{}

	foreignKeys := []string{}

	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.SQLType())

		if col.Collation != "" {
			def += fmt.Sprintf(" COLLATE %q", col.Collation)
		}
		if col.Default != "" {
			def += " DEFAULT " + col.Default
		}
		if col.Description != "" {
			def += fmt.Sprintf(` OPTIONS(description=%q)`, col.Description)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		if col.ForeignKey != nil && col.ForeignKey.Table != "" && col.ForeignKey.Column != "" {
			foreignKeys = append(foreignKeys, fmt.Sprintf(
				"FOREIGN KEY (%s) REFERENCES %s(%s) NOT ENFORCED",
				col.Name, col.ForeignKey.Table, col.ForeignKey.Column,
			))
		}
		columnDefs = append(columnDefs, def)
	}

	if len(primaryKeys) > 0 {
		primaryKeyClause := fmt.Sprintf("PRIMARY KEY (%s) NOT ENFORCED", strings.Join(primaryKeys, ", "))
		columnDefs = append(columnDefs, primaryKeyClause)
	}

	columnDefs = append(columnDefs, foreignKeys...)

	q := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		asset.Name,
		strings.Join(columnDefs, ",\n  "),
	)

	if asset.Materialization.PartitionBy != "" {
		q += "\nPARTITION BY " + asset.Materialization.PartitionBy
	}
	if len(asset.Materialization.ClusterBy) > 0 {
		q += "\nCLUSTER BY " + strings.Join(asset.Materialization.ClusterBy, ", ")
	}

	optionsClause, err := buildTableOptions(asset, asset.Materialization.PartitionBy != "")
	if err != nil {
		return "", err
	}
	if optionsClause != "" {
		q += "\n" + optionsClause
	}

	return q, nil
}

func buildSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")

	if err := validateRequirePartitionFilter(asset, assetIsPartitioned(asset)); err != nil {
		return "", err
	}

	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys  = make([]string, 0, 4)
		joinConds    = make([]string, 0, 5)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)
	for _, col := range asset.Columns {
		switch col.Name {
		case "_valid_from", "_valid_until", "_is_current":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return "", errors.New("incremental_key must be TIMESTAMP or DATE in SCD2_by_time strategy")
			}
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}
	pkList := strings.Join(primaryKeys, ", ")
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(
		insertValues,
		"CAST(source."+asset.Materialization.IncrementalKey+" AS TIMESTAMP)",
		"TIMESTAMP('9999-12-31')",
		"TRUE",
	)

	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf("target.%[1]s = source.%[1]s", pk))
	}
	joinConds = append(joinConds, "target._is_current AND source._is_current")
	onCondition := strings.Join(joinConds, " AND ")
	tbl := fmt.Sprintf("`%s`", asset.Name)

	queryStr := fmt.Sprintf(
		`
MERGE INTO %s AS target
USING (
  WITH s1 AS (
    %s
  )
  SELECT s1.*, TRUE AS _is_current
  FROM   s1
  UNION ALL
  SELECT s1.*, FALSE AS _is_current
  FROM s1
  JOIN   %s AS t1 USING (%s)
  WHERE  t1._valid_from < CAST (s1.%s AS TIMESTAMP) AND t1._is_current
) AS source
ON  %s

WHEN MATCHED AND (
  target._valid_from < CAST (source.%s AS TIMESTAMP)
) THEN
  UPDATE SET
    target._valid_until = CAST (source.%s AS TIMESTAMP),
    target._is_current  = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    target._valid_until = CURRENT_TIMESTAMP(),
    target._is_current  = FALSE

WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		tbl,
		strings.TrimSpace(query),
		tbl,
		pkList,
		asset.Materialization.IncrementalKey,
		onCondition,
		asset.Materialization.IncrementalKey,
		asset.Materialization.IncrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")
	if err := validateRequirePartitionFilter(asset, assetIsPartitioned(asset)); err != nil {
		return "", err
	}

	var (
		primaryKeys      = make([]string, 0, 4)
		compareConds     = make([]string, 0, 12)
		compareCondsS1T1 = make([]string, 0, 4)
		insertCols       = make([]string, 0, 12)
		insertValues     = make([]string, 0, 12)
	)

	incrementalKey := asset.Materialization.IncrementalKey

	for _, col := range asset.Columns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%[1]s != source.%[1]s", col.Name))
			compareCondsS1T1 = append(compareCondsS1T1,
				fmt.Sprintf("t1.%[1]s != s1.%[1]s", col.Name))
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")

	validFromExpr := "CURRENT_TIMESTAMP()"
	validUntilUpdateExpr := "CURRENT_TIMESTAMP()"
	if incrementalKey != "" {
		validFromExpr = fmt.Sprintf("CAST(source.%s AS TIMESTAMP)", incrementalKey)
		validUntilUpdateExpr = fmt.Sprintf("CAST(source.%s AS TIMESTAMP)", incrementalKey)
	}
	insertValues = append(insertValues, validFromExpr, "TIMESTAMP('9999-12-31')", "TRUE")

	pkList := strings.Join(primaryKeys, ", ")
	for i, pk := range primaryKeys {
		primaryKeys[i] = fmt.Sprintf("target.%[1]s = source.%[1]s", pk)
	}
	onCondition := strings.Join(primaryKeys, " AND ")
	onCondition += " AND target._is_current AND source._is_current"

	tbl := fmt.Sprintf("`%s`", asset.Name)
	whereCondition := strings.Join(compareCondsS1T1, " OR ")
	whereCondition = "(" + whereCondition + ")" + " AND t1._is_current"
	queryStr := fmt.Sprintf(
		`
MERGE INTO %s AS target
USING (
  WITH s1 AS (
    %s
  )
  SELECT *, TRUE AS _is_current
  FROM   s1
  UNION ALL
  SELECT s1.*, FALSE AS _is_current
  FROM   s1
  JOIN   %s AS t1 USING (%s)
  WHERE  %s
) AS source
ON  %s

WHEN MATCHED AND (
    %s
) THEN
  UPDATE SET
    target._valid_until = %s,
    target._is_current  = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    target._valid_until = CURRENT_TIMESTAMP(),
    target._is_current  = FALSE


WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		tbl,
		strings.TrimSpace(query),
		tbl,
		pkList,
		whereCondition,
		onCondition,
		strings.Join(compareConds, " OR "),
		validUntilUpdateExpr,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}
	tbl := fmt.Sprintf("`%s`", asset.Name)
	cluster := strings.Join(primaryKeys, ", ")

	// Build partition clause - use user-specified partition or default to DATE(_valid_from)
	var partitionClause string
	if asset.Materialization.PartitionBy != "" {
		partitionClause = "PARTITION BY " + asset.Materialization.PartitionBy
	} else {
		partitionClause = "PARTITION BY DATE(_valid_from)"
	}

	// Build cluster clause - use user-specified cluster or default to _is_current + primary keys
	var clusterClause string
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterClause = "CLUSTER BY " + strings.Join(asset.Materialization.ClusterBy, ", ")
	} else {
		clusterClause = "CLUSTER BY _is_current, " + cluster
	}

	optionsClause, err := buildTableOptions(asset, assetIsPartitioned(asset))
	if err != nil {
		return "", err
	}
	if optionsClause != "" {
		optionsClause = "\n" + optionsClause
	}

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s
%s
%s%s AS
SELECT
  CAST (%s AS TIMESTAMP) AS _valid_from,
  src.*,
  TIMESTAMP('9999-12-31') AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;`,
		tbl,
		partitionClause,
		clusterClause,
		optionsClause,
		asset.Materialization.IncrementalKey,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}
	tbl := fmt.Sprintf("`%s`", asset.Name)
	cluster := strings.Join(primaryKeys, ", ")

	// Build partition clause - use user-specified partition or default to DATE(_valid_from)
	var partitionClause string
	if asset.Materialization.PartitionBy != "" {
		partitionClause = "PARTITION BY " + asset.Materialization.PartitionBy
	} else {
		partitionClause = "PARTITION BY DATE(_valid_from)"
	}

	// Build cluster clause - use user-specified cluster or default to _is_current + primary keys
	var clusterClause string
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterClause = "CLUSTER BY " + strings.Join(asset.Materialization.ClusterBy, ", ")
	} else {
		clusterClause = "CLUSTER BY _is_current, " + cluster
	}

	optionsClause, err := buildTableOptions(asset, assetIsPartitioned(asset))
	if err != nil {
		return "", err
	}
	if optionsClause != "" {
		optionsClause = "\n" + optionsClause
	}

	validFromExpr := "CURRENT_TIMESTAMP()"
	if asset.Materialization.IncrementalKey != "" {
		validFromExpr = fmt.Sprintf("CAST (%s AS TIMESTAMP)", asset.Materialization.IncrementalKey)
	}

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE TABLE %s
%s
%s%s AS
SELECT
  %s AS _valid_from,
  src.*,
  TIMESTAMP('9999-12-31') AS _valid_until,
  TRUE                    AS _is_current
FROM (
%s
) AS src;`,
		tbl,
		partitionClause,
		clusterClause,
		optionsClause,
		validFromExpr,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}
