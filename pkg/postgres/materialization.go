package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// QuoteIdentifier quotes a PostgreSQL identifier (table, column, etc.) to handle case-sensitive names.
// It splits the identifier on "." and quotes each part separately.
// For example, "schema.MyTable" becomes "\"schema\".\"MyTable\"".
func QuoteIdentifier(identifier string) string {
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
		pipeline.MaterializationStrategyNone:               buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:             buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:      buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:       buildIncrementalQuery,
		pipeline.MaterializationStrategyTruncateInsert:     buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:              buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:       buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:                buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:       buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:         buildSCD2QueryByTime,
		pipeline.MaterializationStrategyDataVaultHub:       buildDataVaultHubQuery,
		pipeline.MaterializationStrategyDataVaultLink:      buildDataVaultLinkQuery,
		pipeline.MaterializationStrategyDataVaultSatellite: buildDataVaultSatelliteQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", QuoteIdentifier(asset.Name), query), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return fmt.Sprintf("INSERT INTO %s %s", QuoteIdentifier(asset.Name), query), nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) (string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()
	quotedIncrementalKey := QuoteIdentifier(mat.IncrementalKey)

	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, strings.TrimSuffix(query, ";")),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", QuoteIdentifier(task.Name), quotedIncrementalKey, quotedIncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", QuoteIdentifier(task.Name), tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildTruncateInsertQuery(task *pipeline.Asset, query string) (string, error) {
	queries := []string{
		"BEGIN TRANSACTION",
		"TRUNCATE TABLE " + QuoteIdentifier(task.Name),
		fmt.Sprintf("INSERT INTO %s %s", QuoteIdentifier(task.Name), strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func getColumnsWithMergeLogic(asset *pipeline.Asset) []pipeline.Column {
	var columns []pipeline.Column
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			continue
		}
		if col.MergeSQL != "" || col.UpdateOnMerge {
			columns = append(columns, col)
		}
	}
	return columns
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		return buildRedshiftMergeQuery(asset, query)
	}

	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	mergeColumns := getColumnsWithMergeLogic(asset)
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", QuoteIdentifier(key), QuoteIdentifier(key)))
	}
	onQuery := strings.Join(on, " AND ")

	// Quote all column names for INSERT clause
	quotedColumnNames := make([]string, 0, len(columnNames))
	quotedColumnValues := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		quotedColumnNames = append(quotedColumnNames, QuoteIdentifier(col))
		quotedColumnValues = append(quotedColumnValues, QuoteIdentifier(col))
	}
	allColumnNamesStr := strings.Join(quotedColumnNames, ", ")
	allColumnValuesStr := strings.Join(quotedColumnValues, ", ")

	whenMatchedThenQuery := ""

	if len(mergeColumns) > 0 {
		matchedUpdateStatements := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			if col.MergeSQL != "" {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = %s", QuoteIdentifier(col.Name), col.MergeSQL))
			} else {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", QuoteIdentifier(col.Name), QuoteIdentifier(col.Name)))
			}
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", QuoteIdentifier(asset.Name)),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnNamesStr, allColumnValuesStr),
	}

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildRedshiftMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	mergeColumns := getColumnsWithMergeLogic(asset)
	columnNames := asset.ColumnNames()

	// In Redshift MERGE, target table doesn't have an alias, so we use the table name directly
	targetTableName := QuoteIdentifier(asset.Name)

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("%s.%s = source.%s", targetTableName, QuoteIdentifier(key), QuoteIdentifier(key)))
	}
	onQuery := strings.Join(on, " AND ")

	// Quote all column names for INSERT clause
	quotedColumnNames := make([]string, 0, len(columnNames))
	quotedColumnValues := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		quotedColumnNames = append(quotedColumnNames, QuoteIdentifier(col))
		quotedColumnValues = append(quotedColumnValues, "source."+QuoteIdentifier(col))
	}
	allColumnNamesStr := strings.Join(quotedColumnNames, ", ")
	allColumnValuesStr := strings.Join(quotedColumnValues, ", ")

	whenMatchedThenQuery := ""

	if len(mergeColumns) > 0 {
		matchedUpdateStatements := make([]string, 0, len(mergeColumns))
		for _, col := range mergeColumns {
			if col.MergeSQL != "" {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = %s", QuoteIdentifier(col.Name), col.MergeSQL))
			} else {
				matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", QuoteIdentifier(col.Name), QuoteIdentifier(col.Name)))
			}
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	mergeLines := []string{
		"MERGE INTO " + targetTableName,
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnNamesStr, allColumnValuesStr),
	}

	return strings.Join(mergeLines, "\n") + ";", nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) (string, error) {
	switch task.Materialization.Strategy {
	case pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimefullRefresh(task, query)
	case pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnfullRefresh(task, query)
	case pipeline.MaterializationStrategyDataVaultHub:
		return buildDataVaultHubQueryWithOptions(task, query, true)
	case pipeline.MaterializationStrategyDataVaultLink:
		return buildDataVaultLinkQueryWithOptions(task, query, true)
	case pipeline.MaterializationStrategyDataVaultSatellite:
		return buildDataVaultSatelliteQueryWithOptions(task, query, true)
	default:
		query = strings.TrimSuffix(query, ";")
		return fmt.Sprintf(
			`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s; 
CREATE TABLE %s AS %s;
COMMIT;`, QuoteIdentifier(task.Name), QuoteIdentifier(task.Name), query,
		), nil
	}
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}
	if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp && asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return "", errors.New("time_granularity must be either 'date', or 'timestamp'")
	}
	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	queries := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s'`,
			QuoteIdentifier(asset.Name),
			quotedIncrementalKey,
			startVar,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			QuoteIdentifier(asset.Name),
			strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	primaryKeys := []string{}
	columnComments := []string{}

	for _, col := range asset.Columns {
		quotedColName := QuoteIdentifier(col.Name)
		def := fmt.Sprintf("%s %s", quotedColName, col.Type)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
		columnDefs = append(columnDefs, def)

		if col.Description != "" {
			comment := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", QuoteIdentifier(asset.Name), quotedColName, strings.ReplaceAll(col.Description, "'", "''"))
			columnComments = append(columnComments, comment)
		}
	}

	if len(primaryKeys) > 0 {
		primaryKeyClause := fmt.Sprintf("primary key (%s)", strings.Join(primaryKeys, ", "))
		columnDefs = append(columnDefs, primaryKeyClause)
	}

	q := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n"+
			"%s\n)",
		QuoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	)

	if len(columnComments) > 0 {
		q += ";\n" + strings.Join(columnComments, "\n")
	}

	return q, nil
}

func buildDataVaultHubQuery(asset *pipeline.Asset, query string) (string, error) {
	return buildDataVaultHubQueryWithOptions(asset, query, false)
}

func buildDataVaultHubQueryWithOptions(asset *pipeline.Asset, query string, fullRefresh bool) (string, error) {
	hashKey, businessKeys, loadDatetime, recordSource, err := resolveDataVaultHubColumns(asset)
	if err != nil {
		return "", err
	}

	mandatoryColumns := append([]*pipeline.Column{hashKey, loadDatetime, recordSource}, businessKeys...)
	statements, err := buildDataVaultTableStatements(asset, []string{hashKey.Name}, mandatoryColumns, fullRefresh)
	if err != nil {
		return "", err
	}

	insertQuery := fmt.Sprintf(
		`WITH __bruin_source AS (
%s
),
__bruin_dedup AS (
  SELECT DISTINCT ON (source.%s)
    %s
  FROM __bruin_source AS source
  WHERE %s
  ORDER BY source.%s, source.%s ASC
)
INSERT INTO %s (%s)
SELECT %s
FROM __bruin_dedup AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM %s AS target
  WHERE target.%s = source.%s
)
ON CONFLICT (%s) DO NOTHING`,
		trimMaterializationQuery(query),
		QuoteIdentifier(hashKey.Name),
		dataVaultSourceSelectList(asset),
		strings.Join(dataVaultNotNullConditions("source", mandatoryColumns), " AND "),
		QuoteIdentifier(hashKey.Name),
		QuoteIdentifier(loadDatetime.Name),
		QuoteIdentifier(asset.Name),
		strings.Join(quoteNames(asset.ColumnNames()), ", "),
		dataVaultSourceSelectList(asset),
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(hashKey.Name),
		QuoteIdentifier(hashKey.Name),
		QuoteIdentifier(hashKey.Name),
	)

	statements = append(statements, insertQuery)
	return dataVaultTransaction(statements), nil
}

func buildDataVaultLinkQuery(asset *pipeline.Asset, query string) (string, error) {
	return buildDataVaultLinkQueryWithOptions(asset, query, false)
}

func buildDataVaultLinkQueryWithOptions(asset *pipeline.Asset, query string, fullRefresh bool) (string, error) {
	linkHashKey, relatedHashKeys, loadDatetime, recordSource, err := resolveDataVaultLinkColumns(asset)
	if err != nil {
		return "", err
	}

	mandatoryColumns := append([]*pipeline.Column{linkHashKey, loadDatetime, recordSource}, relatedHashKeys...)
	statements, err := buildDataVaultTableStatements(asset, []string{linkHashKey.Name}, mandatoryColumns, fullRefresh)
	if err != nil {
		return "", err
	}

	insertQuery := fmt.Sprintf(
		`WITH __bruin_source AS (
%s
),
__bruin_dedup AS (
  SELECT DISTINCT ON (source.%s)
    %s
  FROM __bruin_source AS source
  WHERE %s
  ORDER BY source.%s, source.%s ASC
)
INSERT INTO %s (%s)
SELECT %s
FROM __bruin_dedup AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM %s AS target
  WHERE target.%s = source.%s
)
ON CONFLICT (%s) DO NOTHING`,
		trimMaterializationQuery(query),
		QuoteIdentifier(linkHashKey.Name),
		dataVaultSourceSelectList(asset),
		strings.Join(dataVaultNotNullConditions("source", mandatoryColumns), " AND "),
		QuoteIdentifier(linkHashKey.Name),
		QuoteIdentifier(loadDatetime.Name),
		QuoteIdentifier(asset.Name),
		strings.Join(quoteNames(asset.ColumnNames()), ", "),
		dataVaultSourceSelectList(asset),
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(linkHashKey.Name),
		QuoteIdentifier(linkHashKey.Name),
		QuoteIdentifier(linkHashKey.Name),
	)

	statements = append(statements, insertQuery)
	return dataVaultTransaction(statements), nil
}

func buildDataVaultSatelliteQuery(asset *pipeline.Asset, query string) (string, error) {
	return buildDataVaultSatelliteQueryWithOptions(asset, query, false)
}

func buildDataVaultSatelliteQueryWithOptions(asset *pipeline.Asset, query string, fullRefresh bool) (string, error) {
	parentHashKey, hashDiff, loadDatetime, recordSource, err := resolveDataVaultSatelliteColumns(asset)
	if err != nil {
		return "", err
	}

	primaryKeys := dataVaultSatellitePrimaryKeys(asset, parentHashKey, loadDatetime)
	mandatoryColumns := []*pipeline.Column{parentHashKey, hashDiff, loadDatetime, recordSource}
	statements, err := buildDataVaultTableStatements(asset, primaryKeys, mandatoryColumns, fullRefresh)
	if err != nil {
		return "", err
	}

	insertQuery := fmt.Sprintf(
		`WITH __bruin_source AS (
%s
),
__bruin_valid AS (
  SELECT
    %s
  FROM __bruin_source AS source
  WHERE %s
),
__bruin_ordered AS (
  SELECT
    valid.*,
    LAG(valid.%s) OVER (PARTITION BY valid.%s ORDER BY valid.%s, valid.%s) AS __bruin_previous_hashdiff,
    ROW_NUMBER() OVER (PARTITION BY valid.%s ORDER BY valid.%s, valid.%s) AS __bruin_row_number
  FROM __bruin_valid AS valid
),
__bruin_latest AS (
  SELECT DISTINCT ON (target.%s)
    target.%s,
    target.%s
  FROM %s AS target
  WHERE target.%s IS NOT NULL
  ORDER BY target.%s, target.%s DESC
)
INSERT INTO %s (%s)
SELECT %s
FROM __bruin_ordered AS source
LEFT JOIN __bruin_latest AS latest
  ON latest.%s = source.%s
WHERE (
    source.__bruin_row_number = 1
    AND (latest.%s IS NULL OR latest.%s IS DISTINCT FROM source.%s)
  )
  OR (
    source.__bruin_row_number > 1
    AND source.__bruin_previous_hashdiff IS DISTINCT FROM source.%s
  )
ON CONFLICT (%s) DO NOTHING`,
		trimMaterializationQuery(query),
		dataVaultIndentedSelectList(asset, "source", 4),
		strings.Join(dataVaultNotNullConditions("source", mandatoryColumns), " AND "),
		QuoteIdentifier(hashDiff.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(loadDatetime.Name),
		QuoteIdentifier(hashDiff.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(loadDatetime.Name),
		QuoteIdentifier(hashDiff.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(hashDiff.Name),
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(loadDatetime.Name),
		QuoteIdentifier(asset.Name),
		strings.Join(quoteNames(asset.ColumnNames()), ", "),
		dataVaultSourceSelectList(asset),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(parentHashKey.Name),
		QuoteIdentifier(hashDiff.Name),
		QuoteIdentifier(hashDiff.Name),
		QuoteIdentifier(hashDiff.Name),
		strings.Join(quoteNames(primaryKeys), ", "),
	)

	statements = append(statements, insertQuery)
	return dataVaultTransaction(statements), nil
}

func resolveDataVaultHubColumns(asset *pipeline.Asset) (*pipeline.Column, []*pipeline.Column, *pipeline.Column, *pipeline.Column, error) {
	if len(asset.Columns) == 0 {
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_hub requires the `columns` field to be set")
	}

	hashKey := findDataVaultColumn(asset, []string{"hash_key", "hub_hash_key"}, func(col *pipeline.Column) bool {
		return col.PrimaryKey
	})
	if hashKey == nil {
		hashKey = findSingleDataVaultColumnBySuffix(asset, "_hk", nil)
	}

	businessKeys := findDataVaultColumns(asset, []string{"business_key"}, func(col *pipeline.Column) bool {
		return strings.HasSuffix(strings.ToLower(col.Name), "_bk")
	}, nil)
	loadDatetime := findDataVaultLoadDatetimeColumn(asset)
	recordSource := findDataVaultRecordSourceColumn(asset)

	switch {
	case hashKey == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_hub requires a hash key column with datavault_role: hash_key or primary_key: true")
	case len(businessKeys) == 0:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_hub requires at least one business key column with datavault_role: business_key")
	case loadDatetime == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_hub requires a load datetime column with datavault_role: load_datetime")
	case recordSource == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_hub requires a record source column with datavault_role: record_source")
	default:
		return hashKey, businessKeys, loadDatetime, recordSource, nil
	}
}

func resolveDataVaultLinkColumns(asset *pipeline.Asset) (*pipeline.Column, []*pipeline.Column, *pipeline.Column, *pipeline.Column, error) {
	if len(asset.Columns) == 0 {
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_link requires the `columns` field to be set")
	}

	linkHashKey := findDataVaultColumn(asset, []string{"link_hash_key", "hash_key"}, func(col *pipeline.Column) bool {
		return col.PrimaryKey
	})
	if linkHashKey == nil {
		linkHashKey = findSingleDataVaultColumnBySuffix(asset, "_hk", nil)
	}

	excludedColumns := []*pipeline.Column{linkHashKey}
	relatedHashKeys := findDataVaultColumns(asset, []string{"hub_hash_key", "parent_hash_key", "foreign_hash_key"}, func(col *pipeline.Column) bool {
		return strings.HasSuffix(strings.ToLower(col.Name), "_hk")
	}, excludedColumns)
	loadDatetime := findDataVaultLoadDatetimeColumn(asset)
	recordSource := findDataVaultRecordSourceColumn(asset)

	switch {
	case linkHashKey == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_link requires a link hash key column with datavault_role: link_hash_key or primary_key: true")
	case len(relatedHashKeys) == 0:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_link requires at least one related hash key column with datavault_role: hub_hash_key")
	case loadDatetime == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_link requires a load datetime column with datavault_role: load_datetime")
	case recordSource == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_link requires a record source column with datavault_role: record_source")
	default:
		return linkHashKey, relatedHashKeys, loadDatetime, recordSource, nil
	}
}

func resolveDataVaultSatelliteColumns(asset *pipeline.Asset) (*pipeline.Column, *pipeline.Column, *pipeline.Column, *pipeline.Column, error) {
	if len(asset.Columns) == 0 {
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_satellite requires the `columns` field to be set")
	}

	parentHashKey := findDataVaultColumn(asset, []string{"parent_hash_key", "hub_hash_key", "hash_key"}, func(col *pipeline.Column) bool {
		return col.PrimaryKey
	})
	if parentHashKey == nil {
		parentHashKey = findSingleDataVaultColumnBySuffix(asset, "_hk", nil)
	}

	hashDiff := findDataVaultColumn(asset, []string{"hashdiff", "hash_diff"}, func(col *pipeline.Column) bool {
		name := strings.ToLower(col.Name)
		return name == "hashdiff" || name == "hash_diff"
	})
	loadDatetime := findDataVaultLoadDatetimeColumn(asset)
	recordSource := findDataVaultRecordSourceColumn(asset)

	switch {
	case parentHashKey == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_satellite requires a parent hash key column with datavault_role: parent_hash_key or primary_key: true")
	case hashDiff == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_satellite requires a hashdiff column with datavault_role: hashdiff")
	case loadDatetime == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_satellite requires a load datetime column with datavault_role: load_datetime")
	case recordSource == nil:
		return nil, nil, nil, nil, errors.New("materialization strategy datavault_satellite requires a record source column with datavault_role: record_source")
	default:
		return parentHashKey, hashDiff, loadDatetime, recordSource, nil
	}
}

func buildDataVaultTableStatements(asset *pipeline.Asset, primaryKeys []string, mandatoryColumns []*pipeline.Column, fullRefresh bool) ([]string, error) {
	if len(asset.Columns) == 0 {
		return nil, errors.New("data vault materialization requires the `columns` field to be set")
	}

	mandatoryColumnNames := make(map[string]bool, len(mandatoryColumns))
	for _, col := range mandatoryColumns {
		if col != nil {
			mandatoryColumnNames[strings.ToLower(col.Name)] = true
		}
	}

	columnDefs := make([]string, 0, len(asset.Columns)+1)
	for _, col := range asset.Columns {
		if strings.TrimSpace(col.Name) == "" {
			return nil, errors.New("data vault materialization requires every column to have a name")
		}
		if strings.TrimSpace(col.Type) == "" {
			return nil, fmt.Errorf("data vault materialization requires column %q to have a type", col.Name)
		}

		def := fmt.Sprintf("%s %s", QuoteIdentifier(col.Name), col.Type)
		if mandatoryColumnNames[strings.ToLower(col.Name)] || !col.Nullable.Bool() {
			def += " NOT NULL"
		}
		columnDefs = append(columnDefs, def)
	}

	if len(primaryKeys) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("primary key (%s)", strings.Join(quoteNames(primaryKeys), ", ")))
	}

	statements := make([]string, 0, 4)
	if schemaStatement := createSchemaStatement(asset.Name); schemaStatement != "" {
		statements = append(statements, schemaStatement)
	}
	if fullRefresh {
		statements = append(statements, "DROP TABLE IF EXISTS "+QuoteIdentifier(asset.Name))
	}
	statements = append(statements, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
		QuoteIdentifier(asset.Name),
		strings.Join(columnDefs, ",\n"),
	))

	return statements, nil
}

func createSchemaStatement(name string) string {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return ""
	}
	return "CREATE SCHEMA IF NOT EXISTS " + QuoteIdentifier(parts[0])
}

func dataVaultTransaction(statements []string) string {
	return "BEGIN TRANSACTION;\n" + strings.Join(statements, ";\n") + ";\nCOMMIT;"
}

func trimMaterializationQuery(query string) string {
	return strings.TrimSuffix(strings.TrimSpace(query), ";")
}

func dataVaultSatellitePrimaryKeys(asset *pipeline.Asset, parentHashKey, loadDatetime *pipeline.Column) []string {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 || (len(primaryKeys) == 1 && strings.EqualFold(primaryKeys[0], parentHashKey.Name)) {
		return []string{parentHashKey.Name, loadDatetime.Name}
	}
	return primaryKeys
}

func dataVaultSourceSelectList(asset *pipeline.Asset) string {
	return strings.Join(dataVaultSelectColumns(asset, "source"), ", ")
}

func dataVaultIndentedSelectList(asset *pipeline.Asset, tableAlias string, spaces int) string {
	return strings.Join(dataVaultSelectColumns(asset, tableAlias), ",\n"+strings.Repeat(" ", spaces))
}

func dataVaultSelectColumns(asset *pipeline.Asset, tableAlias string) []string {
	columns := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		columns = append(columns, fmt.Sprintf("%s.%s", tableAlias, QuoteIdentifier(col.Name)))
	}
	return columns
}

func dataVaultNotNullConditions(tableAlias string, columns []*pipeline.Column) []string {
	conditions := make([]string, 0, len(columns))
	seen := make(map[string]bool, len(columns))
	for _, col := range columns {
		if col == nil {
			continue
		}
		key := strings.ToLower(col.Name)
		if seen[key] {
			continue
		}
		seen[key] = true
		conditions = append(conditions, fmt.Sprintf("%s.%s IS NOT NULL", tableAlias, QuoteIdentifier(col.Name)))
	}
	return conditions
}

func quoteNames(names []string) []string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, QuoteIdentifier(name))
	}
	return quoted
}

func findDataVaultLoadDatetimeColumn(asset *pipeline.Asset) *pipeline.Column {
	return findDataVaultColumn(asset, []string{"load_datetime", "load_dts"}, func(col *pipeline.Column) bool {
		switch strings.ToLower(col.Name) {
		case "load_dts", "load_datetime", "loaded_at":
			return true
		default:
			return false
		}
	})
}

func findDataVaultRecordSourceColumn(asset *pipeline.Asset) *pipeline.Column {
	return findDataVaultColumn(asset, []string{"record_source"}, func(col *pipeline.Column) bool {
		return strings.EqualFold(col.Name, "record_source")
	})
}

func findDataVaultColumn(asset *pipeline.Asset, roles []string, fallback func(*pipeline.Column) bool) *pipeline.Column {
	for i := range asset.Columns {
		col := &asset.Columns[i]
		if dataVaultRoleMatches(col, roles) {
			return col
		}
	}

	if fallback == nil {
		return nil
	}
	for i := range asset.Columns {
		col := &asset.Columns[i]
		if fallback(col) {
			return col
		}
	}

	return nil
}

func findDataVaultColumns(asset *pipeline.Asset, roles []string, fallback func(*pipeline.Column) bool, excludedColumns []*pipeline.Column) []*pipeline.Column {
	columns := make([]*pipeline.Column, 0)
	seen := make(map[string]bool)
	for i := range asset.Columns {
		col := &asset.Columns[i]
		if dataVaultColumnIsExcluded(col, excludedColumns) || !dataVaultRoleMatches(col, roles) {
			continue
		}
		columns = append(columns, col)
		seen[strings.ToLower(col.Name)] = true
	}

	if fallback == nil {
		return columns
	}
	for i := range asset.Columns {
		col := &asset.Columns[i]
		key := strings.ToLower(col.Name)
		if seen[key] || dataVaultColumnIsExcluded(col, excludedColumns) || !fallback(col) {
			continue
		}
		columns = append(columns, col)
		seen[key] = true
	}

	return columns
}

func findSingleDataVaultColumnBySuffix(asset *pipeline.Asset, suffix string, excludedColumns []*pipeline.Column) *pipeline.Column {
	var matched *pipeline.Column
	for i := range asset.Columns {
		col := &asset.Columns[i]
		if dataVaultColumnIsExcluded(col, excludedColumns) || !strings.HasSuffix(strings.ToLower(col.Name), suffix) {
			continue
		}
		if matched != nil {
			return nil
		}
		matched = col
	}
	return matched
}

func dataVaultRoleMatches(col *pipeline.Column, roles []string) bool {
	if col == nil || len(col.Meta) == 0 {
		return false
	}

	role := strings.ToLower(strings.TrimSpace(col.Meta["datavault_role"]))
	for _, candidate := range roles {
		if role == strings.ToLower(candidate) {
			return true
		}
	}
	return false
}

func dataVaultColumnIsExcluded(col *pipeline.Column, excludedColumns []*pipeline.Column) bool {
	if col == nil {
		return true
	}
	for _, excluded := range excludedColumns {
		if excluded != nil && strings.EqualFold(col.Name, excluded.Name) {
			return true
		}
	}
	return false
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	var validuntil string
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		validuntil = "TIMESTAMP '9999-12-31 00:00:00'"
	} else {
		validuntil = "'9999-12-31 00:00:00'::TIMESTAMP"
	}

	validFromExpr := "CURRENT_TIMESTAMP"
	if asset.Materialization.IncrementalKey != "" {
		validFromExpr = QuoteIdentifier(asset.Materialization.IncrementalKey)
	}

	stmt := fmt.Sprintf(
		`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
SELECT
  %s AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;
COMMIT;`,
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(asset.Name),
		validFromExpr,
		validuntil,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	var validuntil string
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		validuntil = "TIMESTAMP '9999-12-31 00:00:00'"
	} else {
		validuntil = "'9999-12-31 00:00:00'::TIMESTAMP"
	}

	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	stmt := fmt.Sprintf(
		`BEGIN TRANSACTION;
DROP TABLE IF EXISTS %s;
CREATE TABLE %s AS
SELECT
  %s AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src;
COMMIT;`,
		QuoteIdentifier(asset.Name),
		QuoteIdentifier(asset.Name),
		quotedIncrementalKey,
		validuntil,
		strings.TrimSpace(query),
	)

	return strings.TrimSpace(stmt), nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		return buildRedshiftSCD2ByColumnQuery(asset, query)
	}

	query = strings.TrimRight(query, ";")
	var (
		primaryKeys      = make([]string, 0, 4)
		compareConds     = make([]string, 0, 12)
		compareCondsS1T1 = make([]string, 0, 4)
		insertCols       = make([]string, 0, 12)
		insertValues     = make([]string, 0, 12)
	)

	incrementalKey := asset.Materialization.IncrementalKey

	for _, col := range asset.Columns {
		quotedColName := QuoteIdentifier(col.Name)
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		lowerColName := strings.ToLower(col.Name)
		insertCols = append(insertCols, lowerColName)
		insertValues = append(insertValues, "source."+lowerColName)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%s != source.%s", lowerColName, lowerColName))
			compareCondsS1T1 = append(compareCondsS1T1,
				fmt.Sprintf("t1.%s != s1.%s", lowerColName, lowerColName))
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")

	validFromExpr := "CURRENT_TIMESTAMP"
	validUntilUpdateExpr := "CURRENT_TIMESTAMP"
	if incrementalKey != "" {
		lowerIncrementalKey := strings.ToLower(incrementalKey)
		validFromExpr = "source." + lowerIncrementalKey
		validUntilUpdateExpr = "source." + lowerIncrementalKey
	}
	insertValues = append(insertValues, validFromExpr, "'9999-12-31 00:00:00'::TIMESTAMP", "TRUE")

	pkListForUsing := make([]string, 0, len(primaryKeys))
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			pkListForUsing = append(pkListForUsing, strings.ToLower(col.Name))
		}
	}
	pkListUsing := strings.Join(pkListForUsing, ", ")

	onConditions := make([]string, 0, len(primaryKeys)+1)
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			lowerPkName := strings.ToLower(col.Name)
			onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", lowerPkName, lowerPkName))
		}
	}
	onConditions = append(onConditions, "target._is_current AND source._is_current")
	onCondition := strings.Join(onConditions, " AND ")

	var whereCondition string
	var matchedCondition string
	if len(compareCondsS1T1) > 0 {
		whereCondition = "(" + strings.Join(compareCondsS1T1, " OR ") + ")" + " AND t1._is_current"
		matchedCondition = strings.Join(compareConds, " OR ")
	} else {
		whereCondition = "FALSE AND t1._is_current"
		matchedCondition = "FALSE"
	}

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
    _valid_until = %s,
    _is_current  = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP,
    _is_current  = FALSE

WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		QuoteIdentifier(asset.Name),
		strings.TrimSpace(query),
		QuoteIdentifier(asset.Name),
		pkListUsing,
		whereCondition,
		onCondition,
		matchedCondition,
		validUntilUpdateExpr,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

func buildSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
	// Route to Redshift-specific implementation for Redshift assets
	if asset.Type == pipeline.AssetTypeRedshiftQuery {
		return buildRedshiftSCD2QueryByTime(asset, query)
	}

	query = strings.TrimRight(query, ";")

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
		quotedColName := QuoteIdentifier(col.Name)
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
		insertCols = append(insertCols, quotedColName)
		insertValues = append(insertValues, "source."+quotedColName)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}

	// For Postgres USING clause, we need lowercase unquoted identifiers
	// Postgres stores unquoted identifiers as lowercase, so USING needs to match that
	pkListForUsing := make([]string, 0, len(primaryKeys))
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			pkListForUsing = append(pkListForUsing, strings.ToLower(col.Name))
		}
	}
	pkListUsing := strings.Join(pkListForUsing, ", ")

	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(
		insertValues,
		"source."+quotedIncrementalKey,
		"'9999-12-31 00:00:00'",
		"TRUE",
	)

	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	joinConds = append(joinConds, "target._is_current AND source._is_current")
	onCondition := strings.Join(joinConds, " AND ")
	tbl := asset.Name

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
  WHERE  t1._valid_from < s1.%s AND t1._is_current
) AS source
ON  %s

WHEN MATCHED AND (
  target._valid_from < source.%s
) THEN
  UPDATE SET
    _valid_until = source.%s,
    _is_current  = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP,
    _is_current  = FALSE

WHEN NOT MATCHED BY TARGET THEN
  INSERT (%s)
  VALUES (%s);`,
		QuoteIdentifier(tbl),
		strings.TrimSpace(query),
		QuoteIdentifier(tbl),
		pkListUsing,
		quotedIncrementalKey,
		onCondition,
		quotedIncrementalKey,
		quotedIncrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}

// Redshift-specific SCD2 functions - Redshift has different SQL syntax requirements compared to PostgreSQL.
func buildRedshiftSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")
	var (
		primaryKeys  = make([]string, 0, 4)
		compareConds = make([]string, 0, 12)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)

	incrementalKey := asset.Materialization.IncrementalKey

	for _, col := range asset.Columns {
		quotedColName := QuoteIdentifier(col.Name)
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		insertCols = append(insertCols, quotedColName)
		insertValues = append(insertValues, "source."+quotedColName)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%s != source.%s", quotedColName, quotedColName))
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")

	validFromExpr := "(SELECT session_timestamp FROM _ts)"
	if incrementalKey != "" {
		quotedIncrementalKey := QuoteIdentifier(incrementalKey)
		validFromExpr = "source." + quotedIncrementalKey
	}
	insertValues = append(insertValues, validFromExpr, "TIMESTAMP '9999-12-31 00:00:00'", "TRUE")

	onConditions := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onCondition := strings.Join(onConditions, " AND ")

	tempTableName := "__bruin_scd2_tmp_" + helpers.PrefixGenerator()

	var matchedCondition string
	if len(compareConds) > 0 {
		matchedCondition = strings.Join(compareConds, " OR ")
	} else {
		matchedCondition = "FALSE"
	}

	var updateExistsExpr string
	if incrementalKey != "" {
		quotedIncrementalKey := QuoteIdentifier(incrementalKey)
		updateExistsExpr = fmt.Sprintf(
			`UPDATE %s AS target
SET _valid_until = (SELECT %s FROM %s AS source WHERE %s LIMIT 1), _is_current = FALSE
WHERE target._is_current = TRUE
  AND EXISTS (
    SELECT 1 FROM %s AS source
    WHERE %s AND (%s)
  )`,
			QuoteIdentifier(asset.Name),
			"source."+quotedIncrementalKey,
			tempTableName,
			onCondition,
			tempTableName,
			onCondition,
			matchedCondition,
		)
	} else {
		updateExistsExpr = fmt.Sprintf(
			`UPDATE %s AS target
SET _valid_until = (SELECT session_timestamp FROM _ts), _is_current = FALSE
WHERE target._is_current = TRUE
  AND EXISTS (
    SELECT 1 FROM %s AS source
    WHERE %s AND (%s)
  )`,
			QuoteIdentifier(asset.Name),
			tempTableName,
			onCondition,
			matchedCondition,
		)
	}

	queryStr := fmt.Sprintf(
		`
BEGIN TRANSACTION;

-- Capture the timestamp once for the entire transaction
CREATE TEMP TABLE _ts AS 
SELECT CURRENT_TIMESTAMP AS session_timestamp;

-- Create temp table with source data
CREATE TEMP TABLE %s AS 
SELECT *, TRUE AS _is_current FROM (%s) AS src;

-- Update existing records that have changes
%s;

-- Update records that are no longer in source (expired)
UPDATE %s AS target
SET _valid_until = (SELECT session_timestamp FROM _ts), _is_current = FALSE
WHERE target._is_current = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM %s AS source
    WHERE %s
  );

-- Insert new records and new versions of changed records
INSERT INTO %s (%s)
SELECT %s
FROM %s AS source
WHERE NOT EXISTS (
  SELECT 1 FROM %s AS target 
  WHERE %s AND target._is_current = TRUE
);

DROP TABLE %s;
COMMIT;`,
		tempTableName,
		strings.TrimSpace(query),
		updateExistsExpr,
		QuoteIdentifier(asset.Name),
		tempTableName,
		onCondition,
		QuoteIdentifier(asset.Name),
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
		tempTableName,
		QuoteIdentifier(asset.Name),
		onCondition,
		tempTableName,
	)

	return strings.TrimSpace(queryStr), nil
}

func buildRedshiftSCD2QueryByTime(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys  = make([]string, 0, 4)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)
	for _, col := range asset.Columns {
		quotedColName := QuoteIdentifier(col.Name)
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
		insertCols = append(insertCols, quotedColName)
		insertValues = append(insertValues, "source."+quotedColName)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, quotedColName)
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}
	quotedIncrementalKey := QuoteIdentifier(asset.Materialization.IncrementalKey)
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(
		insertValues,
		"source."+quotedIncrementalKey,
		"TIMESTAMP '9999-12-31 00:00:00'",
		"TRUE",
	)

	// Build ON condition
	onConditions := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onCondition := strings.Join(onConditions, " AND ")

	tempTableName := "__bruin_scd2_time_tmp_" + helpers.PrefixGenerator()

	queryStr := fmt.Sprintf(
		`
BEGIN TRANSACTION;

-- Create temp table with source data
CREATE TEMP TABLE %s AS 
SELECT *, TRUE AS _is_current FROM (%s) AS src;

-- Update existing records where source timestamp is newer
UPDATE %s AS target
SET _valid_until = source.%s, _is_current = FALSE
FROM %s AS source
WHERE %s
  AND target._is_current = TRUE
  AND target._valid_from < source.%s;

-- Update records that are no longer in source (expired)
UPDATE %s AS target
SET _valid_until = CURRENT_TIMESTAMP, _is_current = FALSE
WHERE target._is_current = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM %s AS source
    WHERE %s
  );

-- Insert new records and new versions of changed records
INSERT INTO %s (%s)
SELECT %s
FROM %s AS source
WHERE NOT EXISTS (
  SELECT 1 FROM %s AS target 
  WHERE %s AND target._is_current = TRUE
)
OR EXISTS (
  SELECT 1 FROM %s AS target
  WHERE %s AND target._is_current = FALSE 
  AND target._valid_until = source.%s
);

DROP TABLE %s;
COMMIT;`,
		tempTableName,
		strings.TrimSpace(query),
		QuoteIdentifier(asset.Name),
		quotedIncrementalKey,
		tempTableName,
		onCondition,
		quotedIncrementalKey,
		QuoteIdentifier(asset.Name),
		tempTableName,
		onCondition,
		QuoteIdentifier(asset.Name),
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
		tempTableName,
		QuoteIdentifier(asset.Name),
		onCondition,
		QuoteIdentifier(asset.Name),
		onCondition,
		quotedIncrementalKey,
		tempTableName,
	)

	return strings.TrimSpace(queryStr), nil
}
