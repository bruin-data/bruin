package duck

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

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
__bruin_ranked AS (
  SELECT
    %s,
    ROW_NUMBER() OVER (PARTITION BY source.%s ORDER BY source.%s ASC) AS __bruin_row_number
  FROM __bruin_source AS source
  WHERE %s
),
__bruin_dedup AS (
  SELECT %s
  FROM __bruin_ranked AS source
  WHERE source.__bruin_row_number = 1
)
INSERT INTO %s (%s)
SELECT %s
FROM __bruin_dedup AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM %s AS target
  WHERE target.%s = source.%s
)`,
		trimMaterializationQuery(query),
		dataVaultIndentedSourceSelectList(asset, 4),
		hashKey.Name,
		loadDatetime.Name,
		strings.Join(dataVaultNotNullConditions("source", mandatoryColumns), " AND "),
		strings.Join(dataVaultColumnNames(asset), ", "),
		asset.Name,
		strings.Join(dataVaultColumnNames(asset), ", "),
		dataVaultSourceSelectList(asset),
		asset.Name,
		hashKey.Name,
		hashKey.Name,
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
__bruin_ranked AS (
  SELECT
    %s,
    ROW_NUMBER() OVER (PARTITION BY source.%s ORDER BY source.%s ASC) AS __bruin_row_number
  FROM __bruin_source AS source
  WHERE %s
),
__bruin_dedup AS (
  SELECT %s
  FROM __bruin_ranked AS source
  WHERE source.__bruin_row_number = 1
)
INSERT INTO %s (%s)
SELECT %s
FROM __bruin_dedup AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM %s AS target
  WHERE target.%s = source.%s
)`,
		trimMaterializationQuery(query),
		dataVaultIndentedSourceSelectList(asset, 4),
		linkHashKey.Name,
		loadDatetime.Name,
		strings.Join(dataVaultNotNullConditions("source", mandatoryColumns), " AND "),
		strings.Join(dataVaultColumnNames(asset), ", "),
		asset.Name,
		strings.Join(dataVaultColumnNames(asset), ", "),
		dataVaultSourceSelectList(asset),
		asset.Name,
		linkHashKey.Name,
		linkHashKey.Name,
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
__bruin_dedup AS (
  SELECT %s
  FROM (
    SELECT
      valid.*,
      ROW_NUMBER() OVER (PARTITION BY %s ORDER BY valid.%s) AS __bruin_pk_row_number
    FROM __bruin_valid AS valid
  ) AS ranked
  WHERE ranked.__bruin_pk_row_number = 1
),
__bruin_ordered AS (
  SELECT
    dedup.*,
    LAG(dedup.%s) OVER (PARTITION BY dedup.%s ORDER BY dedup.%s, dedup.%s) AS __bruin_previous_hashdiff,
    ROW_NUMBER() OVER (PARTITION BY dedup.%s ORDER BY dedup.%s, dedup.%s) AS __bruin_row_number
  FROM __bruin_dedup AS dedup
),
__bruin_latest AS (
  SELECT %s
  FROM (
    SELECT
      %s,
      ROW_NUMBER() OVER (PARTITION BY target.%s ORDER BY target.%s DESC) AS __bruin_latest_row_number
    FROM %s AS target
    WHERE target.%s IS NOT NULL
  ) AS ranked_latest
  WHERE ranked_latest.__bruin_latest_row_number = 1
)
INSERT INTO %s (%s)
SELECT %s
FROM __bruin_ordered AS source
LEFT JOIN __bruin_latest AS latest
  ON latest.%s = source.%s
WHERE (
    (
      source.__bruin_row_number = 1
      AND (latest.%s IS NULL OR latest.%s IS DISTINCT FROM source.%s)
    )
    OR (
      source.__bruin_row_number > 1
      AND source.__bruin_previous_hashdiff IS DISTINCT FROM source.%s
    )
  )
  AND NOT EXISTS (
    SELECT 1
    FROM %s AS target
    WHERE %s
  )`,
		trimMaterializationQuery(query),
		dataVaultIndentedSourceSelectList(asset, 4),
		strings.Join(dataVaultNotNullConditions("source", mandatoryColumns), " AND "),
		strings.Join(dataVaultColumnNames(asset), ", "),
		strings.Join(dataVaultAliasColumnNames("valid", primaryKeys), ", "),
		hashDiff.Name,
		hashDiff.Name,
		parentHashKey.Name,
		loadDatetime.Name,
		hashDiff.Name,
		parentHashKey.Name,
		loadDatetime.Name,
		hashDiff.Name,
		strings.Join([]string{parentHashKey.Name, hashDiff.Name}, ", "),
		strings.Join(dataVaultAliasColumnNames("target", []string{parentHashKey.Name, hashDiff.Name}), ",\n      "),
		parentHashKey.Name,
		loadDatetime.Name,
		asset.Name,
		parentHashKey.Name,
		asset.Name,
		strings.Join(dataVaultColumnNames(asset), ", "),
		dataVaultSourceSelectList(asset),
		parentHashKey.Name,
		parentHashKey.Name,
		parentHashKey.Name,
		hashDiff.Name,
		hashDiff.Name,
		hashDiff.Name,
		asset.Name,
		strings.Join(dataVaultEqualityConditions("target", "source", primaryKeys), " AND "),
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

		def := fmt.Sprintf("%s %s", col.Name, col.Type)
		if mandatoryColumnNames[strings.ToLower(col.Name)] || !col.Nullable.Bool() {
			def += " NOT NULL"
		}
		columnDefs = append(columnDefs, def)
	}

	if len(primaryKeys) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", ")))
	}

	statements := make([]string, 0, 4)
	if schemaStatement := createSchemaStatement(asset.Name); schemaStatement != "" {
		statements = append(statements, schemaStatement)
	}
	if fullRefresh {
		statements = append(statements, "DROP TABLE IF EXISTS "+asset.Name)
	}
	statements = append(statements, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		asset.Name,
		strings.Join(columnDefs, ",\n  "),
	))

	return statements, nil
}

func createSchemaStatement(name string) string {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return ""
	}
	return "CREATE SCHEMA IF NOT EXISTS " + parts[0]
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

func dataVaultIndentedSourceSelectList(asset *pipeline.Asset, spaces int) string {
	return strings.Join(dataVaultSelectColumns(asset, "source"), ",\n"+strings.Repeat(" ", spaces))
}

func dataVaultSelectColumns(asset *pipeline.Asset, tableAlias string) []string {
	columns := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		columns = append(columns, fmt.Sprintf("%s.%s", tableAlias, col.Name))
	}
	return columns
}

func dataVaultColumnNames(asset *pipeline.Asset) []string {
	columns := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		columns = append(columns, col.Name)
	}
	return columns
}

func dataVaultAliasColumnNames(tableAlias string, columns []string) []string {
	aliased := make([]string, 0, len(columns))
	for _, col := range columns {
		aliased = append(aliased, tableAlias+"."+col)
	}
	return aliased
}

func dataVaultEqualityConditions(leftAlias, rightAlias string, columns []string) []string {
	conditions := make([]string, 0, len(columns))
	for _, col := range columns {
		conditions = append(conditions, fmt.Sprintf("%s.%s = %s.%s", leftAlias, col, rightAlias, col))
	}
	return conditions
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
		conditions = append(conditions, fmt.Sprintf("%s.%s IS NOT NULL", tableAlias, col.Name))
	}
	return conditions
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
