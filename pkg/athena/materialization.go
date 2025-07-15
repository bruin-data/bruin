package athena

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type (
	MaterializerFunc        func(task *pipeline.Asset, query, location string) ([]string, error)
	AssetMaterializationMap map[pipeline.MaterializationType]map[pipeline.MaterializationStrategy]MaterializerFunc
)

var matMap = AssetMaterializationMap{
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
		pipeline.MaterializationStrategyMerge:         buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:  buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:           buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:  buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:    buildSCD2ByTimeQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query, location string) ([]string, error) {
	return nil, fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query, location string) ([]string, error) {
	return []string{fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query)}, nil
}

func buildAppendQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	return []string{fmt.Sprintf("INSERT INTO %s %s", asset.Name, query)}, nil
}

func buildIncrementalQuery(task *pipeline.Asset, query, location string) ([]string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return []string{}, fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	queries := []string{
		fmt.Sprintf("CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s') AS %s\n", tempTableName, location, tempTableName, query),
		fmt.Sprintf("\nDELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
	}

	return queries, nil
}

func buildMergeQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	if len(asset.Columns) == 0 {
		return []string{}, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", asset.Materialization.Strategy)
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return []string{}, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", asset.Materialization.Strategy)
	}

	nonPrimaryKeys := asset.ColumnNamesWithUpdateOnMerge()
	columnNames := asset.ColumnNames()

	on := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		on = append(on, fmt.Sprintf("target.%s = source.%s", key, key))
	}
	onQuery := strings.Join(on, " AND ")

	columnNamesWithSource := make([]string, len(columnNames))
	for i, col := range columnNames {
		columnNamesWithSource[i] = "source." + col
	}

	allColumnValues := strings.Join(columnNames, ", ")

	whenMatchedThenQuery := ""

	if len(nonPrimaryKeys) > 0 {
		matchedUpdateStatements := make([]string, 0, len(nonPrimaryKeys))
		for _, col := range nonPrimaryKeys {
			matchedUpdateStatements = append(matchedUpdateStatements, fmt.Sprintf("%s = source.%s", col, col))
		}

		matchedUpdateQuery := strings.Join(matchedUpdateStatements, ", ")
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + matchedUpdateQuery
	}

	queries := []string{
		fmt.Sprintf("MERGE INTO %s target", asset.Name),
		fmt.Sprintf("USING (%s) source ON %s", strings.TrimSuffix(query, ";"), onQuery),
		whenMatchedThenQuery,
		fmt.Sprintf("WHEN NOT MATCHED THEN INSERT(%s) VALUES(%s)", allColumnValues, strings.Join(columnNamesWithSource, ", ")),
	}

	return []string{strings.Join(queries, " ")}, nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query, location string) ([]string, error) {
	switch {
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime:
		return buildSCD2ByTimefullRefresh(task, query, location)
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnfullRefresh(task, query, location)
	default:
		query = strings.TrimSuffix(query, ";")

		tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

		var partitionBy string
		if task.Materialization.PartitionBy != "" {
			partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", task.Materialization.PartitionBy)
		}

		return []string{
			fmt.Sprintf(
				"CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS %s",
				tempTableName,
				location,
				tempTableName,
				partitionBy,
				query,
			),
			"DROP TABLE IF EXISTS " + task.Name,
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempTableName, task.Name),
		}, nil
	}
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string, location string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for time_interval strategy")
	}
	if asset.Materialization.TimeGranularity == "" {
		return nil, errors.New("time_granularity is required for time_interval strategy")
	}
	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp || asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return nil, errors.New("time_granularity must be either 'date', or 'timestamp'")
	}

	timePrefix := "timestamp"
	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
		timePrefix = "date"
	}

	queries := []string{
		fmt.Sprintf(`DELETE FROM %s WHERE %s BETWEEN %s '%s' AND %s '%s'`,
			asset.Name,
			asset.Materialization.IncrementalKey,
			timePrefix,
			startVar,
			timePrefix,
			endVar),
		fmt.Sprintf(`INSERT INTO %s %s`,
			asset.Name, query),
	}

	return queries, nil
}

func buildDDLQuery(asset *pipeline.Asset, query string, location string) ([]string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)
		if col.Description != "" {
			desc := strings.ReplaceAll(col.Description, `'`, `''`)
			def += fmt.Sprintf(" COMMENT '%s'", desc)
		}
		columnDefs = append(columnDefs, def)
	}

	partitionedBy := ""
	if asset.Materialization.PartitionBy != "" {
		partitionedBy = fmt.Sprintf("\nPARTITIONED BY (%s)", asset.Materialization.PartitionBy)
	}

	ddlQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n"+
			"%s\n"+
			")"+
			"%s"+
			"\nLOCATION '%s/%s'"+
			"\nTBLPROPERTIES('table_type'='ICEBERG')",
		asset.Name,
		strings.Join(columnDefs, ",\n"),
		partitionedBy,
		location,
		asset.Name,
	)

	return []string{ddlQuery}, nil
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	query = strings.TrimSuffix(query, ";")

	var (
		primaryKeys  = make([]string, 0, 4)
		compareConds = make([]string, 0, 12)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)
		if !col.PrimaryKey {
			compareConds = append(compareConds,
				fmt.Sprintf("target.%[1]s != source.%[1]s", col.Name))
		}
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues, "CURRENT_TIMESTAMP", "TIMESTAMP '9999-12-31'", "TRUE")

	// Build ON condition for MERGE
	onConditions := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onConditions = append(onConditions, "target._is_current = TRUE")
	onCondition := strings.Join(onConditions, " AND ")

	var matchedCondition string
	if len(compareConds) > 0 {
		matchedCondition = strings.Join(compareConds, " OR ")
	} else {
		matchedCondition = "FALSE"
	}

	mergeQuery := fmt.Sprintf(`
MERGE INTO %s AS target
USING (%s) AS source
ON %s

WHEN MATCHED AND (%s) THEN
  UPDATE SET
    _valid_until = CURRENT_TIMESTAMP,
    _is_current = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP,
    _is_current = FALSE

WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)`,
		asset.Name,
		strings.TrimSpace(query),
		onCondition,
		matchedCondition,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return []string{strings.TrimSpace(mergeQuery)}, nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	query = strings.TrimSuffix(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys  = make([]string, 0, 4)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_valid_from", "_valid_until", "_is_current":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return nil, errors.New("incremental_key must be TIMESTAMP or DATE in SCD2_by_time strategy")
			}
		}
		insertCols = append(insertCols, col.Name)
		insertValues = append(insertValues, "source."+col.Name)

		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(insertValues,
		"CAST(source."+asset.Materialization.IncrementalKey+" AS TIMESTAMP)",
		"TIMESTAMP '9999-12-31'",
		"TRUE",
	)

	// Build ON condition for MERGE
	onConditions := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", pk, pk))
	}
	onConditions = append(onConditions, "target._is_current = TRUE")
	onCondition := strings.Join(onConditions, " AND ")

	mergeQuery := fmt.Sprintf(`
MERGE INTO %s AS target
USING (%s) AS source
ON %s

WHEN MATCHED AND (target._valid_from < CAST(source.%s AS TIMESTAMP)) THEN
  UPDATE SET
    _valid_until = CAST(source.%s AS TIMESTAMP),
    _is_current = FALSE

WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN
  UPDATE SET 
    _valid_until = CURRENT_TIMESTAMP,
    _is_current = FALSE

WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)`,
		asset.Name,
		strings.TrimSpace(query),
		onCondition,
		asset.Materialization.IncrementalKey,
		asset.Materialization.IncrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return []string{strings.TrimSpace(mergeQuery)}, nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, errors.New("materialization strategy 'SCD2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	createQuery := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS
SELECT
  CAST(%s AS TIMESTAMP) AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		asset.Name,
		location,
		asset.Name,
		partitionBy,
		asset.Materialization.IncrementalKey,
		strings.TrimSpace(query),
	)

	return []string{strings.TrimSpace(createQuery)}, nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	createQuery := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS
SELECT
  CURRENT_TIMESTAMP AS _valid_from,
  src.*,
  TIMESTAMP '9999-12-31' AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		asset.Name,
		location,
		asset.Name,
		partitionBy,
		strings.TrimSpace(query),
	)

	return []string{strings.TrimSpace(createQuery)}, nil
}
