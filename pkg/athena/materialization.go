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
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildIncrementalQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategyDDL:            buildDDLQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2ByTimeQuery,
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

func buildTruncateInsertQuery(task *pipeline.Asset, query, location string) ([]string, error) {
	// Athena doesn't support TRUNCATE for external tables, use DELETE instead
	queries := []string{
		"DELETE FROM " + task.Name,
		fmt.Sprintf("INSERT INTO %s %s", task.Name, query),
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
		return buildSCD2ByTimeFullRefresh(task, query, location)
	case task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn:
		return buildSCD2ByColumnFullRefresh(task, query, location)
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
		primaryKeys = make([]string, 0, 4)
		userCols    = make([]string, 0, 12)
		nonPKCols   = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		} else {
			nonPKCols = append(nonPKCols, col.Name)
		}
		userCols = append(userCols, col.Name)
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the primary_key field to be set on at least one column", asset.Materialization.Strategy)
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	// Build join conditions for primary keys
	onConds := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", pk, pk)
	}
	joinCondition := strings.Join(onConds, " AND ")

	sourcePKCols := make([]string, len(primaryKeys))
	targetPKCols := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		sourcePKCols[i] = "s." + pk
		targetPKCols[i] = "t." + pk
	}

	changeConditions := make([]string, 0, len(nonPKCols))
	for _, col := range nonPKCols {
		changeConditions = append(changeConditions, fmt.Sprintf("t.%s != s.%s", col, col))
	}
	changeCondition := ""
	if len(changeConditions) > 0 {
		changeCondition = strings.Join(changeConditions, " OR ")
	}

	// Build user column list for SELECTs
	userColList := strings.Join(userCols, ", ")
	allCols := append([]string{}, userCols...)
	allCols = append(allCols, "_valid_from", "_valid_until", "_is_current")
	allColList := strings.Join(allCols, ", ")

	// to_keep SELECT
	toKeepSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toKeepSelectCols = append(toKeepSelectCols, "t."+col)
	}

	// to_insert SELECT
	toInsertSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toInsertSelectCols = append(toInsertSelectCols, fmt.Sprintf("s.%s AS %s", col, col))
	}

	// Build the SQL using array formatting
	sqlLines := []string{
		fmt.Sprintf("CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS", tempTableName, location, tempTableName, partitionBy),
		"WITH",
		"time_now AS (",
		"\tSELECT CURRENT_TIMESTAMP AS now",
		"),",
		"source AS (",
		fmt.Sprintf("\tSELECT %s,", userColList),
		"\tTRUE as _matched_by_source",
		"\tFROM (" + strings.TrimSpace(query),
		"\t)",
		"),",
		"target AS (",
		fmt.Sprintf("\tSELECT %s,", allColList),
		"\tTRUE as _matched_by_target FROM " + asset.Name,
		"),",
		"current_data AS (",
		fmt.Sprintf("\tSELECT %s, _matched_by_target", allColList),
		"\tFROM target as t",
		"\tWHERE _is_current = TRUE",
		"),",
		"--current or updated (expired) existing rows from target",
		"to_keep AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toKeepSelectCols, ", ")),
		"\tt._valid_from,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN (SELECT now FROM time_now)", changeCondition),
		"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)",
		"\t\t\tELSE t._valid_until",
		"\t\tEND AS _valid_until,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN FALSE", changeCondition),
		"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE",
		"\t\t\tELSE t._is_current",
		"\t\tEND AS _is_current",
		"\tFROM target t",
		fmt.Sprintf("\tLEFT JOIN source s ON (%s) AND t._is_current = TRUE", joinCondition),
		"),",
		"--new/updated rows from source",
		"to_insert AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toInsertSelectCols, ", ")),
		"\t(SELECT now FROM time_now) AS _valid_from,",
		"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,",
		"\tTRUE AS _is_current",
		"\tFROM source s",
		fmt.Sprintf("\tLEFT JOIN current_data t ON (%s)", joinCondition),
		fmt.Sprintf("\tWHERE (_matched_by_target IS NULL) OR (%s)", changeCondition),
		")",
		fmt.Sprintf("SELECT %s FROM to_keep", allColList),
		"UNION ALL",
		fmt.Sprintf("SELECT %s FROM to_insert;", allColList),
	}
	// Join all lines into a single string
	createQuery := strings.Join(sqlLines, "\n")
	return []string{
		strings.TrimSpace(createQuery),
		"\nDROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("\nALTER TABLE %s RENAME TO %s;", tempTableName, asset.Name),
	}, nil
}

func buildSCD2ByColumnFullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	srcCols := make([]string, len(asset.Columns))
	for i, col := range asset.Columns {
		srcCols[i] = "src." + col.Name
	}

	createQuery := fmt.Sprintf(
		"CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS\n"+
			"SELECT %s,\n"+
			"CURRENT_TIMESTAMP AS _valid_from,\n"+
			"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n"+
			"TRUE AS _is_current\n"+
			"FROM (%s\n"+
			") AS src",
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		strings.Join(srcCols, ", "),
		strings.TrimSpace(query),
	)

	return []string{
		strings.TrimSpace(createQuery),
		"\nDROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("\nALTER TABLE %s RENAME TO %s;", tempTableName, asset.Name),
	}, nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query, location string) ([]string, error) {
	query = strings.TrimSuffix(query, ";")

	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	incrementalKey := asset.Materialization.IncrementalKey

	var (
		primaryKeys = make([]string, 0, 4)
		userCols    = make([]string, 0, 12)
	)

	for _, col := range asset.Columns {
		switch col.Name {
		case "_is_current", "_valid_from", "_valid_until":
			return nil, fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if lcType != "timestamp" && lcType != "date" {
				return nil, errors.New("incremental_key must be TIMESTAMP or DATE in SCD2_by_time strategy")
			}
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		userCols = append(userCols, col.Name)
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the primary_key field to be set on at least one column", asset.Materialization.Strategy)
	}

	var partitionBy string
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	// Build join conditions for primary keys
	onConds := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		onConds[i] = fmt.Sprintf("t.%s = s.%s", pk, pk)
	}
	joinCondition := strings.Join(onConds, " AND ")

	changeCondition := fmt.Sprintf("CAST(s.%s AS TIMESTAMP) > t._valid_from", incrementalKey)

	// Build user column list for SELECTs
	userColList := strings.Join(userCols, ", ")
	allCols := append([]string{}, userCols...)
	allCols = append(allCols, "_valid_from", "_valid_until", "_is_current")
	allColList := strings.Join(allCols, ", ")

	// to_keep SELECT
	toKeepSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toKeepSelectCols = append(toKeepSelectCols, "t."+col)
	}

	// to_insert SELECT
	toInsertSelectCols := make([]string, 0, len(userCols)+3)
	for _, col := range userCols {
		toInsertSelectCols = append(toInsertSelectCols, fmt.Sprintf("s.%s AS %s", col, col))
	}

	// Build the SQL using array formatting
	sqlLines := []string{
		fmt.Sprintf("CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS", tempTableName, location, tempTableName, partitionBy),
		"WITH",
		"time_now AS (",
		"\tSELECT CURRENT_TIMESTAMP AS now",
		"),",
		"source AS (",
		fmt.Sprintf("\tSELECT %s,", userColList),
		"\tTRUE as _matched_by_source",
		"\tFROM (" + strings.TrimSpace(query),
		"\t)",
		"),",
		"target AS (",
		fmt.Sprintf("\tSELECT %s,", allColList),
		"\tTRUE as _matched_by_target FROM " + asset.Name,
		"),",
		"current_data AS (",
		fmt.Sprintf("\tSELECT %s, _matched_by_target", allColList),
		"\tFROM target as t",
		"\tWHERE _is_current = TRUE",
		"),",
		"--current or updated (expired) existing rows from target",
		"to_keep AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toKeepSelectCols, ", ")),
		"\tt._valid_from,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN CAST(s.%s AS TIMESTAMP)", changeCondition, incrementalKey),
		"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)",
		"\t\t\tELSE t._valid_until",
		"\t\tEND AS _valid_until,",
		"\t\tCASE",
		fmt.Sprintf("\t\t\tWHEN _matched_by_source IS NOT NULL AND (%s) THEN FALSE", changeCondition),
		"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE",
		"\t\t\tELSE t._is_current",
		"\t\tEND AS _is_current",
		"\tFROM target t",
		fmt.Sprintf("\tLEFT JOIN source s ON (%s) AND t._is_current = TRUE", joinCondition),
		"),",
		"--new/updated rows from source",
		"to_insert AS (",
		fmt.Sprintf("\tSELECT %s,", strings.Join(toInsertSelectCols, ", ")),
		fmt.Sprintf("\tCAST(s.%s AS TIMESTAMP) AS _valid_from,", incrementalKey),
		"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,",
		"\tTRUE AS _is_current",
		"\tFROM source s",
		fmt.Sprintf("\tLEFT JOIN current_data t ON (%s)", joinCondition),
		fmt.Sprintf("\tWHERE (_matched_by_target IS NULL) OR (%s)", changeCondition),
		")",
		fmt.Sprintf("SELECT %s FROM to_keep", allColList),
		"UNION ALL",
		fmt.Sprintf("SELECT %s FROM to_insert;", allColList),
	}
	// Join all lines into a single string
	createQuery := strings.Join(sqlLines, "\n")
	return []string{
		strings.TrimSpace(createQuery),
		"\nDROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("\nALTER TABLE %s RENAME TO %s;", tempTableName, asset.Name),
	}, nil
}

func buildSCD2ByTimeFullRefresh(asset *pipeline.Asset, query, location string) ([]string, error) {
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

	tempTableName := "__bruin_tmp_" + helpers.PrefixGenerator()

	srcCols := make([]string, len(asset.Columns))
	for i, col := range asset.Columns {
		srcCols[i] = "src." + col.Name
	}

	createQuery := fmt.Sprintf(
		"CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS\n"+
			"SELECT %s,\n"+
			"CAST(src.%s AS TIMESTAMP) AS _valid_from,\n"+
			"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n"+
			"TRUE AS _is_current\n"+
			"FROM (%s\n"+
			") AS src",
		tempTableName,
		location,
		tempTableName,
		partitionBy,
		strings.Join(srcCols, ", "),
		asset.Materialization.IncrementalKey,
		strings.TrimSpace(query),
	)

	return []string{
		strings.TrimSpace(createQuery),
		"\nDROP TABLE IF EXISTS " + asset.Name,
		fmt.Sprintf("\nALTER TABLE %s RENAME TO %s;", tempTableName, asset.Name),
	}, nil
}
