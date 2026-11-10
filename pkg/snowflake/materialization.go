package snowflake

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
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
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2ByTimeQuery,
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

// tableKeyword returns the object keyword used when Bruin creates the asset's table.
func tableKeyword(asset *pipeline.Asset) string {
	if asset.Materialization.IsIceberg() {
		return "ICEBERG TABLE"
	}
	return "TABLE"
}

// icebergTableOptions returns the options for a Snowflake-managed Iceberg table. Without them, Snowflake
// uses the default external volume and builds the base location from the table name.
func icebergTableOptions(asset *pipeline.Asset) string {
	if !asset.Materialization.IsIceberg() {
		return ""
	}

	options := []string{"CATALOG = 'SNOWFLAKE'"}
	if asset.Snowflake.ExternalVolume != "" {
		options = append(options, fmt.Sprintf("EXTERNAL_VOLUME = '%s'", escapeSQLString(asset.Snowflake.ExternalVolume)))
	}
	if asset.Snowflake.BaseLocation != "" {
		options = append(options, fmt.Sprintf("BASE_LOCATION = '%s'", escapeSQLString(asset.Snowflake.BaseLocation)))
	}
	return " " + strings.Join(options, " ")
}

// scd2TimestampTypes returns the SCD2 validity column type and the open-ended _valid_until value.
// Iceberg tables don't support TIMESTAMP_TZ, so they use TIMESTAMP_LTZ(6) with the session in UTC.
func scd2TimestampTypes(asset *pipeline.Asset) (string, string) {
	if asset.Materialization.IsIceberg() {
		return "TIMESTAMP_LTZ(6)", "CAST('9999-12-31 23:59:59' AS TIMESTAMP_LTZ(6))"
	}
	return "TIMESTAMP_TZ", "TO_TIMESTAMP_TZ('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"
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
		fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, strings.TrimSuffix(query, ";")),
		ansisql.BuildIncrementalDelete(task.Name, fmt.Sprintf("%s in (SELECT DISTINCT %s FROM %s)", mat.IncrementalKey, mat.IncrementalKey, tempTableName), mat.IncrementalPredicate),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) (string, error) {
	if task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn {
		return buildSCD2ByColumnfullRefresh(task, query)
	}
	if task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime {
		return buildSCD2ByTimefullRefresh(task, query)
	}

	mat := task.Materialization

	clusterByClause := ""
	if len(mat.ClusterBy) > 0 {
		clusterByClause = fmt.Sprintf("CLUSTER BY (%s)", strings.Join(mat.ClusterBy, ", "))
	}

	return fmt.Sprintf("CREATE OR REPLACE %s %s %s%s AS\n%s", tableKeyword(task), task.Name, clusterByClause, icebergTableOptions(task), query), nil
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
	on = ansisql.AddIncrementalPredicate(on, asset.Materialization.IncrementalPredicate)
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

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == "date" {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}
	if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp && asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return "", errors.New("time_granularity must be either 'date', or 'timestamp'")
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

func buildDDLQuery(asset *pipeline.Asset, query string) (string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))
	primaryKeys := make([]string, 0)
	foreignKeys := make([]string, 0)
	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.SQLType())
		if col.Collation != "" {
			def += fmt.Sprintf(" COLLATE '%s'", col.Collation)
		}
		if col.Default != "" {
			def += " DEFAULT " + col.Default
		}
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
		if col.ForeignKey != nil && col.ForeignKey.Table != "" && col.ForeignKey.Column != "" {
			foreignKeys = append(foreignKeys, fmt.Sprintf(
				"foreign key (%s) references %s (%s)",
				col.Name, col.ForeignKey.Table, col.ForeignKey.Column,
			))
		}
		if col.Description != "" {
			desc := strings.ReplaceAll(col.Description, `'`, `''`)
			def += fmt.Sprintf(" COMMENT '%s'", desc)
		}
		columnDefs = append(columnDefs, def)
	}
	clusterByClause := ""
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterByClause = "CLUSTER BY (" + strings.Join(asset.Materialization.ClusterBy, ", ") + ") "
	}
	primaryKeyClause := ""
	if len(primaryKeys) > 0 {
		primaryKeyClause = fmt.Sprintf(",\nprimary key (%s)", strings.Join(primaryKeys, ", "))
	}
	foreignKeyClause := ""
	if len(foreignKeys) > 0 {
		foreignKeyClause = ",\n" + strings.Join(foreignKeys, ",\n")
	}
	ddl := fmt.Sprintf(
		"CREATE %s IF NOT EXISTS %s %s(\n"+
			"%s%s%s\n"+
			")%s",
		tableKeyword(asset),
		asset.Name,
		clusterByClause,
		strings.Join(columnDefs, ",\n"),
		primaryKeyClause,
		foreignKeyClause,
		icebergTableOptions(asset),
	)

	return ddl, nil
}

//nolint:unparam
func buildPKConditions(primaryKeys []string, leftAlias, rightAlias string) []string {
	conditions := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		conditions[i] = fmt.Sprintf("%s.%s = %s.%s", leftAlias, pk, rightAlias, pk)
	}
	return conditions
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) (string, error) {
	query = strings.TrimRight(query, ";")
	var (
		primaryKeys  = make([]string, 0, 4)
		compareConds = make([]string, 0, 12)
		insertCols   = make([]string, 0, 12)
		insertValues = make([]string, 0, 12)
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
		}
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column",
			asset.Materialization.Strategy)
	}

	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")

	tbl := asset.Name
	timestampType, maxValidUntil := scd2TimestampTypes(asset)

	if incrementalKey != "" {
		validFromExpr := fmt.Sprintf("CAST(source.%s AS %s)", incrementalKey, timestampType)
		insertValues = append(insertValues, validFromExpr, maxValidUntil, "TRUE")

		queryStr := fmt.Sprintf(
			`
BEGIN TRANSACTION;

-- Step 1: Update expired records that are no longer in source
UPDATE %s AS target
SET _valid_until = CURRENT_TIMESTAMP(), _is_current = FALSE
WHERE target._is_current = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM (%s) AS source 
    WHERE %s
  );

-- Step 2: Update existing records that have changes
UPDATE %s AS target
SET _valid_until = (SELECT CAST(source.%s AS %s) FROM (%s) AS source WHERE %s LIMIT 1), _is_current = FALSE
WHERE target._is_current = TRUE
  AND EXISTS (
    SELECT 1 FROM (%s) AS source
    WHERE %s AND (%s)
  );

-- Step 3: Insert new records and new versions of changed records
INSERT INTO %s (%s)
SELECT %s
FROM (%s) AS source
WHERE NOT EXISTS (
  SELECT 1 FROM %s AS target 
  WHERE %s AND target._is_current = TRUE
);

COMMIT;`,
			tbl,
			strings.TrimSpace(query),
			strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
			tbl,
			incrementalKey,
			timestampType,
			strings.TrimSpace(query),
			strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
			strings.TrimSpace(query),
			strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
			strings.Join(compareConds, " OR "),
			tbl,
			strings.Join(insertCols, ", "),
			strings.Join(insertValues, ", "),
			strings.TrimSpace(query),
			tbl,
			strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
		)

		return "ALTER SESSION SET TIMEZONE = 'UTC';\n" + strings.TrimSpace(queryStr), nil
	}

	insertValues = append(insertValues, "$current_scd2_ts", maxValidUntil, "TRUE")

	queryStr := fmt.Sprintf(
		`
BEGIN TRANSACTION;

-- Capture timestamp once for consistency across all operations
SET current_scd2_ts = CURRENT_TIMESTAMP();

-- Step 1: Update expired records that are no longer in source
UPDATE %s AS target
SET _valid_until = $current_scd2_ts, _is_current = FALSE
WHERE target._is_current = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM (%s) AS source 
    WHERE %s
  );

-- Step 2: Update existing records that have changes
UPDATE %s AS target
SET _valid_until = $current_scd2_ts, _is_current = FALSE
WHERE target._is_current = TRUE
  AND EXISTS (
    SELECT 1 FROM (%s) AS source
    WHERE %s AND (%s)
  );

-- Step 3: Insert new records and new versions of changed records
INSERT INTO %s (%s)
SELECT %s
FROM (%s) AS source
WHERE NOT EXISTS (
  SELECT 1 FROM %s AS target 
  WHERE %s AND target._is_current = TRUE
)
OR EXISTS (
  SELECT 1 FROM %s AS target
  WHERE %s AND target._is_current = FALSE AND target._valid_until = $current_scd2_ts
);

COMMIT;`,
		tbl,
		strings.TrimSpace(query),
		strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
		tbl,
		strings.TrimSpace(query),
		strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
		strings.Join(compareConds, " OR "),
		tbl,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
		strings.TrimSpace(query),
		tbl,
		strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
		tbl,
		strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
	)

	return "ALTER SESSION SET TIMEZONE = 'UTC';\n" + strings.TrimSpace(queryStr), nil
}

func buildSCD2ByColumnfullRefresh(asset *pipeline.Asset, query string) (string, error) {
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_column' requires the `primary_key` field to be set on at least one column")
	}

	tbl := asset.Name
	cluster := strings.Join(primaryKeys, ", ")

	var clusterByClause string
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterByClause = fmt.Sprintf("CLUSTER BY (%s)", strings.Join(asset.Materialization.ClusterBy, ", "))
	} else {
		clusterByClause = fmt.Sprintf("CLUSTER BY (_is_current, %s)", cluster)
	}

	timestampType, maxValidUntil := scd2TimestampTypes(asset)
	validFromExpr := fmt.Sprintf("CAST(CURRENT_TIMESTAMP() AS %s)", timestampType)
	if asset.Materialization.IncrementalKey != "" {
		validFromExpr = fmt.Sprintf("CAST(%s AS %s)", asset.Materialization.IncrementalKey, timestampType)
	}

	stmt := fmt.Sprintf(
		`CREATE OR REPLACE %s %s %s%s AS
SELECT
  %s AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		tableKeyword(asset),
		tbl,
		clusterByClause,
		icebergTableOptions(asset),
		validFromExpr,
		maxValidUntil,
		strings.TrimSpace(query),
	)

	return "ALTER SESSION SET TIMEZONE = 'UTC';\n" + strings.TrimSpace(stmt), nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query string) (string, error) {
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
		switch col.Name {
		case "_valid_from", "_valid_until", "_is_current":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			lcType := strings.ToLower(col.Type)
			if !strings.Contains(lcType, "timestamp") && !strings.Contains(lcType, "date") {
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
	timestampType, maxValidUntil := scd2TimestampTypes(asset)
	insertCols = append(insertCols, "_valid_from", "_valid_until", "_is_current")
	insertValues = append(
		insertValues,
		"CAST(source."+asset.Materialization.IncrementalKey+" AS "+timestampType+")",
		maxValidUntil,
		"TRUE",
	)

	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf("target.%[1]s = source.%[1]s", pk))
	}
	joinConds = append(joinConds, "target._is_current AND source._is_current")
	onCondition := strings.Join(joinConds, " AND ")
	tbl := asset.Name

	queryStr := fmt.Sprintf(
		`
BEGIN TRANSACTION;

-- Capture timestamp once for consistency across all operations
SET current_scd2_ts = CURRENT_TIMESTAMP();

-- Step 1: Update expired records that are no longer in source
UPDATE %s AS target
SET _valid_until = $current_scd2_ts, _is_current = FALSE
WHERE target._is_current = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM (%s) AS source 
    WHERE %s
  );

-- Step 2: Handle new and changed records
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
  WHERE  t1._valid_from < CAST(s1.%s AS %s) AND t1._is_current
) AS source
ON  %s

WHEN MATCHED AND (
  target._valid_from < CAST(source.%s AS %s)
) THEN
  UPDATE SET
    target._valid_until = CAST(source.%s AS %s),
    target._is_current  = FALSE

WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s);

COMMIT;`,
		tbl,
		strings.TrimSpace(query),
		strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND "),
		tbl,
		strings.TrimSpace(query),
		tbl,
		pkList,
		asset.Materialization.IncrementalKey,
		timestampType,
		onCondition,
		asset.Materialization.IncrementalKey,
		timestampType,
		asset.Materialization.IncrementalKey,
		timestampType,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return "ALTER SESSION SET TIMEZONE = 'UTC';\n" + strings.TrimSpace(queryStr), nil
}

func buildSCD2ByTimefullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2 strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	tbl := asset.Name
	cluster := strings.Join(primaryKeys, ", ")

	var clusterByClause string
	if len(asset.Materialization.ClusterBy) > 0 {
		clusterByClause = fmt.Sprintf("CLUSTER BY (%s)", strings.Join(asset.Materialization.ClusterBy, ", "))
	} else {
		clusterByClause = fmt.Sprintf("CLUSTER BY (_is_current, %s)", cluster)
	}

	timestampType, maxValidUntil := scd2TimestampTypes(asset)
	stmt := fmt.Sprintf(
		`CREATE OR REPLACE %s %s %s%s AS
SELECT
  CAST(%s AS %s) AS _valid_from,
  src.*,
  %s AS _valid_until,
  TRUE AS _is_current
FROM (
%s
) AS src`,
		tableKeyword(asset),
		tbl,
		clusterByClause,
		icebergTableOptions(asset),
		asset.Materialization.IncrementalKey,
		timestampType,
		maxValidUntil,
		strings.TrimSpace(query),
	)

	return "ALTER SESSION SET TIMEZONE = 'UTC';\n" + strings.TrimSpace(stmt), nil
}
