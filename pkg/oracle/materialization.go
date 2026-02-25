package oracle

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// validIdentifierRe matches Oracle unquoted identifiers.
// Allows schema-qualified names like "MY_SCHEMA.MY_TABLE".
var validIdentifierRe = regexp.MustCompile(`^[A-Za-z_#$][A-Za-z0-9_#$.]*$`)

// validateIdentifier rejects names that could cause SQL injection when
// interpolated outside of EXECUTE IMMEDIATE string literals.
func validateIdentifier(name, kind string) error {
	if !validIdentifierRe.MatchString(name) {
		return fmt.Errorf("invalid Oracle %s: %q contains unsupported characters", kind, name)
	}
	return nil
}

func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        fullRefresh,
	}
}

var matMap = pipeline.AssetMaterializationMap{
	pipeline.MaterializationTypeView: {
		pipeline.MaterializationStrategyNone:           viewMaterializer,
		pipeline.MaterializationStrategyCreateReplace:  viewMaterializer,
		pipeline.MaterializationStrategyAppend:         errorMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:   errorMaterializer,
		pipeline.MaterializationStrategyTruncateInsert: errorMaterializer,
		pipeline.MaterializationStrategyMerge:          errorMaterializer,
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildIncrementalQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTimeInterval:   buildTimeIntervalQuery,
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2ByTimeQuery,
	},
}

// escapeOracleString doubles single quotes for use inside PL/SQL string literals (EXECUTE IMMEDIATE).
func escapeOracleString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s", asset.Materialization.Strategy, asset.Materialization.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	if err := validateIdentifier(asset.Name, "view name"); err != nil {
		return "", err
	}
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query), nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) (string, error) {
	if task.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime {
		return buildSCD2ByTimeFullRefresh(task, query)
	}

	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	// Oracle (pre-23c) lacks CREATE OR REPLACE TABLE, so we use a PL/SQL block
	// that drops (with exception handling for non-existent tables) then creates.
	// DDL requires EXECUTE IMMEDIATE inside PL/SQL; single quotes in the query
	// and identifiers are escaped for the string literal context.
	escapedQuery := escapeOracleString(query)
	escapedName := escapeOracleString(task.Name)
	return fmt.Sprintf(`BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE %s PURGE';
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE != -942 THEN
            RAISE;
         END IF;
   END;
   EXECUTE IMMEDIATE 'CREATE TABLE %s AS %s';
END;`, escapedName, escapedName, escapedQuery), nil
}

func buildSCD2ByTimeFullRefresh(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return "", errors.New("materialization strategy 'SCD2_by_time' requires the `primary_key` field to be set on at least one column")
	}

	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))

	createAsQuery := fmt.Sprintf(`SELECT
  src.*,
  CAST(src.%s AS TIMESTAMP) AS bruin_valid_from,
  TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS bruin_valid_until,
  1 AS bruin_is_current
FROM (
%s
) src`, asset.Materialization.IncrementalKey, query)

	escapedQuery := escapeOracleString(createAsQuery)
	escapedName := escapeOracleString(asset.Name)

	return fmt.Sprintf(`BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE %s PURGE';
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE != -942 THEN
            RAISE;
         END IF;
   END;
   EXECUTE IMMEDIATE 'CREATE TABLE %s AS %s';
END;`, escapedName, escapedName, escapedQuery), nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	if err := validateIdentifier(asset.Name, "table name"); err != nil {
		return "", err
	}
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	return fmt.Sprintf("INSERT INTO %s %s", asset.Name, query), nil
}

// buildTruncateInsertQuery wraps TRUNCATE + INSERT in a PL/SQL block.
// TRUNCATE is DDL in Oracle, so it must be executed via EXECUTE IMMEDIATE.
func buildTruncateInsertQuery(task *pipeline.Asset, query string) (string, error) {
	if err := validateIdentifier(task.Name, "table name"); err != nil {
		return "", err
	}
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	escapedName := escapeOracleString(task.Name)
	return fmt.Sprintf(`BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE %s';
   INSERT INTO %s
%s
;
END;`, escapedName, task.Name, query), nil
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) (string, error) {
	if err := validateIdentifier(asset.Name, "table name"); err != nil {
		return "", err
	}
	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for time_interval strategy")
	}
	if err := validateIdentifier(asset.Materialization.IncrementalKey, "incremental_key column"); err != nil {
		return "", err
	}
	if asset.Materialization.TimeGranularity == "" {
		return "", errors.New("time_granularity is required for time_interval strategy")
	}
	if !(asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityTimestamp ||
		asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate) {
		return "", errors.New("time_granularity must be either 'date' or 'timestamp'")
	}

	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))

	startVar := "{{start_timestamp}}"
	endVar := "{{end_timestamp}}"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "{{start_date}}"
		endVar = "{{end_date}}"
	}

	return fmt.Sprintf(`BEGIN
   DELETE FROM %s WHERE %s BETWEEN '%s' AND '%s';
   INSERT INTO %s
%s
;
END;`, asset.Name, asset.Materialization.IncrementalKey, startVar, endVar, asset.Name, query), nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) (string, error) {
	if err := validateIdentifier(task.Name, "table name"); err != nil {
		return "", err
	}
	mat := task.Materialization
	if mat.IncrementalKey == "" {
		return "", fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", mat.Strategy)
	}
	if err := validateIdentifier(mat.IncrementalKey, "incremental_key column"); err != nil {
		return "", err
	}

	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))

	// Wrap DELETE + INSERT in a single PL/SQL block so both DML statements
	// execute atomically through the Go driver in one ExecContext call.
	// Uses EXISTS instead of IN (SELECT DISTINCT ...) to let Oracle's optimizer
	// choose the best semi-join strategy without forcing a full DISTINCT sort.
	return fmt.Sprintf(`BEGIN
   DELETE FROM %s t WHERE EXISTS (
      SELECT 1 FROM (%s) s WHERE s.%s = t.%s
   );
   INSERT INTO %s
%s
;
END;`, task.Name, query, mat.IncrementalKey, mat.IncrementalKey, task.Name, query), nil
}

func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	if err := validateIdentifier(asset.Name, "table name"); err != nil {
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
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))

	onQuery := strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND ")

	insertCols := strings.Join(columnNames, ", ")

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
		whenMatchedThenQuery = "WHEN MATCHED THEN UPDATE SET " + strings.Join(matchedUpdateStatements, ", ")
	}

	mergeLines := []string{
		fmt.Sprintf("MERGE INTO %s target", asset.Name),
		fmt.Sprintf("USING (\n%s\n) source ON (%s)", query, onQuery),
	}
	if whenMatchedThenQuery != "" {
		mergeLines = append(mergeLines, whenMatchedThenQuery)
	}
	mergeLines = append(mergeLines, fmt.Sprintf("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)", insertCols, getSourcePrefix(columnNames)))

	return strings.Join(mergeLines, "\n") + ";", nil
}

func getSourcePrefix(columns []string) string {
	res := make([]string, len(columns))
	for i, c := range columns {
		res[i] = "source." + c
	}
	return strings.Join(res, ", ")
}

// buildPKConditions generates NULL-safe equality comparisons for primary key columns.
// Standard equality (a = b) evaluates to UNKNOWN when either side is NULL, which
// would cause a MERGE to treat the row as NOT MATCHED and insert duplicates.
func buildPKConditions(primaryKeys []string, leftAlias, rightAlias string) []string {
	conditions := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		conditions[i] = fmt.Sprintf(
			"(%[1]s.%[3]s = %[2]s.%[3]s OR (%[1]s.%[3]s IS NULL AND %[2]s.%[3]s IS NULL))",
			leftAlias, rightAlias, pk,
		)
	}
	return conditions
}

func buildPKJoin(leftAlias, rightAlias string, primaryKeys []string) string {
	return strings.Join(buildPKConditions(primaryKeys, leftAlias, rightAlias), " AND ")
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query string) (string, error) {
	if err := validateIdentifier(asset.Name, "table name"); err != nil {
		return "", err
	}
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))

	if asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}

	var (
		primaryKeys         = make([]string, 0, 4)
		joinConds           = make([]string, 0, 5)
		insertCols          = make([]string, 0, 12)
		insertValues        = make([]string, 0, 12)
		incrementalKeyFound bool
	)
	for _, col := range asset.Columns {
		switch col.Name {
		case "bruin_valid_from", "bruin_valid_until", "bruin_is_current":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", col.Name)
		}
		if col.Name == asset.Materialization.IncrementalKey {
			incrementalKeyFound = true
			lcType := strings.ToLower(col.Type)
			if strings.Contains(lcType, "time zone") {
				return "", errors.New("TIMESTAMP WITH TIME ZONE is not supported for SCD2_by_time incremental_key; use TIMESTAMP or DATE instead")
			}
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

	if !incrementalKeyFound {
		return "", fmt.Errorf("incremental_key '%s' not found in column definitions", asset.Materialization.IncrementalKey)
	}

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf(
			"materialization strategy %s requires the primary_key field to be set on at least one column",
			asset.Materialization.Strategy,
		)
	}

	insertCols = append(insertCols, "bruin_valid_from", "bruin_valid_until", "bruin_is_current")
	insertValues = append(insertValues,
		"CAST(source."+asset.Materialization.IncrementalKey+" AS TIMESTAMP)",
		"TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')",
		"1",
	)

	for _, pk := range primaryKeys {
		joinConds = append(joinConds, fmt.Sprintf(
			"(target.%[1]s = source.%[1]s OR (target.%[1]s IS NULL AND source.%[1]s IS NULL))", pk,
		))
	}
	joinConds = append(joinConds, "source.bruin_is_current_src = 1")
	onCondition := strings.Join(joinConds, " AND ")
	tbl := asset.Name
	pkJoinUpdateStr := strings.Join(buildPKConditions(primaryKeys, "target", "source"), " AND ")

	// Wrap the entire SCD2 logic in a PL/SQL block so both UPDATE and MERGE
	// execute atomically through the Go driver.
	// The EXISTS guard on the UPDATE prevents an empty source batch from
	// marking all existing current rows as expired.
	queryStr := fmt.Sprintf(`BEGIN
UPDATE %s target
SET bruin_valid_until = LOCALTIMESTAMP, bruin_is_current = 0
WHERE target.bruin_is_current = 1
  AND NOT EXISTS (
    SELECT 1 FROM (%s) source
    WHERE %s
  )
  AND EXISTS (SELECT 1 FROM (%s) source_exists);

MERGE INTO (SELECT * FROM %s WHERE bruin_is_current = 1) target
USING (
  WITH s1 AS (
    %s
  )
  SELECT s1.*, 1 AS bruin_is_current_src
  FROM s1
  UNION ALL
  SELECT s1.*, 0 AS bruin_is_current_src
  FROM s1
  JOIN %s t1 ON (%s)
  WHERE t1.bruin_valid_from < CAST(s1.%s AS TIMESTAMP) AND t1.bruin_is_current = 1
) source
ON (%s)
WHEN MATCHED THEN
  UPDATE SET
    target.bruin_valid_until = CAST(source.%s AS TIMESTAMP),
    target.bruin_is_current  = 0
  WHERE target.bruin_valid_from < CAST(source.%s AS TIMESTAMP)
WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s);
END;`,
		tbl,
		query,
		pkJoinUpdateStr,
		query,
		tbl,
		query,
		tbl,
		buildPKJoin("t1", "s1", primaryKeys),
		asset.Materialization.IncrementalKey,
		onCondition,
		asset.Materialization.IncrementalKey,
		asset.Materialization.IncrementalKey,
		strings.Join(insertCols, ", "),
		strings.Join(insertValues, ", "),
	)

	return strings.TrimSpace(queryStr), nil
}
