package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type (
	MaterializerFunc        func(task *pipeline.Asset, query string) ([]string, error)
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
		pipeline.MaterializationStrategySCD2ByTime:     buildSCD2ByTimeQuery,
		pipeline.MaterializationStrategySCD2ByColumn:   buildSCD2ByColumnQuery,
	},
}

func errorMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return nil, fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", asset.Name, query)}, nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return []string{fmt.Sprintf("INSERT INTO %s %s", asset.Name, query)}, nil
}

func buildIncrementalQuery(task *pipeline.Asset, query string) ([]string, error) {
	mat := task.Materialization
	strategy := pipeline.MaterializationStrategyDeleteInsert

	if mat.IncrementalKey == "" {
		return nil, fmt.Errorf("materialization strategy %s requires the `incremental_key` field to be set", strategy)
	}

	if len(task.Columns) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", strategy)
	}

	primaryKeys := task.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) != 1 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at EXACTLY one column", strategy)
	}

	// Extract database name from task.Name to ensure temp table is created in the correct database
	assetNameParts := strings.Split(task.Name, ".")
	var tempTableName string
	if len(assetNameParts) == 2 {
		databaseName := assetNameParts[0]
		tempTableName = databaseName + ".__bruin_tmp_" + helpers.PrefixGenerator()
	} else {
		// Fallback for tables without explicit database
		tempTableName = "__bruin_tmp_" + helpers.PrefixGenerator()
	}

	queries := []string{
		fmt.Sprintf(
			"CREATE TABLE %s ENGINE = MergeTree() PRIMARY KEY %s AS %s",
			tempTableName,
			task.ColumnNamesWithPrimaryKey()[0],
			query,
		),
		fmt.Sprintf("DELETE FROM %s WHERE %s in (SELECT DISTINCT %s FROM %s)", task.Name, mat.IncrementalKey, mat.IncrementalKey, tempTableName),
		// An identical rerun must replace rows deleted above instead of being
		// discarded by ClickHouse's block-level insert deduplication.
		fmt.Sprintf("INSERT INTO %s SETTINGS insert_deduplicate = 0 SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
	}

	return queries, nil
}

// buildMergeQuery implements the `merge` materialization strategy for ClickHouse.
//
// ClickHouse has no `MERGE INTO` statement, so the upsert is performed with a
// delete+insert pattern keyed on the asset's primary key column(s): the query
// result is staged in a temporary table, rows in the target whose primary key
// appears in the staged rows are deleted, and the staged rows are then
// inserted. This preserves the semantics of `merge` (existing rows are
// replaced, new rows are added, untouched rows remain) while staying within
// ClickHouse's SQL surface: a lightweight DELETE cannot reference other tables
// in its WHERE clause, so the match is expressed as an IN subquery against the
// temp table. The INSERT disables insert deduplication because ClickHouse's
// block-level dedup would otherwise silently drop the insert on a rerun after
// the matching rows were deleted (see issue #2396). The optional
// `incremental_predicate` is appended to the delete condition to scope which
// target rows are considered for replacement; it must reference the target
// table's columns unqualified, since ClickHouse DELETE has no table aliases.
func buildMergeQuery(task *pipeline.Asset, query string) ([]string, error) {
	if len(task.Columns) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", task.Materialization.Strategy)
	}

	primaryKeys := task.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", task.Materialization.Strategy)
	}

	// Extract database name from task.Name to ensure temp table is created in the correct database
	assetNameParts := strings.Split(task.Name, ".")
	var tempTableName string
	if len(assetNameParts) == 2 {
		databaseName := assetNameParts[0]
		tempTableName = databaseName + ".__bruin_tmp_" + helpers.PrefixGenerator()
	} else {
		// Fallback for tables without explicit database
		tempTableName = "__bruin_tmp_" + helpers.PrefixGenerator()
	}

	keys := strings.Join(primaryKeys, ", ")
	deleteCondition := keys
	if len(primaryKeys) > 1 {
		deleteCondition = "(" + deleteCondition + ")"
	}
	deleteCondition += fmt.Sprintf(" IN (SELECT %s FROM %s)", keys, tempTableName)

	if pred := strings.TrimSpace(task.Materialization.IncrementalPredicate); pred != "" {
		deleteCondition += " AND (" + pred + ")"
	}

	// Validate that ClickHouse can plan an insert from the staged schema before
	// deleting target rows. LIMIT 0 performs no write and avoids deduplication.
	queries := []string{
		fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree() PRIMARY KEY (%s) AS %s", tempTableName, keys, query),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s LIMIT 0", task.Name, tempTableName),
		fmt.Sprintf("DELETE FROM %s WHERE %s", task.Name, deleteCondition),
		fmt.Sprintf("INSERT INTO %s SETTINGS insert_deduplicate = 0 SELECT * FROM %s", task.Name, tempTableName),
		"DROP TABLE IF EXISTS " + tempTableName,
	}

	return queries, nil
}

func buildTruncateInsertQuery(task *pipeline.Asset, query string) ([]string, error) {
	// ClickHouse doesn't support transactions, so we return individual statements
	queries := []string{
		"TRUNCATE TABLE " + task.Name,
		fmt.Sprintf("INSERT INTO %s %s", task.Name, strings.TrimSuffix(query, ";")),
	}
	return queries, nil
}

func buildCreateReplaceQuery(task *pipeline.Asset, query string) ([]string, error) {
	clauses, err := createReplaceTableClauses(task)
	if err != nil {
		return nil, err
	}

	query = strings.TrimSuffix(query, ";")

	return []string{
		fmt.Sprintf(
			"CREATE OR REPLACE TABLE %s %s AS %s",
			task.Name,
			strings.Join(clauses, " "),
			query,
		),
	}, nil
}

const (
	clickHouseSCD2Now = "now64(6, 'UTC')"
	// This sentinel fits DateTime64 on ClickHouse versions predating the
	// extended date range and keeps validity columns at microsecond precision.
	clickHouseSCD2Max = "toDateTime64('2299-12-31 23:59:59', 6, 'UTC')"
)

func clickHouseIncrementalKeyType(asset *pipeline.Asset) (string, error) {
	key := asset.Materialization.IncrementalKey
	if key == "" {
		return "", nil
	}
	column := asset.GetColumnWithName(key)
	if column == nil {
		return "", fmt.Errorf("incremental_key column %s not found", key)
	}
	columnType := strings.ToLower(strings.TrimSpace(column.Type))
	if strings.HasPrefix(columnType, "nullable(") && strings.HasSuffix(columnType, ")") {
		columnType = strings.TrimSpace(columnType[len("nullable(") : len(columnType)-1])
	}
	if strings.Contains(columnType, "timestamp") || columnType == "date" || columnType == "date32" ||
		columnType == "datetime" || strings.HasPrefix(columnType, "datetime(") ||
		columnType == "datetime64" || strings.HasPrefix(columnType, "datetime64(") {
		return column.Type, nil
	}
	return "", fmt.Errorf("incremental_key must be TIMESTAMP or DATE (DateTime, DateTime64, Date or Date32) in %s strategy", asset.Materialization.Strategy)
}

// scd2SourceQuery gives first runs, incremental runs and full refreshes the
// same validity columns and validates the declared source schema.
func scd2SourceQuery(asset *pipeline.Asset, query string) (string, error) {
	if asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime && asset.Materialization.IncrementalKey == "" {
		return "", errors.New("incremental_key is required for SCD2_by_time strategy")
	}
	if _, err := clickHouseIncrementalKeyType(asset); err != nil {
		return "", err
	}
	columns := make([]string, 0, len(asset.Columns)+3)
	for _, column := range asset.Columns {
		switch strings.ToLower(unquoteKey(column.Name)) {
		case "_valid_from", "_valid_until", "_is_current":
			return "", fmt.Errorf("column name %s is reserved for SCD-2 and cannot be used", column.Name)
		}
		columns = append(columns, "src."+column.Name)
	}
	if len(asset.ColumnNamesWithPrimaryKey()) == 0 {
		return "", fmt.Errorf("materialization strategy %s requires the primary_key field to be set on at least one column", asset.Materialization.Strategy)
	}
	validFrom := clickHouseSCD2Now
	if asset.Materialization.IncrementalKey != "" {
		validFrom = fmt.Sprintf("toDateTime64(src.%s, 6, 'UTC')", asset.Materialization.IncrementalKey)
	}
	columns = append(columns, validFrom+" AS _valid_from", clickHouseSCD2Max+" AS _valid_until", "TRUE AS _is_current")
	return "SELECT " + strings.Join(columns, ", ") + "\nFROM (" + strings.TrimSuffix(strings.TrimSpace(query), ";") + "\n) AS src", nil
}

func buildSCD2ByTimeQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return buildSCD2Query(asset, query)
}

func buildSCD2ByColumnQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return buildSCD2Query(asset, query)
}

func buildSCD2ByTimeFullRefreshQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return buildSCD2FullRefreshQuery(asset, query, "")
}

func buildSCD2ByColumnFullRefreshQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return buildSCD2FullRefreshQuery(asset, query, "")
}

func buildSCD2FullRefreshQuery(asset *pipeline.Asset, query, cluster string) ([]string, error) {
	source, err := scd2SourceQuery(asset, query)
	if err != nil {
		return nil, err
	}
	if cluster != "" {
		return buildClusterQuery(asset, source, pipeline.MaterializationStrategyCreateReplace, cluster, buildCreateReplaceQuery)
	}
	return buildCreateReplaceQuery(asset, source)
}

func buildSCD2Query(asset *pipeline.Asset, query string) ([]string, error) {
	source, err := scd2SourceQuery(asset, query)
	if err != nil {
		return nil, err
	}
	clauses, err := tableDefinitionClauses(asset)
	if err != nil {
		return nil, err
	}
	stage := "__bruin_tmp_" + helpers.PrefixGenerator()
	if database, _, qualified := strings.Cut(asset.Name, "."); qualified {
		stage = database + "." + stage
	}
	sourceTable, changesTable := stage+"_source", stage+"_changes"
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	keys := strings.Join(primaryKeys, ", ")
	deleteKeys := keys
	if len(primaryKeys) > 1 {
		deleteKeys = "(" + keys + ")"
	}
	joinConditions := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		joinConditions = append(joinConditions, "t."+key+" = s."+key)
	}
	joinCondition := strings.Join(joinConditions, " AND ")
	columns := make([]string, 0, len(asset.Columns)+3)
	targetColumns := make([]string, 0, len(asset.Columns)+3)
	sourceColumns := make([]string, 0, len(asset.Columns)+3)
	changeConditions := make([]string, 0, len(asset.Columns))
	for _, column := range asset.Columns {
		columns = append(columns, column.Name)
		targetColumns = append(targetColumns, "t."+column.Name)
		sourceColumns = append(sourceColumns, "s."+column.Name)
		if !column.PrimaryKey {
			changeConditions = append(changeConditions, fmt.Sprintf("(ifNull(t.%[1]s != s.%[1]s, FALSE) OR isNull(t.%[1]s) != isNull(s.%[1]s))", column.Name))
		}
	}
	changeCondition := "FALSE"
	if asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByTime {
		changeCondition = "s._valid_from > t._valid_from"
	} else if len(changeConditions) > 0 {
		changeCondition = strings.Join(changeConditions, " OR ")
	}
	columns = append(columns, "_valid_from", "_valid_until", "_is_current")
	allColumns := strings.Join(columns, ", ")
	// The current flag doubles as a join match marker: source rows are always
	// current, and unmatched rows are false or NULL depending on join_use_nulls.
	sourceMissing := "ifNull(s._is_current, FALSE) = FALSE"
	targetMissing := "ifNull(t._is_current, FALSE) = FALSE"
	targetColumns = append(targetColumns, "t._valid_from", "if("+sourceMissing+", "+clickHouseSCD2Now+", s._valid_from) AS _valid_until", "FALSE AS _is_current")
	sourceColumns = append(sourceColumns, "s._valid_from", "s._valid_until", "s._is_current")
	currentTarget := "(SELECT " + allColumns + " FROM " + asset.Name + " WHERE _is_current = TRUE)"
	changes := "SELECT " + strings.Join(targetColumns, ", ") + "\nFROM " + currentTarget + " AS t\n" +
		"LEFT JOIN " + sourceTable + " AS s ON " + joinCondition + "\n" +
		"WHERE " + sourceMissing + " OR (" + changeCondition + ")\nUNION ALL\n" +
		"SELECT " + strings.Join(sourceColumns, ", ") + "\nFROM " + sourceTable + " AS s\n" +
		"LEFT JOIN " + currentTarget + " AS t ON " + joinCondition + "\n" +
		"WHERE " + targetMissing + " OR (" + changeCondition + ")"
	// Materialize both sides of each replacement before deleting anything.
	// Already expired versions are never selected or rewritten.
	return []string{
		fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree() PRIMARY KEY (%s) AS %s", sourceTable, keys, source),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s EMPTY AS SELECT * FROM %s", asset.Name, strings.Join(clauses, " "), sourceTable),
		fmt.Sprintf("CREATE TABLE %s ENGINE = MergeTree() PRIMARY KEY (%s) AS %s", changesTable, keys, changes),
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s LIMIT 0", asset.Name, allColumns, allColumns, changesTable),
		fmt.Sprintf("DELETE FROM %s WHERE _is_current = TRUE AND %s IN (SELECT %s FROM %s)", asset.Name, deleteKeys, keys, changesTable),
		// A rerun after DELETE must not lose replacement rows to block deduplication.
		fmt.Sprintf("INSERT INTO %s (%s) SETTINGS insert_deduplicate = 0 SELECT %s FROM %s", asset.Name, allColumns, allColumns, changesTable),
		"DROP TABLE IF EXISTS " + changesTable,
		"DROP TABLE IF EXISTS " + sourceTable,
	}, nil
}

func createReplaceTableClauses(task *pipeline.Asset) ([]string, error) {
	if len(task.Columns) == 0 && len(task.ClickHouse.OrderBy) == 0 && task.ClickHouse.Engine == "" {
		return nil, fmt.Errorf("materialization strategy %s requires the `columns` field to be set", task.Materialization.Strategy)
	}
	primaryKeys := task.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) == 0 && len(task.ClickHouse.OrderBy) == 0 && task.ClickHouse.Engine == "" {
		return nil, fmt.Errorf("materialization strategy %s requires the `primary_key` field to be set on at least one column", task.Materialization.Strategy)
	}
	return tableDefinitionClauses(task)
}

// tableDefinitionClauses is shared by the strategies that create the target
// table. Temporary staging tables use their own MergeTree definition.
func tableDefinitionClauses(asset *pipeline.Asset) ([]string, error) {
	options := asset.ClickHouse
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(options.OrderBy) > 0 {
		for i, key := range primaryKeys {
			if i >= len(options.OrderBy) || unquoteKey(key) != unquoteKey(options.OrderBy[i]) {
				return nil, errors.New("ClickHouse primary key columns must be a prefix of clickhouse.order_by")
			}
		}
	}

	var clauses []string
	if options.Engine != "" {
		clauses = append(clauses, "ENGINE = "+options.Engine)
	}
	if asset.Materialization.PartitionBy != "" {
		clauses = append(clauses, "PARTITION BY ("+asset.Materialization.PartitionBy+")")
	}
	if len(primaryKeys) > 0 {
		clauses = append(clauses, "PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	if len(options.OrderBy) > 0 {
		clauses = append(clauses, "ORDER BY ("+strings.Join(options.OrderBy, ", ")+")")
	}
	if options.TTL != "" {
		clauses = append(clauses, "TTL "+options.TTL)
	}
	if len(options.Settings) > 0 {
		keys := make([]string, 0, len(options.Settings))
		for key := range options.Settings {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		settings := make([]string, 0, len(keys))
		for _, key := range keys {
			settings = append(settings, key+" = "+options.Settings[key])
		}
		clauses = append(clauses, "SETTINGS "+strings.Join(settings, ", "))
	}
	return clauses, nil
}

func unquoteKey(key string) string {
	key = strings.TrimSpace(key)
	if len(key) >= 2 && (key[0] == '`' || key[0] == '"') && key[len(key)-1] == key[0] {
		return key[1 : len(key)-1]
	}
	return key
}

func buildTimeIntervalQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return buildTimeIntervalQueryForCluster(asset, query, "")
}

func buildTimeIntervalQueryForCluster(asset *pipeline.Asset, query, clusterClause string) ([]string, error) {
	if asset.Materialization.IncrementalKey == "" {
		return nil, errors.New("incremental_key is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity == "" {
		return nil, errors.New("time_granularity is required for time_interval strategy")
	}

	if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp && asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate {
		return nil, errors.New("time_granularity must be either 'date', or 'timestamp'")
	}

	startVar := "toDateTime64('{{ start_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6)"
	endVar := "toDateTime64('{{ end_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6)"
	if asset.Materialization.TimeGranularity == pipeline.MaterializationTimeGranularityDate {
		startVar = "'{{start_date}}'"
		endVar = "'{{end_date}}'"
	}

	predicate := fmt.Sprintf("%s BETWEEN %s AND %s", asset.Materialization.IncrementalKey, startVar, endVar)
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s", asset.Name, predicate)
	if clusterClause != "" {
		deleteQuery = fmt.Sprintf("ALTER TABLE %s%s DELETE WHERE %s SETTINGS mutations_sync = 2", asset.Name, clusterClause, predicate)
	}
	queries := []string{
		deleteQuery,
		fmt.Sprintf(`INSERT INTO %s SETTINGS insert_deduplicate = 0 %s`,
			asset.Name, query),
	}

	return queries, nil
}

func buildDDLQuery(asset *pipeline.Asset, query string) ([]string, error) {
	return buildDDLQueryForCluster(asset, "")
}

func buildDDLQueryForCluster(asset *pipeline.Asset, clusterClause string) ([]string, error) {
	columnDefs := make([]string, 0, len(asset.Columns))

	for _, col := range asset.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.SQLType())

		if col.Default != "" {
			def += " DEFAULT " + col.Default
		}
		if col.Description != "" {
			def += fmt.Sprintf(" COMMENT '%s'", col.Description)
		}
		columnDefs = append(columnDefs, def)
	}

	clauses, err := tableDefinitionClauses(asset)
	if err != nil {
		return nil, err
	}

	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s%s (\n"+
			"%s\n"+
			")",
		asset.Name,
		clusterClause,
		strings.Join(columnDefs, ",\n"),
	)
	if len(clauses) > 0 {
		ddl += "\n" + strings.Join(clauses, "\n")
	}

	return []string{ddl}, nil
}

func buildClusterQuery(asset *pipeline.Asset, query string, strategy pipeline.MaterializationStrategy, cluster string, fallback MaterializerFunc) ([]string, error) {
	clusterClause := " ON CLUSTER `" + strings.NewReplacer("\\", "\\\\", "`", "\\`").Replace(cluster) + "`"
	if asset.Materialization.Type == pipeline.MaterializationTypeView && strategy == pipeline.MaterializationStrategyNone {
		return []string{fmt.Sprintf("CREATE OR REPLACE VIEW %s%s AS\n%s", asset.Name, clusterClause, query)}, nil
	}
	if asset.Materialization.Type != pipeline.MaterializationTypeTable {
		return fallback(asset, query)
	}

	switch strategy {
	case pipeline.MaterializationStrategyDeleteInsert, pipeline.MaterializationStrategyMerge,
		pipeline.MaterializationStrategySCD2ByTime, pipeline.MaterializationStrategySCD2ByColumn:
		return nil, fmt.Errorf("ClickHouse materialization strategy %s is not supported with cluster: staging data cannot be shared safely across replicas; use append, time_interval, or a full refresh", strategy)
	case pipeline.MaterializationStrategyNone, pipeline.MaterializationStrategyCreateReplace:
		if !isReplicatedMergeTree(asset.ClickHouse.Engine) {
			return nil, errors.New("ClickHouse cluster table replacement requires an explicit Replicated*MergeTree engine in clickhouse.engine, including Keeper arguments or configured server defaults")
		}
		if len(asset.ColumnNamesWithPrimaryKey()) == 0 && len(asset.ClickHouse.OrderBy) == 0 {
			return nil, errors.New("ClickHouse cluster table replacement requires primary_key columns or clickhouse.order_by for the replicated engine")
		}
		clauses, err := createReplaceTableClauses(asset)
		if err != nil {
			return nil, err
		}
		// SYNC releases the old replica's Keeper path before it is reused.
		// EMPTY keeps the SELECT from inserting once per cluster member.
		return []string{
			fmt.Sprintf("DROP TABLE IF EXISTS %s%s SYNC", asset.Name, clusterClause),
			fmt.Sprintf("CREATE TABLE %s%s %s EMPTY AS %s", asset.Name, clusterClause, strings.Join(clauses, " "), query),
			fmt.Sprintf("INSERT INTO %s %s", asset.Name, query),
		}, nil
	case pipeline.MaterializationStrategyDDL:
		if strings.TrimSpace(asset.ClickHouse.Engine) == "" {
			return nil, errors.New("ClickHouse cluster ddl requires an explicit clickhouse.engine")
		}
		return buildDDLQueryForCluster(asset, clusterClause)
	case pipeline.MaterializationStrategyTruncateInsert, pipeline.MaterializationStrategyTimeInterval:
		if asset.ClickHouse.Engine != "" && !isReplicatedMergeTree(asset.ClickHouse.Engine) {
			return nil, fmt.Errorf("ClickHouse cluster %s requires a local Replicated*MergeTree engine on a single shard; Distributed and non-replicated targets are not supported", strategy)
		}
		if strategy == pipeline.MaterializationStrategyTimeInterval {
			return buildTimeIntervalQueryForCluster(asset, query, clusterClause)
		}
		return []string{
			fmt.Sprintf("TRUNCATE TABLE %s%s SYNC", asset.Name, clusterClause),
			fmt.Sprintf("INSERT INTO %s %s", asset.Name, query),
		}, nil
	default:
		return fallback(asset, query)
	}
}

func isReplicatedMergeTree(engine string) bool {
	name, _, _ := strings.Cut(strings.TrimSpace(engine), "(")
	name = strings.TrimSpace(name)
	return strings.HasPrefix(name, "Replicated") && strings.HasSuffix(name, "MergeTree")
}
