package clickhouse

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMaterializer_ClickHouseServer uses a disposable database on the native
// ClickHouse endpoint configured in BRUIN_CLICKHOUSE_TEST_ADDR (host:port).
// Optional credentials use BRUIN_CLICKHOUSE_TEST_USER and
// BRUIN_CLICKHOUSE_TEST_PASSWORD. The user must be able to create databases.
func TestMaterializer_ClickHouseServer(t *testing.T) {
	t.Parallel()
	address := os.Getenv("BRUIN_CLICKHOUSE_TEST_ADDR")
	if address == "" {
		t.Skip("set BRUIN_CLICKHOUSE_TEST_ADDR to run ClickHouse server integration tests")
	}
	username := os.Getenv("BRUIN_CLICKHOUSE_TEST_USER")
	if username == "" {
		username = "default"
	}
	conn, err := click_house.Open(&click_house.Options{
		Addr: []string{address},
		Auth: click_house.Auth{
			Username: username,
			Password: os.Getenv("BRUIN_CLICKHOUSE_TEST_PASSWORD"),
		},
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		MaxOpenConns: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	require.NoError(t, conn.Ping(t.Context()))
	var version string
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT version()").Scan(&version))
	t.Logf("ClickHouse server version: %s", version)

	database := fmt.Sprintf("bruin_materialization_test_%d", time.Now().UnixNano())
	require.NoError(t, conn.Exec(t.Context(), "CREATE DATABASE "+database))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		assert.NoError(t, conn.Exec(ctx, "DROP DATABASE IF EXISTS "+database+" SYNC"))
	})

	const query = "SELECT toUInt64(1) AS id, number + 1 AS version, " +
		"toDateTime('2100-01-01 00:00:00') AS event_time, " +
		"toUInt64(10 * (number + 1)) AS amount FROM numbers(2)"
	columns := func(primaryKey bool) []pipeline.Column {
		return []pipeline.Column{
			{Name: "id", Type: "UInt64", PrimaryKey: primaryKey},
			{Name: "version", Type: "UInt64"},
			{Name: "event_time", Type: "DateTime"},
			{Name: "amount", Type: "UInt64"},
		}
	}
	options := pipeline.ClickHouseConfig{
		Engine:  "ReplacingMergeTree(version)",
		OrderBy: []string{"id", "event_time"},
		TTL:     "event_time + INTERVAL 1 DAY",
		Settings: map[string]string{
			"index_granularity":       "4096",
			"min_bytes_for_wide_part": "0",
			"storage_policy":          "'default'",
		},
	}
	indexAndProjectionOptions := pipeline.ClickHouseConfig{
		Engine:     "MergeTree",
		OrderBy:    []string{"id", "event_time"},
		PrimaryKey: []string{"id"},
		Indexes: []pipeline.ClickHouseIndex{
			{Name: "idx_id", Expression: "id", Type: "minmax"},
			{Name: "idx_amount", Expression: "amount + version", Type: "minmax", Granularity: 2},
		},
		Projections: []pipeline.ClickHouseProjection{
			{Name: "p_by_time", Select: "SELECT event_time, count() GROUP BY event_time"},
		},
	}
	cases := []struct {
		name          string
		options       pipeline.ClickHouseConfig
		partitionBy   string
		primaryKey    bool
		inferColumns  bool
		engine        string
		sortingKey    string
		primaryKeySQL string
		finalAmount   uint64
	}{
		{
			name: "primary_key_only", primaryKey: true,
			engine: "MergeTree", sortingKey: "id", primaryKeySQL: "id",
		},
		{
			name: "partition_only", partitionBy: "toYYYYMM(event_time)", primaryKey: true,
			engine: "MergeTree", sortingKey: "id", primaryKeySQL: "id",
		},
		{
			name: "all_options", options: options, partitionBy: "toYYYYMM(event_time)", primaryKey: true,
			engine: "ReplacingMergeTree", sortingKey: "id, event_time", primaryKeySQL: "id", finalAmount: 20,
		},
		{
			name:    "summing_without_primary_key",
			options: pipeline.ClickHouseConfig{Engine: "SummingMergeTree()", OrderBy: []string{"id", "event_time"}},
			engine:  "SummingMergeTree", sortingKey: "id, event_time", primaryKeySQL: "id, event_time", finalAmount: 30,
		},
		{
			name:    "order_by_without_primary_key",
			options: pipeline.ClickHouseConfig{OrderBy: []string{"id", "event_time"}},
			engine:  "MergeTree", sortingKey: "id, event_time", primaryKeySQL: "id, event_time",
		},
		{
			name: "memory_without_keys", options: pipeline.ClickHouseConfig{Engine: "Memory()"},
			engine: "Memory",
		},
		{
			name: "independent_primary_key", primaryKey: true,
			options: pipeline.ClickHouseConfig{
				OrderBy: []string{"event_time", "id"}, PrimaryKey: []string{"event_time"},
			},
			engine: "MergeTree", sortingKey: "event_time, id", primaryKeySQL: "event_time",
		},
		{
			name: "indexes_and_projections", options: indexAndProjectionOptions, primaryKey: true,
			engine: "MergeTree", sortingKey: "id, event_time", primaryKeySQL: "id",
		},
		{
			name: "indexes_and_projections_inferred_columns", options: indexAndProjectionOptions, inferColumns: true,
			engine: "MergeTree", sortingKey: "id, event_time", primaryKeySQL: "id",
		},
	}
	for _, strategy := range []struct {
		name        string
		strategy    pipeline.MaterializationStrategy
		fullRefresh bool
	}{
		{name: "none", strategy: pipeline.MaterializationStrategyNone},
		{name: "create_replace", strategy: pipeline.MaterializationStrategyCreateReplace},
		{name: "ddl", strategy: pipeline.MaterializationStrategyDDL},
		{name: "merge_full_refresh", strategy: pipeline.MaterializationStrategyMerge, fullRefresh: true},
	} {
		for _, tc := range cases {
			if tc.inferColumns && strategy.strategy == pipeline.MaterializationStrategyDDL {
				continue
			}
			t.Run(strategy.name+"/"+tc.name, func(t *testing.T) {
				table := strategy.name + "_" + tc.name
				asset := &pipeline.Asset{
					Name: database + "." + table,
					Type: pipeline.AssetTypeClickHouse,
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable, Strategy: strategy.strategy, PartitionBy: tc.partitionBy,
					},
					Columns: columns(tc.primaryKey), ClickHouse: tc.options,
				}
				if tc.inferColumns {
					asset.Columns = nil
				}
				queries, renderErr := NewMaterializer(strategy.fullRefresh).Render(asset, query)
				require.NoError(t, renderErr)
				for range 2 {
					for _, statement := range queries {
						require.NoError(t, conn.Exec(t.Context(), statement), statement)
					}
				}
				if strategy.strategy == pipeline.MaterializationStrategyDDL {
					require.NoError(t, conn.Exec(t.Context(), "INSERT INTO "+asset.Name+" "+query))
				}

				var engine, partitionKey, sortingKey, primaryKey, createSQL string
				require.NoError(t, conn.QueryRow(
					t.Context(),
					"SELECT engine, partition_key, sorting_key, primary_key, create_table_query FROM system.tables WHERE database = ? AND name = ?",
					database, table,
				).Scan(&engine, &partitionKey, &sortingKey, &primaryKey, &createSQL))
				assert.Equal(t, tc.engine, engine)
				assert.Equal(t, tc.partitionBy, partitionKey)
				assert.Equal(t, tc.sortingKey, sortingKey)
				assert.Equal(t, tc.primaryKeySQL, primaryKey)
				if tc.options.TTL != "" {
					assert.Contains(t, createSQL, "TTL event_time + toIntervalDay(1)")
				}
				for name, value := range tc.options.Settings {
					assert.Contains(t, createSQL, name+" = "+value)
				}
				if tc.options.Engine == "ReplacingMergeTree(version)" {
					assert.Contains(t, createSQL, "ENGINE = ReplacingMergeTree(version)")
				}
				for _, index := range tc.options.Indexes {
					var expression, indexType string
					var granularity uint64
					require.NoError(t, conn.QueryRow(
						t.Context(),
						"SELECT expr, type, granularity FROM system.data_skipping_indices WHERE database = ? AND table = ? AND name = ?",
						database, table, index.Name,
					).Scan(&expression, &indexType, &granularity))
					assert.Equal(t, index.Expression, expression)
					assert.Equal(t, index.Type, indexType)
					if index.Granularity > 0 {
						assert.Equal(t, uint64(index.Granularity), granularity)
					} else {
						assert.Equal(t, uint64(1), granularity)
					}
				}
				for _, projection := range tc.options.Projections {
					var projectionCount uint64
					require.NoError(t, conn.QueryRow(
						t.Context(),
						"SELECT count() FROM system.projection_parts WHERE database = ? AND table = ? AND name = ? AND active",
						database, table, projection.Name,
					).Scan(&projectionCount))
					assert.Positive(t, projectionCount, "projection must be populated for inserted rows")
				}
				var amount uint64
				if tc.finalAmount != 0 {
					require.NoError(t, conn.QueryRow(t.Context(), "SELECT sum(amount) FROM "+asset.Name+" FINAL").Scan(&amount))
					assert.Equal(t, tc.finalAmount, amount)
				} else {
					require.NoError(t, conn.QueryRow(t.Context(), "SELECT sum(amount) FROM "+asset.Name).Scan(&amount))
					assert.Equal(t, uint64(30), amount)
				}
			})
		}
	}

	//nolint:paralleltest // Staging subtests share a temporary table name.
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyMerge,
	} {
		t.Run(string(strategy)+"/staging", func(t *testing.T) {
			stagingOptions := options
			stagingOptions.Indexes = indexAndProjectionOptions.Indexes
			stagingOptions.Projections = indexAndProjectionOptions.Projections
			asset := &pipeline.Asset{
				Name: database + ".staging_target",
				Type: pipeline.AssetTypeClickHouse,
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable, Strategy: strategy,
					IncrementalKey: "id", PartitionBy: "toYYYYMM(event_time)",
				},
				Columns: columns(true), ClickHouse: stagingOptions,
			}
			queries, cleanup, renderErr := NewMaterializer(false).RenderWithCleanup(asset, query)
			require.NoError(t, renderErr)
			require.NotEmpty(t, queries)
			require.NoError(t, conn.Exec(t.Context(), queries[0]), queries[0])
			var engine, partitionKey, createSQL string
			require.NoError(t, conn.QueryRow(
				t.Context(),
				"SELECT engine, partition_key, create_table_query FROM system.tables WHERE database = ? AND startsWith(name, '__bruin_tmp_')",
				database,
			).Scan(&engine, &partitionKey, &createSQL))
			assert.Equal(t, "MergeTree", engine)
			assert.Empty(t, partitionKey)
			assert.NotContains(t, createSQL, "TTL")
			assert.NotContains(t, createSQL, "index_granularity = 4096")
			assert.NotContains(t, createSQL, "INDEX idx_id")
			assert.NotContains(t, createSQL, "PROJECTION p_by_time")
			for _, statement := range cleanup {
				require.NoError(t, conn.Exec(t.Context(), statement))
			}
		})
	}

	t.Run("create_replace/failed_insert", func(t *testing.T) {
		asset := &pipeline.Asset{
			Name: database + ".replacement_failure",
			Type: pipeline.AssetTypeClickHouse,
			Materialization: pipeline.Materialization{
				Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyCreateReplace,
			},
			ClickHouse: pipeline.ClickHouseConfig{
				OrderBy: []string{"id"},
				Indexes: []pipeline.ClickHouseIndex{{Name: "idx_id", Expression: "id", Type: "minmax"}},
			},
		}
		require.NoError(t, conn.Exec(t.Context(), "CREATE TABLE "+asset.Name+" ENGINE=MergeTree ORDER BY id AS SELECT toUInt64(99) AS id, toUInt8(0) AS amount"))
		queries, cleanup, renderErr := NewMaterializer(false).RenderWithCleanup(
			asset, "SELECT number AS id, throwIf(number = 0, 'replacement insert failed') AS amount FROM numbers(1)",
		)
		require.NoError(t, renderErr)
		require.NotEmpty(t, cleanup)
		var insertErr error
		for _, statement := range queries {
			if err := conn.Exec(t.Context(), statement); err != nil {
				require.True(t, strings.HasPrefix(statement, "INSERT INTO "), statement)
				insertErr = err
				break
			}
		}
		require.ErrorContains(t, insertErr, "replacement insert failed")
		for _, statement := range cleanup {
			require.NoError(t, conn.Exec(t.Context(), statement))
		}
		var id, stagingTables uint64
		require.NoError(t, conn.QueryRow(t.Context(), "SELECT id FROM "+asset.Name).Scan(&id))
		assert.Equal(t, uint64(99), id, "failed replacement must preserve the target")
		require.NoError(t, conn.QueryRow(
			t.Context(), "SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__bruin_tmp_')", database,
		).Scan(&stagingTables))
		assert.Zero(t, stagingTables)
	})

	testClickHouseSCD2History(t, conn, database)
}

func testClickHouseSCD2History(t *testing.T, conn driver.Conn, database string) {
	t.Helper()

	var projectionMutationSettings uint64
	require.NoError(t, conn.QueryRow(
		t.Context(), "SELECT count() FROM system.merge_tree_settings WHERE name = 'lightweight_mutation_projection_mode'",
	).Scan(&projectionMutationSettings))

	for _, tc := range []struct {
		name           string
		strategy       pipeline.MaterializationStrategy
		incrementalKey string
		tableOptions   bool
		projection     bool
	}{
		{"scd2_by_time", pipeline.MaterializationStrategySCD2ByTime, "updated_at", false, false},
		{"scd2_by_column", pipeline.MaterializationStrategySCD2ByColumn, "updated_at", false, false},
		{"scd2_by_column_without_key", pipeline.MaterializationStrategySCD2ByColumn, "", false, false},
		{"scd2_by_column_with_index", pipeline.MaterializationStrategySCD2ByColumn, "updated_at", true, false},
		{"scd2_by_column_with_projection", pipeline.MaterializationStrategySCD2ByColumn, "updated_at", true, true},
	} {
		//nolint:paralleltest // Staging subtests share a temporary table name.
		for _, joinUseNulls := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/join_use_nulls=%d", tc.name, joinUseNulls), func(t *testing.T) {
				if tc.projection && projectionMutationSettings == 0 {
					t.Skip("ClickHouse server does not support lightweight mutations on projection tables")
				}
				ctx := click_house.Context(t.Context(), click_house.WithSettings(click_house.Settings{
					"join_use_nulls": joinUseNulls,
				}))
				asset := &pipeline.Asset{
					Name: fmt.Sprintf("%s.%s_%d", database, tc.name, joinUseNulls),
					Type: pipeline.AssetTypeClickHouse,
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable, Strategy: tc.strategy, IncrementalKey: tc.incrementalKey,
					},
					Columns: []pipeline.Column{
						{Name: "tenant", Type: "UInt64", PrimaryKey: true},
						{Name: "id", Type: "UInt64", PrimaryKey: true},
						{Name: "value", Type: "Nullable(String)"},
						{Name: "updated_at", Type: "DateTime64(6, 'UTC')"},
					},
				}
				if tc.tableOptions {
					asset.ClickHouse = pipeline.ClickHouseConfig{
						OrderBy:    []string{"tenant", "id", "_valid_from"},
						PrimaryKey: []string{"tenant"},
						Indexes: []pipeline.ClickHouseIndex{
							{Name: "idx_valid_until", Expression: "_valid_until", Type: "minmax"},
						},
					}
				}
				if tc.projection {
					asset.ClickHouse.Projections = []pipeline.ClickHouseProjection{
						{Name: "p_by_current", Select: "SELECT _is_current, count() GROUP BY _is_current"},
					}
					asset.ClickHouse.Settings = map[string]string{"lightweight_mutation_projection_mode": "'drop'"}
				}
				checkProjections := func() {
					t.Helper()
					for _, projection := range asset.ClickHouse.Projections {
						var projectionParts uint64
						require.NoError(t, conn.QueryRow(
							ctx, "SELECT count() FROM system.projection_parts WHERE database = ? AND table = ? AND name = ? AND active AND rows > 0",
							database, fmt.Sprintf("%s_%d", tc.name, joinUseNulls), projection.Name,
						).Scan(&projectionParts))
						assert.Positive(t, projectionParts, "projection must be populated for inserted history rows")
					}
				}
				type sourceRow struct {
					tenant uint64
					id     uint64
					value  string
					date   string
				}
				sourceQuery := func(source []sourceRow) string {
					rows := make([]string, len(source))
					for i, row := range source {
						rows[i] = fmt.Sprintf("SELECT toUInt64(%d) AS tenant, toUInt64(%d) AS id, CAST(%s AS Nullable(String)) AS value, toDateTime64('%s', 6, 'UTC') AS updated_at", row.tenant, row.id, row.value, row.date)
					}
					return strings.Join(rows, " UNION ALL ")
				}
				run := func(query string, fullRefresh bool) {
					t.Helper()
					statements, cleanup, renderErr := NewMaterializer(fullRefresh).RenderWithCleanup(asset, query)
					require.NoError(t, renderErr)
					defer func() {
						for _, statement := range cleanup {
							assert.NoError(t, conn.Exec(ctx, statement), statement)
						}
					}()
					for _, statement := range statements {
						require.NoError(t, conn.Exec(ctx, statement), statement)
					}
				}
				type historyRow struct {
					updatedAt  time.Time
					validFrom  time.Time
					validUntil time.Time
					current    uint8
				}
				read := func() map[string]historyRow {
					t.Helper()
					// String conversion avoids the native driver's nanosecond range limit for the sentinel.
					rows, queryErr := conn.Query(ctx, "SELECT tenant, id, ifNull(value, '<NULL>'), updated_at, _valid_from, toString(_valid_until), toUInt8(_is_current) FROM "+asset.Name)
					require.NoError(t, queryErr)
					defer rows.Close()
					result := make(map[string]historyRow)
					for rows.Next() {
						var tenant, id uint64
						var value, validUntil string
						var row historyRow
						require.NoError(t, rows.Scan(&tenant, &id, &value, &row.updatedAt, &row.validFrom, &validUntil, &row.current))
						var parseErr error
						row.validUntil, parseErr = time.Parse("2006-01-02 15:04:05.999999", validUntil)
						require.NoError(t, parseErr)
						key := fmt.Sprintf("%d/%d/%s", tenant, id, value)
						require.NotContains(t, result, key, "duplicate SCD2 version")
						result[key] = row
					}
					require.NoError(t, rows.Err())
					return result
				}
				checkCurrent := func(history map[string]historyRow, want ...string) {
					t.Helper()
					var current []string
					for key, row := range history {
						if row.current != 0 {
							current = append(current, key)
							assert.Equal(t, time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC), row.validUntil)
							if tc.incrementalKey != "" {
								assert.Equal(t, row.updatedAt, row.validFrom)
							}
						}
					}
					assert.ElementsMatch(t, want, current)
				}
				const firstDate = "2025-01-01 12:00:00.123456"
				const secondDate = "2025-01-02 12:00:00.234567"
				const thirdDate = "2025-01-03 12:00:00.345678"
				firstQuery := sourceQuery([]sourceRow{
					{1, 1, "'original'", firstDate},
					{2, 1, "'unchanged'", firstDate},
					{1, 2, "NULL", firstDate},
					{1, 3, "'removed'", firstDate},
				})
				run(firstQuery, false)
				if tc.tableOptions {
					var primaryKey, createSQL string
					require.NoError(t, conn.QueryRow(
						ctx, "SELECT primary_key, create_table_query FROM system.tables WHERE database = ? AND name = ?",
						database, fmt.Sprintf("%s_%d", tc.name, joinUseNulls),
					).Scan(&primaryKey, &createSQL))
					assert.Equal(t, "tenant", primaryKey)
					assert.Contains(t, createSQL, "INDEX idx_valid_until _valid_until TYPE minmax GRANULARITY 1")
				}
				first := read()
				require.Len(t, first, 4)
				checkCurrent(first, "1/1/original", "2/1/unchanged", "1/2/<NULL>", "1/3/removed")
				checkProjections()
				run(firstQuery, false)
				assert.Equal(t, first, read(), "unchanged reruns must retain all validity timestamps")

				changeDate := secondDate
				if tc.strategy == pipeline.MaterializationStrategySCD2ByColumn {
					changeDate = firstDate
				}
				secondQuery := sourceQuery([]sourceRow{
					{1, 1, "NULL", changeDate},
					{2, 1, "'unchanged'", firstDate},
					{1, 2, "'changed'", changeDate},
					{1, 4, "'new'", secondDate},
				})
				beforeRemoval := time.Now().UTC()
				run(secondQuery, false)
				second := read()
				require.Len(t, second, 7)
				checkCurrent(second, "1/1/<NULL>", "2/1/unchanged", "1/2/changed", "1/4/new")
				checkProjections()
				assert.Equal(t, first["2/1/unchanged"], second["2/1/unchanged"])
				for old, next := range map[string]string{"1/1/original": "1/1/<NULL>", "1/2/<NULL>": "1/2/changed"} {
					assert.Equal(t, first[old].validFrom, second[old].validFrom)
					assert.Equal(t, second[next].validFrom, second[old].validUntil)
					assert.Zero(t, second[old].current)
				}
				assert.Zero(t, second["1/3/removed"].current)
				assert.True(t, second["1/3/removed"].validUntil.After(beforeRemoval))
				assert.True(t, second["1/3/removed"].validUntil.Before(time.Now().UTC()))
				run(secondQuery, false)
				assert.Equal(t, second, read(), "reruns must not duplicate changes or re-expire history")

				thirdQuery := sourceQuery([]sourceRow{
					{1, 1, "'latest'", thirdDate},
					{2, 1, "'unchanged'", firstDate},
					{1, 2, "'changed'", changeDate},
					{1, 3, "'returned'", thirdDate},
				})
				run(thirdQuery, false)
				third := read()
				require.Len(t, third, 9)
				checkCurrent(third, "1/1/latest", "2/1/unchanged", "1/2/changed", "1/3/returned")
				for key, row := range second {
					if row.current == 0 {
						assert.Equal(t, row, third[key], "historical validity must remain unchanged: %s", key)
					}
				}
				assert.Equal(t, third["1/1/latest"].validFrom, third["1/1/<NULL>"].validUntil)
				assert.Equal(t, second["1/2/changed"], third["1/2/changed"])

				emptyQuery := "SELECT * FROM (" + thirdQuery + ") WHERE 0"
				run(emptyQuery, false)
				empty := read()
				require.Len(t, empty, 9)
				checkCurrent(empty)
				run(emptyQuery, false)
				assert.Equal(t, empty, read(), "an empty snapshot must preserve expired history on rerun")

				run(thirdQuery, true)
				refreshed := read()
				require.Len(t, refreshed, 4)
				checkCurrent(refreshed, "1/1/latest", "2/1/unchanged", "1/2/changed", "1/3/returned")
				checkProjections()
			})
		}
	}
}
