package clickhouse

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMaterializer_ClickHouseServer uses a disposable database on the native
// ClickHouse endpoint configured in BRUIN_CLICKHOUSE_TEST_ADDR (host:port).
// Optional credentials use BRUIN_CLICKHOUSE_TEST_USER and
// BRUIN_CLICKHOUSE_TEST_PASSWORD. The user must be able to create databases.
func TestMaterializer_ClickHouseServer(t *testing.T) { //nolint:paralleltest // Staging subtests share a temporary table name.
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
	cases := []struct {
		name          string
		options       pipeline.ClickHouseConfig
		partitionBy   string
		primaryKey    bool
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

	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyMerge,
	} {
		t.Run(string(strategy)+"/staging", func(t *testing.T) {
			asset := &pipeline.Asset{
				Name: database + ".staging_target",
				Type: pipeline.AssetTypeClickHouse,
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable, Strategy: strategy,
					IncrementalKey: "id", PartitionBy: "toYYYYMM(event_time)",
				},
				Columns: columns(true), ClickHouse: options,
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
			for _, statement := range cleanup {
				require.NoError(t, conn.Exec(t.Context(), statement))
			}
		})
	}
}
