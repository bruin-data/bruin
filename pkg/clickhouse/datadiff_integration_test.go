package clickhouse

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/stretchr/testify/require"
)

// Run against a local ClickHouse server with BRUIN_TEST_CLICKHOUSE_PORT=9000.
func TestGetTableSummaryIntegration(t *testing.T) {
	portString := os.Getenv("BRUIN_TEST_CLICKHOUSE_PORT")
	if portString == "" {
		t.Skip("BRUIN_TEST_CLICKHOUSE_PORT is not set")
	}
	port, err := strconv.Atoi(portString)
	require.NoError(t, err)
	client, err := NewClient(&Config{Host: "127.0.0.1", Port: port, Database: "default"})
	require.NoError(t, err)
	ctx := context.Background()
	table := fmt.Sprintf("bruin_diff_test_%d", time.Now().UnixNano())
	require.NoError(t, client.connection.Exec(ctx, "CREATE TABLE "+table+` (
id UInt64, amount Nullable(Decimal(18, 2)), label LowCardinality(Nullable(String)),
active Nullable(Bool), happened Nullable(DateTime64(3, 'UTC')), data JSON,
items Array(Int32), code Enum8('a' = 1, 'b' = 2), day Date32
) ENGINE = MergeTree ORDER BY id`))
	t.Cleanup(func() { require.NoError(t, client.connection.Exec(ctx, "DROP TABLE "+table)) })

	empty, err := client.GetTableSummary(ctx, table, false)
	require.NoError(t, err)
	require.Zero(t, empty.RowCount)
	require.Nil(t, empty.Table.Columns[1].Stats.(*diff.NumericalStatistics).Min)
	require.Zero(t, empty.Table.Columns[2].Stats.(*diff.StringStatistics).AvgLength)

	require.NoError(t, client.connection.Exec(ctx, "INSERT INTO "+table+` VALUES
(1, 10.5, 'é', true, '2026-01-01 00:00:00', '{"x":1}', [1], 'a', '2026-01-01'),
(2, 20.5, '', false, '2026-01-02 00:00:00', '{}', [], 'b', '2026-01-02'),
(3, NULL, NULL, NULL, NULL, '{}', [], 'a', '2026-01-03')`))
	schema, err := client.GetTableSummary(ctx, "default."+table, true)
	require.NoError(t, err)
	require.Len(t, schema.Table.Columns, 9)
	require.Nil(t, schema.Table.Columns[1].Stats)
	require.True(t, schema.Table.Columns[0].PrimaryKey)
	require.False(t, schema.Table.Columns[0].Unique)
	full, err := client.GetTableSummary(ctx, table, false)
	require.NoError(t, err)
	require.EqualValues(t, 3, full.RowCount)
	numeric := full.Table.Columns[1].Stats.(*diff.NumericalStatistics)
	require.Equal(t, 31.0, *numeric.Sum)
	require.EqualValues(t, 1, numeric.NullCount)
	require.EqualValues(t, 1, full.Table.Columns[2].Stats.(*diff.StringStatistics).MaxLength)
	require.EqualValues(t, 1, full.Table.Columns[3].Stats.(*diff.BooleanStatistics).TrueCount)
	require.Equal(t, "2026-01-01", full.Table.Columns[4].Stats.(*diff.DateTimeStatistics).EarliestDate.Format("2006-01-02"))

	// A normal SELECT must exclude lightweight-deleted rows even before parts merge.
	require.NoError(t, client.connection.Exec(ctx, "DELETE FROM "+table+" WHERE id = 2 SETTINGS lightweight_deletes_sync = 2"))
	after, err := client.GetTableSummary(ctx, table, false)
	require.NoError(t, err)
	require.EqualValues(t, 2, after.RowCount)
	require.Equal(t, 10.5, *after.Table.Columns[1].Stats.(*diff.NumericalStatistics).Sum)
	require.True(t, diff.CompareTableSchemas(full, after, table, table).HasRowCountDifference)
}
