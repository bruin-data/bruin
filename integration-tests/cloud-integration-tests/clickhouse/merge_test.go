package clickhouse_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/clickhouse"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

// TestMergeUpsertsByPrimaryKey exercises the clickhouse.sql `merge`
// materialization strategy end to end: existing rows are replaced by primary
// key, new rows are inserted, and untouched rows remain. The rerun asserts
// idempotency, which requires the materializer to disable ClickHouse block
// deduplication for the insert (see issue #2396).
func TestMergeUpsertsByPrimaryKey(t *testing.T) {
	host, port := startClickHouse(t)
	configPath := writeClickHouseConfig(t, host, port)
	binary := bruinBinary(t)

	runBruinQuery(t, binary, configPath,
		"CREATE TABLE merge_target (row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		"CREATE TABLE merge_seed (row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		"INSERT INTO merge_target (row_id, amount) VALUES ('row_1', 10), ('row_2', 20), ('row_3', 30)",
	)
	runBruinQuery(t, binary, configPath,
		"INSERT INTO merge_seed (row_id, amount) VALUES ('row_2', 200), ('row_4', 400)",
	)

	assetPath := writeMergePipeline(t, "clickhouse-merge-test", `/* @bruin
name: merge_target
type: clickhouse.sql
materialization:
  type: table
  strategy: merge
columns:
  - name: row_id
    type: String
    primary_key: true
  - name: amount
    type: Int32
@bruin */

SELECT row_id, amount
FROM merge_seed
`)

	for range 2 {
		runBruin(t, binary, configPath, "run", assetPath)
	}

	client := newClient(t, host, port)
	rows, err := client.Select(t.Context(), &query.Query{
		Query: "SELECT row_id, amount FROM merge_target ORDER BY row_id",
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"row_1", int32(10)},
		{"row_2", int32(200)},
		{"row_3", int32(30)},
		{"row_4", int32(400)},
	}, rows)

	require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: "SYSTEM FLUSH LOGS"}))
	rows, err = client.Select(t.Context(), &query.Query{
		Query: `SELECT count()
FROM system.query_log
WHERE type = 'QueryFinish'
  AND position(query, 'INSERT INTO merge_target SETTINGS insert_deduplicate = 0') > 0`,
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{uint64(2)}}, rows)

	// The temp staging table must be dropped after each run.
	rows, err = client.Select(t.Context(), &query.Query{
		Query: "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%__bruin_tmp%'",
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{uint64(0)}}, rows)
}

// TestMergeCompositePrimaryKey verifies that merge matches rows on the full
// primary key tuple: a row sharing one key column but not the other is left
// untouched.
func TestMergeCompositePrimaryKey(t *testing.T) {
	host, port := startClickHouse(t)
	configPath := writeClickHouseConfig(t, host, port)
	binary := bruinBinary(t)

	runBruinQuery(t, binary, configPath,
		"CREATE TABLE merge_composite_target (row_id String, event_date Date, amount Int32) ENGINE = MergeTree ORDER BY (row_id, event_date)",
	)
	runBruinQuery(t, binary, configPath,
		"CREATE TABLE merge_composite_seed (row_id String, event_date Date, amount Int32) ENGINE = MergeTree ORDER BY (row_id, event_date)",
	)
	runBruinQuery(t, binary, configPath,
		`INSERT INTO merge_composite_target (row_id, event_date, amount) VALUES
			('row_1', toDate('2026-07-16'), 10),
			('row_2', toDate('2026-07-16'), 20),
			('row_2', toDate('2026-07-17'), 21),
			('row_3', toDate('2026-07-16'), 30)`,
	)
	runBruinQuery(t, binary, configPath,
		`INSERT INTO merge_composite_seed (row_id, event_date, amount) VALUES
			('row_2', toDate('2026-07-16'), 200),
			('row_4', toDate('2026-07-16'), 400)`,
	)

	assetPath := writeMergePipeline(t, "clickhouse-merge-composite-test", `/* @bruin
name: merge_composite_target
type: clickhouse.sql
materialization:
  type: table
  strategy: merge
columns:
  - name: row_id
    type: String
    primary_key: true
  - name: event_date
    type: Date
    primary_key: true
  - name: amount
    type: Int32
@bruin */

SELECT row_id, event_date, amount
FROM merge_composite_seed
`)

	for range 2 {
		runBruin(t, binary, configPath, "run", assetPath)
	}

	client := newClient(t, host, port)
	rows, err := client.Select(t.Context(), &query.Query{
		Query: "SELECT row_id, toString(event_date), amount FROM merge_composite_target ORDER BY row_id, event_date",
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"row_1", "2026-07-16", int32(10)},
		{"row_2", "2026-07-16", int32(200)},
		{"row_2", "2026-07-17", int32(21)},
		{"row_3", "2026-07-16", int32(30)},
		{"row_4", "2026-07-16", int32(400)},
	}, rows)
}

// TestMergeFullRefreshReplacesTable verifies that a full refresh of a merge
// asset falls back to create+replace, so the table ends up holding exactly the
// asset query result regardless of its previous contents.
func TestMergeFullRefreshReplacesTable(t *testing.T) {
	host, port := startClickHouse(t)
	configPath := writeClickHouseConfig(t, host, port)
	binary := bruinBinary(t)

	runBruinQuery(t, binary, configPath,
		"CREATE TABLE merge_fr_target (row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		"CREATE TABLE merge_fr_seed (row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		"INSERT INTO merge_fr_target (row_id, amount) VALUES ('stale_row', 999)",
	)
	runBruinQuery(t, binary, configPath,
		"INSERT INTO merge_fr_seed (row_id, amount) VALUES ('row_2', 200), ('row_4', 400)",
	)

	assetPath := writeMergePipeline(t, "clickhouse-merge-full-refresh-test", `/* @bruin
name: merge_fr_target
type: clickhouse.sql
materialization:
  type: table
  strategy: merge
columns:
  - name: row_id
    type: String
    primary_key: true
  - name: amount
    type: Int32
@bruin */

SELECT row_id, amount
FROM merge_fr_seed
`)

	runBruin(t, binary, configPath, "run", "--full-refresh", assetPath)

	client := newClient(t, host, port)
	rows, err := client.Select(t.Context(), &query.Query{
		Query: "SELECT row_id, amount FROM merge_fr_target ORDER BY row_id",
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"row_2", int32(200)},
		{"row_4", int32(400)},
	}, rows)
}

func newClient(t *testing.T, host string, port int) *clickhouse.Client {
	t.Helper()

	client, err := clickhouse.NewClient(&clickhouse.Config{
		Username: clickHouseUser,
		Password: clickHousePassword,
		Host:     host,
		Port:     port,
		Database: clickHouseDatabase,
	})
	require.NoError(t, err)
	return client
}

func writeMergePipeline(t *testing.T, pipelineName, assetSQL string) string {
	t.Helper()

	pipelineDir, err := os.MkdirTemp(".", "merge-pipeline-")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(pipelineDir))
	})
	pipelineDir, err = filepath.Abs(pipelineDir)
	require.NoError(t, err)
	assetsDir := filepath.Join(pipelineDir, "assets")
	require.NoError(t, os.MkdirAll(assetsDir, 0o755))

	pipelineYAML := fmt.Sprintf(`name: %s
default_connections:
  clickhouse: clickhouse-default
`, pipelineName)
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte(pipelineYAML), 0o600))

	assetPath := filepath.Join(assetsDir, "merge_asset.sql")
	require.NoError(t, os.WriteFile(assetPath, []byte(assetSQL), 0o600))
	return assetPath
}
