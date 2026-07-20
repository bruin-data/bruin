package clickhouse_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

// TestDeleteInsertRerunKeepsRows protects the regression from
// https://github.com/bruin-data/bruin/issues/2420. ClickHouse's insert
// deduplication must not treat a delete+insert refresh as a retry of the
// previous refresh after Bruin has deleted the matching interval.
func TestDeleteInsertRerunKeepsRows(t *testing.T) {
	// This ReplicatedMergeTree version reproduces SharedMergeTree's behavior:
	// the lightweight delete does not prevent the replacement block from being
	// deduplicated on an identical incremental rerun.
	host, port := startClickHouseWithKeeper(t, clickHouseDeduplicationImage)
	configPath := writeClickHouseConfig(t, host, port)
	binary := bruinBinary(t)

	runBruinQuery(t, binary, configPath,
		"CREATE TABLE bug_test_seed (event_date Date, row_id String, amount Int32) ENGINE = MergeTree ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		"CREATE TABLE strategy_delete_insert (event_date Date, row_id String, amount Int32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/strategy_delete_insert', 'replica1') ORDER BY row_id",
	)
	runBruinQuery(t, binary, configPath,
		`INSERT INTO bug_test_seed (event_date, row_id, amount) VALUES
			(toDate('2026-07-11'), 'row_1', 11),
			(toDate('2026-07-12'), 'row_2', 22),
			(toDate('2026-07-13'), 'row_3', 33),
			(toDate('2026-07-18'), 'row_4', 44),
			(toDate('2026-07-19'), 'row_5', 55)`,
	)
	runBruinQuery(t, binary, configPath,
		"INSERT INTO strategy_delete_insert SELECT * FROM bug_test_seed",
	)
	runBruinQuery(t, binary, configPath,
		"INSERT INTO bug_test_seed (event_date, row_id, amount) VALUES (toDate('2026-07-16'), 'row_7', 77)",
	)

	assetPath := writeDeleteInsertPipeline(t)
	client := newClient(t, host, port)
	for range 2 {
		runBruin(t, binary, configPath,
			"run",
			"--env", "default",
			"--start-date", "2026-07-16",
			"--end-date", "2026-07-16",
			assetPath,
		)

		rows, err := client.Select(t.Context(), &query.Query{
			Query: "SELECT row_id, amount FROM strategy_delete_insert WHERE event_date = toDate('2026-07-16')",
		})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"row_7", int32(77)}}, rows)
	}

	rows, err := client.Select(t.Context(), &query.Query{
		Query: "SELECT row_id, amount FROM strategy_delete_insert ORDER BY row_id",
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"row_1", int32(11)},
		{"row_2", int32(22)},
		{"row_3", int32(33)},
		{"row_4", int32(44)},
		{"row_5", int32(55)},
		{"row_7", int32(77)},
	}, rows)

	require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: "SYSTEM FLUSH LOGS"}))
	rows, err = client.Select(t.Context(), &query.Query{
		Query: `SELECT count()
FROM system.query_log
WHERE type = 'QueryFinish'
  AND position(query, 'INSERT INTO strategy_delete_insert SETTINGS insert_deduplicate = 0') > 0`,
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{uint64(2)}}, rows)
}

func writeDeleteInsertPipeline(t *testing.T) string {
	t.Helper()

	pipelineDir, err := os.MkdirTemp(".", "delete-insert-pipeline-")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(pipelineDir))
	})
	pipelineDir, err = filepath.Abs(pipelineDir)
	require.NoError(t, err)
	assetsDir := filepath.Join(pipelineDir, "assets")
	require.NoError(t, os.MkdirAll(assetsDir, 0o755))

	pipelineYAML := `name: clickhouse-delete-insert-test
default_connections:
  clickhouse: clickhouse-default
`
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte(pipelineYAML), 0o600))

	assetSQL := `/* @bruin
name: strategy_delete_insert
type: clickhouse.sql
materialization:
  type: table
  strategy: delete+insert
  incremental_key: event_date
columns:
  - name: event_date
    type: Date
  - name: row_id
    type: String
    primary_key: true
  - name: amount
    type: Int32
@bruin */

SELECT event_date, row_id, amount
FROM bug_test_seed
WHERE event_date BETWEEN toDate('{{start_date}}') AND toDate('{{end_date}}')
`
	assetPath := filepath.Join(assetsDir, "strategy_delete_insert.sql")
	require.NoError(t, os.WriteFile(assetPath, []byte(assetSQL), 0o600))
	return assetPath
}
