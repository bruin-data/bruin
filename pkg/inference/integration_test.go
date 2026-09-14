//go:build !bruin_no_duckdb

package inference

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type warehouseGetter struct{ client *duck.Client }

func (g warehouseGetter) GetConnection(string) any { return g.client }

// This exercises real DuckDB reads and the real ingestr writer. Model requests
// are mocked unless the separate paid live-test flag is enabled.
func TestDuckDBMaterialization(t *testing.T) {
	if os.Getenv("BRUIN_INFERENCE_INTEGRATION_TEST") != "1" {
		t.Skip("set BRUIN_INFERENCE_INTEGRATION_TEST=1 for the DuckDB/ingestr integration test")
	}
	ctx := context.WithValue(t.Context(), executor.ContextLogger, zap.NewNop().Sugar())
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o700))
	client, err := duck.NewClient(duck.Config{Path: filepath.Join(root, "test.duckdb")})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	exec := func(sql string) {
		t.Helper()
		require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: sql}))
	}
	assertSQL := func(condition string) {
		t.Helper()
		exec("SELECT CASE WHEN (" + condition + ") THEN true ELSE error('integration assertion failed') END")
	}

	exec("CREATE SCHEMA raw")
	exec(`CREATE TABLE raw.tickets AS SELECT
        1::BIGINT AS id,
        'charged twice'::VARCHAR AS body,
        12345678901234567890.123456789012345678::DECIMAL(38,18) AS amount,
        TIMESTAMP '2026-09-12 10:11:12.123456' AS event_at,
        from_hex('00FF10')::BLOB AS payload,
        NULL::VARCHAR AS optional_note`)
	asset := testAsset()
	asset.Parameters["input_asset"] = "raw.tickets"
	delete(asset.Parameters, "input_query")
	asset.Parameters["prompt"] = "Reply with exactly billing for this ticket: {{ row.body }}"
	asset.Parameters["model"] = "muse-spark-1.3"
	asset.Parameters["max_output_tokens"] = 2048
	asset.DefinitionFile.Path = filepath.Join(root, "classifications.asset.yml")
	source := &pipeline.Asset{Name: "raw.tickets", Type: pipeline.AssetTypeDuckDBQuery}
	pipe := &pipeline.Pipeline{
		Name:               "integration",
		DefaultConnections: pipeline.EmptyStringMap{"duckdb": "warehouse"},
		Assets:             []*pipeline.Asset{source, asset},
	}
	ti := &scheduler.AssetInstance{Asset: asset, Pipeline: pipe}
	op := NewOperator(warehouseGetter{client})
	op.cacheDir = filepath.Join(root, "cache")
	calls := 0
	if os.Getenv("BRUIN_INFERENCE_LIVE_TEST") != "1" {
		op.complete = func(_ context.Context, _ *Client, prompt string) (string, error) {
			calls++
			require.Contains(t, prompt, "charged twice")
			return "billing", nil
		}
	}

	require.NoError(t, op.Run(ctx, ti))
	if os.Getenv("BRUIN_INFERENCE_LIVE_TEST") != "1" {
		require.Equal(t, 1, calls)
	}
	assertSQL(`(SELECT count(*) = 1
        AND bool_and(id = 1)
        AND bool_and(amount = 12345678901234567890.123456789012345678::DECIMAL(38,18))
        AND bool_and(event_at = TIMESTAMP '2026-09-12 10:11:12.123456')
        AND bool_and(payload = from_hex('00FF10'))
        AND bool_and(optional_note IS NULL)
        AND bool_and(category = 'billing')
        FROM analytics.classified)`)

	// Exercise explicit query input and updating an existing primary key.
	delete(asset.Parameters, "input_asset")
	asset.Parameters["input_query"] = "SELECT * FROM raw.tickets"
	exec("UPDATE raw.tickets SET amount = 7.123456789012345678")
	require.NoError(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 1 AND bool_and(amount = 7.123456789012345678::DECIMAL(38,18)) FROM analytics.classified)")
	exec("DELETE FROM raw.tickets")
	require.NoError(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 1 FROM analytics.classified)")

	// Empty create+replace must replace an existing table and create a new one.
	exec("DELETE FROM raw.tickets")
	asset.Materialization.Strategy = pipeline.MaterializationStrategyCreateReplace
	require.NoError(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 0 FROM analytics.classified)")
	exec("DROP TABLE analytics.classified")
	require.NoError(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 0 FROM analytics.classified)")
	assertSQL("(SELECT numeric_precision = 38 AND numeric_scale = 18 FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'classified' AND column_name = 'amount')")

	// An empty full refresh of merge materialization must clear the destination.
	exec(`INSERT INTO raw.tickets VALUES (1, 'charged twice',
        12345678901234567890.123456789012345678, TIMESTAMP '2026-09-12 10:11:12.123456',
        from_hex('00FF10'), NULL)`)
	asset.Materialization.Strategy = pipeline.MaterializationStrategyMerge
	require.NoError(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 1 FROM analytics.classified)")
	exec("DELETE FROM raw.tickets")
	fullRefreshCtx := context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
	require.NoError(t, op.Run(fullRefreshCtx, ti))
	assertSQL("(SELECT count(*) = 0 FROM analytics.classified)")

	// The loader must reject unsupported nanosecond precision, not round it.
	asset.Parameters["input_query"] = "SELECT 1::BIGINT AS id, 'charged twice' AS body, TIMESTAMP_NS '2026-09-12 10:11:12.123456789' AS event_at"
	require.Error(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 0 FROM analytics.classified)")
}
