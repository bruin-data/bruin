//go:build !bruin_no_duckdb

package inference

import (
	"context"
	"encoding/json"
	"net/http"
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

// The API transport is the only mocked layer: YAML parsing, provider request /
// response conversion, Arrow, ingestr, and DuckDB all execute normally.
func TestTypeSafeDuckDBTypes(t *testing.T) {
	if os.Getenv("BRUIN_INFERENCE_INTEGRATION_TEST") != "1" {
		t.Skip("set BRUIN_INFERENCE_INTEGRATION_TEST=1 for the DuckDB/ingestr integration test")
	}
	live := os.Getenv("BRUIN_TYPESAFE_LIVE_TEST") == "1"
	ctx := context.WithValue(t.Context(), executor.ContextLogger, zap.NewNop().Sugar())
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o700))
	client, err := duck.NewClient(duck.Config{Path: filepath.Join(root, "typesafe.duckdb")})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	exec := func(sql string) {
		t.Helper()
		require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: sql}))
	}
	assertSQL := func(condition string) {
		t.Helper()
		exec("SELECT CASE WHEN (" + condition + ") THEN true ELSE error('TypeSafe integration assertion failed') END")
	}
	exec(`CREATE TABLE tickets AS SELECT 1::BIGINT AS id, 'URGENT: I was charged twice. Please refund me immediately!' AS body
        UNION ALL SELECT 7::BIGINT, 'The login bug is resolved. Everything works. No action needed, thank you.'`)
	asset, err := pipeline.ConvertYamlToTask([]byte(`
name: analytics.jev
type: inference
connection: warehouse
parameters:
  provider: typesafe
  model: jev-1.13.0
  input_query: SELECT * FROM tickets ORDER BY id
  context: '{{ row.body }}'
  extract_parallelism: 1
materialization:
  type: table
  strategy: merge
columns:
  - name: id
    primary_key: true
  - name: category
    type: string
    inference:
      prompt: What subject does the message concern?
      choices:
        billing: Payments and refunds
        technical: Login and software
  - name: probability
    type: number
    inference:
      prompt: Does the customer need urgent action?
  - name: urgent
    type: boolean
    inference:
      prompt: Does the customer need urgent action?
  - name: page
    type: boolean
    inference:
      prompt: Does the customer need urgent action?
      threshold: 0.8
  - name: severity
    type: number
    inference:
      prompt: Rate the need for action using the provided levels.
      levels:
        - Resolved, no action needed
        - Needs nonurgent attention
        - Explicitly requires immediate action
`))
	require.NoError(t, err)
	asset.DefinitionFile.Path = filepath.Join(root, "jev.asset.yml")
	ti := &scheduler.AssetInstance{Asset: asset, Pipeline: &pipeline.Pipeline{Name: "jev-types", Assets: []*pipeline.Asset{asset}}}
	op := NewOperator(testProviderConnections(t, warehouseGetter{client}, live))
	op.cacheDir = filepath.Join(root, "cache")
	calls := 0
	op.structured = func(callCtx context.Context, c *Client, state, instructions string, columns []outputColumn) (map[string]any, error) {
		calls++
		if !live {
			c.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				var body map[string]any
				require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
				require.Len(t, body["questions"], 5, "all Jev primitives must share one request")
				if state == "URGENT: I was charged twice. Please refund me immediately!" {
					return response(200, `{"answers":{"category":{"type":"choice","choice":"billing"},"probability":{"type":"noul","noul":0.8125},"urgent":{"type":"noul","noul":0.4999},"page":{"type":"noul","noul":0.8},"severity":{"type":"score","score":1.25}}}`), nil
				}
				return response(200, `{"answers":{"category":{"type":"choice","choice":"technical"},"probability":{"type":"noul","noul":0.125},"urgent":{"type":"noul","noul":0.5},"page":{"type":"noul","noul":0.7999},"severity":{"type":"score","score":0.375}}}`), nil
			})}
		}
		return c.CompleteStructured(callCtx, state, instructions, columns)
	}
	assertTypes := func() {
		t.Helper()
		assertSQL(`(SELECT count(*) = 5 FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'jev'
            AND ((column_name = 'category' AND data_type = 'VARCHAR')
              OR (column_name IN ('urgent', 'page') AND data_type = 'BOOLEAN')
              OR (column_name IN ('probability', 'severity') AND data_type = 'DOUBLE')))`)
	}
	require.NoError(t, op.Run(ctx, ti))
	require.Equal(t, 2, calls)
	assertTypes()
	if live {
		assertSQL(`(SELECT count(*) = 2
            AND count(*) FILTER (WHERE id = 1 AND category = 'billing' AND urgent AND page AND probability > 0.8 AND severity > 1) = 1
            AND count(*) FILTER (WHERE id = 7 AND category = 'technical' AND NOT urgent AND NOT page AND probability < 0.5 AND severity < 1) = 1
            FROM analytics.jev)`)
	} else {
		assertSQL(`(SELECT count(*) = 2
            AND count(*) FILTER (WHERE id = 1 AND category = 'billing' AND NOT urgent AND page AND probability = 0.8125 AND severity = 1.25) = 1
            AND count(*) FILTER (WHERE id = 7 AND category = 'technical' AND urgent AND NOT page AND probability = 0.125 AND severity = 0.375) = 1
            FROM analytics.jev)`)
	}
	// Update generated values through merge; the omitted row must remain.
	exec("UPDATE tickets SET body = (SELECT body FROM tickets WHERE id = 7) WHERE id = 1")
	asset.Parameters["input_query"] = "SELECT * FROM tickets WHERE id = 1"
	require.NoError(t, op.Run(ctx, ti))
	require.Equal(t, 2, calls, "a different primary key must reuse the identical rendered request")
	assertTypes()
	if live {
		assertSQL("(SELECT count(*) = 2 AND bool_and(category = 'technical' AND NOT urgent AND NOT page AND probability < 0.5 AND severity < 1) FROM analytics.jev)")
	} else {
		assertSQL("(SELECT count(*) = 2 AND bool_and(category = 'technical' AND urgent AND NOT page AND probability = 0.125 AND severity = 0.375) FROM analytics.jev)")
	}
	// Populated replacement, then empty replacement, must retain physical types.
	asset.Materialization.Strategy = pipeline.MaterializationStrategyCreateReplace
	require.NoError(t, op.Run(ctx, ti))
	assertTypes()
	assertSQL("(SELECT count(*) = 1 AND bool_and(id = 1 AND category = 'technical') FROM analytics.jev)")
	asset.Parameters["input_query"] = "SELECT * FROM tickets WHERE false"
	require.NoError(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 0 FROM analytics.jev)")
	assertTypes()
	require.Equal(t, 2, calls, "cached and empty replacements must not call Jev")
}

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
	asset.Parameters["context"] = "{{ row.body }}"
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
	op := NewOperator(testProviderConnections(t, warehouseGetter{client}, os.Getenv("BRUIN_INFERENCE_LIVE_TEST") == "1"))
	op.cacheDir = filepath.Join(root, "cache")
	calls := 0
	if os.Getenv("BRUIN_INFERENCE_LIVE_TEST") != "1" {
		op.structured = func(callCtx context.Context, c *Client, state, instructions string, columns []outputColumn) (map[string]any, error) {
			calls++
			require.Contains(t, state, "charged twice")
			c.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return response(http.StatusOK, responseText(`{"category":"billing"}`)), nil
			})}
			return c.CompleteStructured(callCtx, state, instructions, columns)
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

// TestGroupedDuckDBMaterialization exercises grouped structured inference with
// real DuckDB reads and the real ingestr writer. OpenCode is always mocked; the
// TypeSafe group is live only when the separate paid-test flag is enabled.
func TestGroupedDuckDBMaterialization(t *testing.T) {
	if os.Getenv("BRUIN_INFERENCE_INTEGRATION_TEST") != "1" {
		t.Skip("set BRUIN_INFERENCE_INTEGRATION_TEST=1 for the DuckDB/ingestr integration test")
	}
	liveTypeSafe := os.Getenv("BRUIN_TYPESAFE_LIVE_TEST") == "1"

	ctx := context.WithValue(t.Context(), executor.ContextLogger, zap.NewNop().Sugar())
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o700))
	client, err := duck.NewClient(duck.Config{Path: filepath.Join(root, "grouped.duckdb")})
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
	exec(`CREATE TABLE raw.grouped_tickets AS
        SELECT 1::BIGINT AS id, 'charged twice after upgrading'::VARCHAR AS body, 'enterprise'::VARCHAR AS customer_tier
        UNION ALL
        SELECT 2::BIGINT, 'settings page crashes on open'::VARCHAR, NULL::VARCHAR`)
	asset := &pipeline.Asset{
		Name: "analytics.grouped_classified", Type: pipeline.AssetTypeInference, Connection: "warehouse",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge},
		Columns: []pipeline.Column{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "category", Type: "string", Inference: &pipeline.ColumnInference{
				Prompt: "Classify the ticket", Provider: "typesafe", Model: "jev-1.13.0",
				Choices: map[string]string{"billing": "A billing or payment problem", "technical": "A product technical problem"},
			}},
			{Name: "priority", Type: "string", Inference: &pipeline.ColumnInference{
				Prompt: "Mark duplicate charges urgent; mark other problems normal.", Provider: "typesafe", Model: "jev-1.13.0",
				Choices: map[string]string{"normal": "Any problem other than a duplicate charge", "urgent": "The customer was charged twice"},
			}},
			{Name: "urgent", Type: "boolean", Inference: &pipeline.ColumnInference{Prompt: "Whether this needs an urgent response"}},
			{Name: "summary", Type: "string", Inference: &pipeline.ColumnInference{Prompt: "Summarize the ticket briefly"}},
			{Name: "rank", Type: "integer", Inference: &pipeline.ColumnInference{Prompt: "Rank severity as an integer"}},
			{Name: "score", Type: "number", Inference: &pipeline.ColumnInference{Prompt: "Give a confidence score"}},
		},
		Parameters: pipeline.ParameterMap{
			"provider": "opencode", "model": "muse-spark-1.3", "input_asset": "raw.grouped_tickets",
			"context": "{{ row.body }}", "instructions": "Use the ticket text only.", "extract_parallelism": 1,
		},
	}
	asset.DefinitionFile.Path = filepath.Join(root, "grouped.asset.yml")
	source := &pipeline.Asset{Name: "raw.grouped_tickets", Type: pipeline.AssetTypeDuckDBQuery}
	pipe := &pipeline.Pipeline{Name: "grouped-integration", DefaultConnections: pipeline.EmptyStringMap{"duckdb": "warehouse"}, Assets: []*pipeline.Asset{source, asset}}
	ti := &scheduler.AssetInstance{Asset: asset, Pipeline: pipe}
	op := NewOperator(testProviderConnections(t, warehouseGetter{client}, liveTypeSafe))
	op.cacheDir = filepath.Join(root, "cache")
	realStructured := op.structured
	typeSafeCalls, openCodeCalls := 0, 0
	op.structured = func(callCtx context.Context, c *Client, state, instructions string, columns []outputColumn) (map[string]any, error) {
		require.Equal(t, "Use the ticket text only.", instructions)
		if c.Provider == "typesafe" {
			typeSafeCalls++
			require.Equal(t, "jev-1.13.0", c.Model)
			require.Len(t, columns, 2)
			if liveTypeSafe {
				return realStructured(callCtx, c, state, instructions, columns)
			}
			if state == "charged twice after upgrading" {
				return map[string]any{"category": "billing", "priority": "urgent"}, nil
			}
			return map[string]any{"category": "technical", "priority": "normal"}, nil
		}
		openCodeCalls++
		require.Equal(t, "opencode", c.Provider)
		require.Equal(t, "muse-spark-1.3", c.Model)
		require.Len(t, columns, 4)
		if state == "charged twice after upgrading" {
			return map[string]any{"urgent": true, "summary": "duplicate charge", "rank": int64(1), "score": 0.95}, nil
		}
		return map[string]any{"urgent": false, "summary": "settings crash", "rank": int64(2), "score": 0.75}, nil
	}

	require.NoError(t, op.Run(ctx, ti))
	require.Equal(t, 2, typeSafeCalls)
	require.Equal(t, 2, openCodeCalls)
	assertSQL(`(SELECT count(*) = 2
        AND count(*) FILTER (WHERE id = 1 AND body = 'charged twice after upgrading' AND customer_tier = 'enterprise'
            AND category = 'billing' AND priority = 'urgent' AND urgent AND summary = 'duplicate charge' AND rank = 1 AND score = 0.95) = 1
        AND count(*) FILTER (WHERE id = 2 AND body = 'settings page crashes on open' AND customer_tier IS NULL
            AND category = 'technical' AND priority = 'normal' AND NOT urgent AND summary = 'settings crash' AND rank = 2 AND score = 0.75) = 1
        FROM analytics.grouped_classified)`)

	// Merge one changed source row and ensure the omitted destination row remains.
	exec("UPDATE raw.grouped_tickets SET customer_tier = 'premium' WHERE id = 1")
	delete(asset.Parameters, "input_asset")
	asset.Parameters["input_query"] = "SELECT * FROM raw.grouped_tickets WHERE id = 1"
	require.NoError(t, op.Run(ctx, ti))
	assertSQL(`(SELECT count(*) = 2
        AND count(*) FILTER (WHERE id = 1 AND customer_tier = 'premium' AND category = 'billing' AND priority = 'urgent') = 1
        AND count(*) FILTER (WHERE id = 2 AND customer_tier IS NULL AND category = 'technical' AND summary = 'settings crash') = 1
        FROM analytics.grouped_classified)`)

	// Empty create+replace retains all generated output types.
	asset.Materialization.Strategy = pipeline.MaterializationStrategyCreateReplace
	asset.Parameters["input_query"] = "SELECT * FROM raw.grouped_tickets WHERE false"
	require.NoError(t, op.Run(ctx, ti))
	assertSQL("(SELECT count(*) = 0 FROM analytics.grouped_classified)")
	assertSQL(`(SELECT count(*) = 3 FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = 'grouped_classified'
        AND ((column_name = 'urgent' AND data_type = 'BOOLEAN')
          OR (column_name = 'rank' AND data_type = 'BIGINT')
          OR (column_name = 'score' AND data_type = 'DOUBLE')))`)
}
