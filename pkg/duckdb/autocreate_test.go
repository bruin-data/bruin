//go:build !bruin_no_duckdb

package duck

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

type staticConnections map[string]any

func (c staticConnections) GetConnection(name string) any {
	return c[name]
}

func newTestClient(t *testing.T, file string) *Client {
	t.Helper()

	client, err := NewClient(Config{Path: filepath.Join(t.TempDir(), file)})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	return client
}

func execAll(t *testing.T, client *Client, statements ...string) {
	t.Helper()

	for _, statement := range statements {
		require.NoError(t, client.RunQueryWithoutResult(context.Background(), &query.Query{Query: statement}))
	}
}

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, "warehouse.duckdb")
	execAll(
		t, client,
		"CREATE SCHEMA demo",
		"CREATE TABLE demo.items (id INTEGER)",
		"CREATE VIEW demo.items_view AS SELECT * FROM demo.items",
		"CREATE TABLE main_items (id INTEGER)",
	)

	tests := []struct {
		name  string
		table string
		want  bool
	}{
		{name: "schema.table", table: "demo.items", want: true},
		{name: "identifiers are case-insensitive", table: "DEMO.Items", want: true},
		{name: "view", table: "demo.items_view", want: true},
		{name: "table in the default schema", table: "main_items", want: true},
		{name: "catalog.schema.table", table: "warehouse.demo.items", want: true},
		{name: "database.table", table: "warehouse.main_items", want: true},
		{name: "missing table", table: "demo.missing", want: false},
		{name: "missing schema", table: "nowhere.items", want: false},
		{name: "missing one-part table", table: "missing", want: false},
		{name: "one-part name ignores schemas off the search path", table: "items", want: false},
		{name: "table in another schema", table: "main.items", want: false},
		{name: "missing catalog", table: "nowhere.demo.items", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := client.RelationExists(context.Background(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := client.RelationExists(context.Background(), "demo..items")
	require.Error(t, err)
	_, err = client.RelationExists(context.Background(), "a.b.c.d")
	require.Error(t, err)
}

// TestAutoCreateFirstRun runs every incremental strategy twice against a fresh
// database: the first run has to create the table, the second has to load into it.
func TestAutoCreateFirstRun(t *testing.T) {
	t.Parallel()

	columns := []pipeline.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true},
		{Name: "label", Type: "VARCHAR"},
		{Name: "updated_at", Type: "TIMESTAMP"},
	}

	tests := []struct {
		strategy        pipeline.MaterializationStrategy
		incrementalKey  string
		timeGranularity pipeline.MaterializationTimeGranularity
		afterSecondRun  int64
	}{
		{strategy: pipeline.MaterializationStrategyAppend, afterSecondRun: 4},
		{strategy: pipeline.MaterializationStrategyMerge, afterSecondRun: 2},
		{strategy: pipeline.MaterializationStrategyDeleteInsert, incrementalKey: "id", afterSecondRun: 2},
		{strategy: pipeline.MaterializationStrategyTruncateInsert, afterSecondRun: 2},
		{strategy: pipeline.MaterializationStrategyTimeInterval, incrementalKey: "updated_at", timeGranularity: pipeline.MaterializationTimeGranularityTimestamp, afterSecondRun: 2},
		{strategy: pipeline.MaterializationStrategySCD2ByColumn, afterSecondRun: 2},
		{strategy: pipeline.MaterializationStrategySCD2ByTime, incrementalKey: "updated_at", afterSecondRun: 2},
	}

	for _, tt := range tests {
		t.Run(string(tt.strategy), func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, "autocreate.duckdb")
			asset := &pipeline.Asset{
				Name:       "demo.items",
				Type:       pipeline.AssetTypeDuckDBQuery,
				Connection: "duck",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        tt.strategy,
					IncrementalKey:  tt.incrementalKey,
					TimeGranularity: tt.timeGranularity,
				},
				Columns: columns,
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT * FROM (VALUES (1, 'a', TIMESTAMP '2024-01-01 10:00:00'), (2, 'b', TIMESTAMP '2024-01-01 11:00:00')) AS t(id, label, updated_at)",
				},
			}

			out := runAutoCreate(t, client, asset)
			assert.Contains(t, out, "Table 'demo.items' does not exist yet, creating it from the query.")
			assert.Equal(t, int64(2), countRows(t, client, "demo.items"))

			out = runAutoCreate(t, client, asset)
			assert.NotContains(t, out, "does not exist yet")
			assert.Equal(t, tt.afterSecondRun, countRows(t, client, "demo.items"))
		})
	}
}

func TestAutoCreateFirstRunRendersFullRefreshBranch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, "autocreate.duckdb")
	asset := &pipeline.Asset{
		Name:       "demo.events",
		Type:       pipeline.AssetTypeDuckDBQuery,
		Connection: "duck",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: pipeline.MaterializationStrategyAppend,
		},
		// The incremental branch reads the table, which fails while it does not exist.
		ExecutableFile: pipeline.ExecutableFile{
			Content: "{% if full_refresh %}SELECT 1 AS id{% else %}SELECT max(id) + 1 AS id FROM demo.events{% endif %}",
		},
	}

	runAutoCreate(t, client, asset)
	runAutoCreate(t, client, asset)

	rows, err := client.Select(context.Background(), &query.Query{Query: "SELECT id FROM demo.events ORDER BY id"})
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, rows)
}

func runAutoCreate(t *testing.T, client *Client, asset *pipeline.Asset) string {
	t.Helper()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	var out bytes.Buffer
	ctx := context.WithValue(context.Background(), executor.KeyPrinter, &out)
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, end)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, start)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "run")
	ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)

	connections := staticConnections{"duck": client}
	extractor := &query.WholeFileExtractor{
		Fs:       afero.NewMemMapFs(),
		Renderer: jinja.NewRendererWithStartEndDates(&start, &end, &start, "p", "run", nil),
	}
	parser, err := sqlparser.NewSQLParser(false)
	require.NoError(t, err)
	op := autocreate.Wrap(NewBasicOperator(connections, extractor, NewMaterializer(false), parser), connections)

	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{Name: "p"}, Asset: asset}
	require.NoError(t, op.Run(ctx, ti))

	return out.String()
}

func countRows(t *testing.T, client *Client, table string) int64 {
	t.Helper()

	rows, err := client.Select(context.Background(), &query.Query{Query: "SELECT count(*) FROM " + table})
	require.NoError(t, err)
	require.Len(t, rows, 1)

	return rows[0][0].(int64)
}
