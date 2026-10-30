//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestSCD2RerunKeepsExpiredRows(t *testing.T) {
	t.Parallel()

	const source = "SELECT id, label, updated_at FROM demo.src"
	for _, tt := range []struct {
		strategy    pipeline.MaterializationStrategy
		fullRefresh func(*pipeline.Asset, string) (string, error)
		incremental func(*pipeline.Asset, string) (string, error)
	}{
		{pipeline.MaterializationStrategySCD2ByColumn, buildSCD2ByColumnfullRefresh, buildSCD2ByColumnQuery},
		{pipeline.MaterializationStrategySCD2ByTime, buildSCD2ByTimefullRefresh, buildSCD2ByTimeQuery},
	} {
		t.Run(string(tt.strategy), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, err := NewClient(Config{Path: filepath.Join(t.TempDir(), "scd2.duckdb")})
			require.NoError(t, err)
			defer client.Close()

			exec := func(sql string) {
				t.Helper()
				require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{Query: sql}))
			}
			asset := &pipeline.Asset{
				Name: "demo.items",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       tt.strategy,
					IncrementalKey: "updated_at",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "label", Type: "VARCHAR"},
					{Name: "updated_at", Type: "TIMESTAMP"},
				},
			}
			fullRefresh, err := tt.fullRefresh(asset, source)
			require.NoError(t, err)
			incremental, err := tt.incremental(asset, source)
			require.NoError(t, err)

			exec("CREATE SCHEMA demo")
			exec("CREATE TABLE demo.src AS SELECT * FROM (VALUES (1, 'a', TIMESTAMP '2026-01-01'), (2, 'b', TIMESTAMP '2026-01-01'), (3, 'c', TIMESTAMP '2026-01-01')) t(id, label, updated_at)")
			exec(fullRefresh)
			exec("UPDATE demo.src SET label = 'a2', updated_at = TIMESTAMP '2026-01-02' WHERE id = 1")
			exec("DELETE FROM demo.src WHERE id = 3")
			exec(incremental)

			closed := &query.Query{Query: "SELECT id, CAST(_valid_until AS VARCHAR) FROM demo.items WHERE NOT _is_current ORDER BY id"}
			before, err := client.Select(ctx, closed)
			require.NoError(t, err)
			require.Len(t, before, 2)

			exec(incremental)
			after, err := client.Select(ctx, closed)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
