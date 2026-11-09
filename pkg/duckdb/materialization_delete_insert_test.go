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

func TestDeleteInsertIncrementalPredicateScopesDelete(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		predicate string
		want      [][]interface{}
	}{
		{
			name: "without predicate",
			want: [][]interface{}{
				{int64(1), "eu", "new"},
				{int64(2), "eu", "old"},
			},
		},
		{
			name:      "qualified predicate",
			predicate: "target.region = 'eu'",
			want: [][]interface{}{
				{int64(1), "eu", "new"},
				{int64(1), "us", "old"},
				{int64(2), "eu", "old"},
			},
		},
		{
			name:      "unqualified predicate",
			predicate: "region = 'eu'",
			want: [][]interface{}{
				{int64(1), "eu", "new"},
				{int64(1), "us", "old"},
				{int64(2), "eu", "old"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			client, err := NewClient(Config{Path: filepath.Join(t.TempDir(), "delete_insert.duckdb")})
			require.NoError(t, err)
			defer client.Close()

			exec := func(sql string) {
				t.Helper()
				require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{Query: sql}))
			}
			asset := &pipeline.Asset{
				Name: "demo.items",
				Materialization: pipeline.Materialization{
					Type:                 pipeline.MaterializationTypeTable,
					Strategy:             pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey:       "batch_id",
					IncrementalPredicate: tt.predicate,
				},
			}
			incremental, err := buildIncrementalQuery(asset, "SELECT 1 AS batch_id, 'eu' AS region, 'new' AS label")
			require.NoError(t, err)

			exec("CREATE SCHEMA demo")
			exec("CREATE TABLE demo.items (batch_id BIGINT, region VARCHAR, label VARCHAR)")
			exec("INSERT INTO demo.items VALUES (1, 'eu', 'old'), (1, 'us', 'old'), (2, 'eu', 'old')")
			exec(incremental)

			got, err := client.Select(ctx, &query.Query{Query: "SELECT batch_id, region, label FROM demo.items ORDER BY ALL"})
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
