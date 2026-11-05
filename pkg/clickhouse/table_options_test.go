package clickhouse

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializer_IndependentPrimaryKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		primaryKey []string
		orderBy    []string
		clusterBy  []string
		want       string
		wantErr    string
	}{
		{
			name: "physical key overrides logical key", primaryKey: []string{"account_id"},
			orderBy: []string{"account_id", "id"}, want: "PRIMARY KEY (account_id) ORDER BY (account_id, id)",
		},
		{
			name: "primary key without explicit sorting key", primaryKey: []string{"account_id"},
			want: "PRIMARY KEY (account_id)",
		},
		{
			name: "cluster by fallback", primaryKey: []string{"account_id"},
			clusterBy: []string{"account_id", "id"}, want: "PRIMARY KEY (account_id) ORDER BY (account_id, id)",
		},
		{
			name: "expression prefix", primaryKey: []string{"toDate(ts)"},
			orderBy: []string{"toDate(ts)", "id"}, want: "PRIMARY KEY (toDate(ts)) ORDER BY (toDate(ts), id)",
		},
		{
			name: "quoted prefix", primaryKey: []string{"`account_id`"},
			orderBy: []string{"account_id", "id"}, want: "PRIMARY KEY (`account_id`) ORDER BY (account_id, id)",
		},
		{
			name: "nonprefix rejected", primaryKey: []string{"id"}, orderBy: []string{"account_id", "id"},
			wantErr: "primary key columns must be a prefix of clickhouse.order_by",
		},
		{
			name: "longer primary key rejected", primaryKey: []string{"account_id", "id"}, orderBy: []string{"account_id"},
			wantErr: "primary key columns must be a prefix of clickhouse.order_by",
		},
		{
			name: "blank primary key rejected", primaryKey: []string{" "},
			wantErr: "clickhouse.primary_key[0] must not be empty",
		},
	} {
		for _, strategy := range []pipeline.MaterializationStrategy{
			pipeline.MaterializationStrategyCreateReplace, pipeline.MaterializationStrategyDDL,
		} {
			t.Run(tc.name+"/"+string(strategy), func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name: "events", Type: pipeline.AssetTypeClickHouse,
					Columns:         []pipeline.Column{{Name: "id", Type: "UInt64", PrimaryKey: true}, {Name: "account_id", Type: "UInt64"}},
					Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy, ClusterBy: tc.clusterBy},
					ClickHouse:      pipeline.ClickHouseConfig{PrimaryKey: tc.primaryKey, OrderBy: tc.orderBy},
				}
				queries, err := NewMaterializer(false).Render(asset, "SELECT id, account_id FROM source")
				if tc.wantErr != "" {
					require.ErrorContains(t, err, tc.wantErr)
					return
				}
				require.NoError(t, err)
				require.Len(t, queries, 1)
				assert.Contains(t, strings.ReplaceAll(queries[0], "\n", " "), tc.want)
				assert.Equal(t, []string{"id"}, asset.ColumnNamesWithPrimaryKey())
			})
		}
	}
}

func TestMaterializer_PrimaryKeyWithoutColumnMetadata(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name:            "events",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
		ClickHouse:      pipeline.ClickHouseConfig{PrimaryKey: []string{"id"}},
	}
	queries, err := NewMaterializer(false).Render(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	assert.Equal(t, []string{"CREATE OR REPLACE TABLE events PRIMARY KEY (id) AS SELECT 1 AS id"}, queries)
	asset.ClickHouse.Engine = "ReplicatedMergeTree()"
	queries, err = NewMaterializer(false, "analytics").Render(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	assert.Contains(t, queries[1], "PRIMARY KEY (id)")
}

func TestMaterializer_LogicalKeysRemainIndependent(t *testing.T) {
	t.Parallel()
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyMerge, pipeline.MaterializationStrategyDeleteInsert,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name:            "events",
				Columns:         []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "account_id"}},
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy, IncrementalKey: "id"},
				ClickHouse: pipeline.ClickHouseConfig{
					PrimaryKey: []string{"account_id"}, OrderBy: []string{"account_id", "id"},
					Indexes:     []pipeline.ClickHouseIndex{{Name: "idx", Expression: "id", Type: "minmax"}},
					Projections: []pipeline.ClickHouseProjection{{Name: "p", Select: "SELECT id ORDER BY id"}},
				},
			}
			queries, err := NewMaterializer(false).Render(asset, "SELECT id, account_id FROM source")
			require.NoError(t, err)
			assert.Contains(t, queries[0], "PRIMARY KEY")
			assert.NotContains(t, queries[0], "PRIMARY KEY (account_id)")
			for _, query := range queries {
				assert.NotContains(t, query, "INDEX")
				assert.NotContains(t, query, "PROJECTION")
			}
			asset.Columns[0].PrimaryKey = false
			_, err = NewMaterializer(false).Render(asset, "SELECT id, account_id FROM source")
			require.ErrorContains(t, err, "primary_key")
		})
	}
}

func TestMaterializer_ReplicatedDeclarationPaths(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name:            "events",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
		ClickHouse: pipeline.ClickHouseConfig{
			Engine: "ReplicatedMergeTree('/tables/{table}', '{replica}')", OrderBy: []string{"id"},
			Indexes: []pipeline.ClickHouseIndex{{Name: "idx", Expression: "id", Type: "minmax"}},
		},
	}
	queries, err := NewMaterializer(false).Render(asset, "SELECT 1 AS id")
	require.ErrorContains(t, err, "use a {uuid} path")
	assert.Empty(t, queries)
	_, err = NewMaterializer(false, "analytics").Render(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	asset.ClickHouse.Engine = "ReplicatedMergeTree('/tables/{uuid}', '{replica}')"
	queries, err = NewMaterializer(false).Render(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	assert.Contains(t, queries[0], "ReplicatedMergeTree('/tables/{uuid}', '{replica}')")
}
