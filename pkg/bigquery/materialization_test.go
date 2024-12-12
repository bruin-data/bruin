package bigquery

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializer_Render(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		task        *pipeline.Asset
		query       string
		want        string
		wantErr     bool
		fullRefresh bool
	}{
		{
			name:  "no materialization, return raw query",
			task:  &pipeline.Asset{},
			query: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name: "materialize to a view",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeView,
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE VIEW my.asset AS\nSELECT 1",
		},
		{
			name: "materialize to a table, no partition or cluster, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset   AS\nSELECT 1",
		},
		{
			name: "materialize to a table, no partition or cluster, full refresh results in create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			fullRefresh: true,
			query:       "SELECT 1",
			want:        "CREATE OR REPLACE TABLE my.asset   AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition, no cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset PARTITION BY dt  AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition and cluster, single field to cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
					ClusterBy:   []string{"event_type"},
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset PARTITION BY dt CLUSTER BY event_type AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition and cluster, multiple fields to cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
					ClusterBy:   []string{"event_type", "event_name"},
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset PARTITION BY dt CLUSTER BY event_type, event_name AS\nSELECT 1",
		},
		{
			name: "materialize to a table with append",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT 1",
			want:  "INSERT INTO my.asset SELECT 1",
		},
		{
			name: "incremental strategies require the incremental_key to be set",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "incremental strategies require the incremental_key to be set",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "delete+insert builds a proper transaction",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1\n;\n" +
				"DELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "delete+insert builds a proper transaction where columns are defined",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "somekey",
				},
				Columns: []pipeline.Column{
					{Name: "somekey", Type: "date"},
				},
			},
			query: "SELECT 1",
			want: "^DECLARE distinct_keys.+ array<date>;\n" +
				"BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1\n;\n" +
				"SET distinct_keys_.+ = \\(SELECT array_agg\\(distinct somekey\\) FROM __bruin_tmp_.+\\);\n" +
				"DELETE FROM my\\.asset WHERE somekey in unnest\\(distinct_keys.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "delete+insert comment out",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1\n -- This is a comment",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1\n -- This is a comment\n;\n" +
				"DELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "merge with no columns defined fails",
			task: &pipeline.Asset{
				Name:    "my.asset",
				Columns: []pipeline.Column{},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "merge with no primary key fails",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "dt"},
					{Name: "event_type"},
					{Name: "value"},
					{Name: "value2"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "merge with no columns to update",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "dt", PrimaryKey: true},
					{Name: "event_type", PrimaryKey: true},
					{Name: "value"},
					{Name: "value2"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query: "SELECT 1",
			want: "MERGE my\\.asset target\n" +
				"USING \\(SELECT 1\\) source ON target\\.dt = source.dt AND target\\.event_type = source\\.event_type\n" +
				"\n" +
				"WHEN NOT MATCHED THEN INSERT\\(dt, event_type, value, value2\\) VALUES\\(dt, event_type, value, value2\\);",
		},
		{
			name: "merge with some columns to update",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "dt", PrimaryKey: true},
					{Name: "event_type", PrimaryKey: true},
					{Name: "value", UpdateOnMerge: true},
					{Name: "value2"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query: "SELECT 1;",
			want: "MERGE my\\.asset target\n" +
				"USING \\(SELECT 1\\) source ON target\\.dt = source\\.dt AND target\\.event_type = source\\.event_type\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.value = source\\.value\n" +
				"WHEN NOT MATCHED THEN INSERT\\(dt, event_type, value, value2\\) VALUES\\(dt, event_type, value, value2\\);",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.task, tt.query)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Regexp(t, tt.want, render)
		})
	}
}
