package vertica

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
			want:  "^CREATE OR REPLACE VIEW my\\.asset AS\nSELECT 1$",
		},
		{
			name: "materialize to a table, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 1",
			want: "^DROP TABLE IF EXISTS my\\.asset CASCADE;\n" +
				"CREATE TABLE my\\.asset AS \\(SELECT 1\\);$",
		},
		{
			name: "materialize to a table, full refresh defaults to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			fullRefresh: true,
			query:       "SELECT 1",
			want: "^DROP TABLE IF EXISTS my\\.asset CASCADE;\n" +
				"CREATE TABLE my\\.asset AS \\(SELECT 1\\);$",
		},
		{
			name: "materialize to a table with cluster is unsupported",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyCreateReplace,
					ClusterBy: []string{"event_type"},
				},
			},
			query:   "SELECT 1",
			wantErr: true,
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
			name: "delete+insert builds a proper query with temp table",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1",
			want: "^CREATE LOCAL TEMPORARY TABLE __bruin_tmp_.+ ON COMMIT PRESERVE ROWS AS \\(SELECT 1\\);\n" +
				"BEGIN TRANSACTION;\n" +
				"DELETE FROM my\\.asset WHERE dt IN \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp_.+;\n" +
				"COMMIT;\n" +
				"DROP TABLE IF EXISTS __bruin_tmp_.+;$",
		},
		{
			name: "merge without columns",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{},
			},
			query:   "SELECT 1 as id",
			wantErr: true,
		},
		{
			name: "merge without primary keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int"},
				},
			},
			query:   "SELECT 1 as id",
			wantErr: true,
		},
		{
			name: "merge with primary keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "name", Type: "varchar", PrimaryKey: false, UpdateOnMerge: true},
				},
			},
			query: "SELECT 1 as id, 'abc' as name",
			want: "^MERGE INTO my\\.asset target\n" +
				"USING \\(SELECT 1 as id, 'abc' as name\\) source ON target\\.id = source.id\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.name = source\\.name\n" +
				"WHEN NOT MATCHED THEN INSERT\\(id, name\\) VALUES\\(source\\.id, source\\.name\\);$",
		},
		{
			name: "merge with merge_sql custom expressions",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "pk", PrimaryKey: true},
					{Name: "col1", MergeSQL: "min(target.col1, source.col1)"},
					{Name: "col2", MergeSQL: "target.col1 - source.col1"},
					{Name: "col3", UpdateOnMerge: true},
					{Name: "col4"},
				},
			},
			query: "SELECT pk, col1, col2, col3, col4 from input_table",
			want: "^MERGE INTO my\\.asset target\n" +
				"USING \\(SELECT pk, col1, col2, col3, col4 from input_table\\) source ON target\\.pk = source.pk\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.col1 = min\\(target\\.col1, source\\.col1\\), target\\.col2 = target\\.col1 - source\\.col1, target\\.col3 = source\\.col3\n" +
				"WHEN NOT MATCHED THEN INSERT\\(pk, col1, col2, col3, col4\\) VALUES\\(source\\.pk, source\\.col1, source\\.col2, source\\.col3, source\\.col4\\);$",
		},
		{
			name: "merge with only merge_sql no update_on_merge",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "value", MergeSQL: "GREATEST(target.value, source.value)"},
					{Name: "count", MergeSQL: "target.count + source.count"},
					{Name: "status"},
				},
			},
			query: "SELECT id, value, count, status FROM source",
			want: "^MERGE INTO my\\.asset target\n" +
				"USING \\(SELECT id, value, count, status FROM source\\) source ON target\\.id = source.id\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.value = GREATEST\\(target\\.value, source\\.value\\), target\\.count = target\\.count \\+ source\\.count\n" +
				"WHEN NOT MATCHED THEN INSERT\\(id, value, count, status\\) VALUES\\(source\\.id, source\\.value, source\\.count, source\\.status\\);$",
		},
		{
			name: "merge with both merge_sql and update_on_merge prioritizes merge_sql",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "col1", MergeSQL: "COALESCE(source.col1, target.col1)", UpdateOnMerge: true},
					{Name: "col2", UpdateOnMerge: true},
					{Name: "col3"},
				},
			},
			query: "SELECT id, col1, col2, col3 FROM source",
			want: "^MERGE INTO my\\.asset target\n" +
				"USING \\(SELECT id, col1, col2, col3 FROM source\\) source ON target\\.id = source.id\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.col1 = COALESCE\\(source\\.col1, target\\.col1\\), target\\.col2 = source\\.col2\n" +
				"WHEN NOT MATCHED THEN INSERT\\(id, col1, col2, col3\\) VALUES\\(source\\.id, source\\.col1, source\\.col2, source\\.col3\\);$",
		},
		{
			name: "time_interval_no_incremental_key",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "time_interval_timestamp_granularity",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
					IncrementalKey:  "ts",
				},
			},
			query: "SELECT ts, event_name from source_table where ts between '{{start_timestamp}}' AND '{{end_timestamp}}'",
			want: "^BEGIN TRANSACTION;\n" +
				"DELETE FROM my\\.asset WHERE ts BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}';\n" +
				"INSERT INTO my\\.asset SELECT ts, event_name from source_table where ts between '{{start_timestamp}}' AND '{{end_timestamp}}';\n" +
				"COMMIT;$",
		},
		{
			name: "time_interval_date",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					TimeGranularity: pipeline.MaterializationTimeGranularityDate,
					IncrementalKey:  "dt",
				},
			},
			query: "SELECT dt, event_name from source_table where dt between '{{start_date}}' and '{{end_date}}'",
			want: "^BEGIN TRANSACTION;\n" +
				"DELETE FROM my\\.asset WHERE dt BETWEEN '{{start_date}}' AND '{{end_date}}';\n" +
				"INSERT INTO my\\.asset SELECT dt, event_name from source_table where dt between '{{start_date}}' and '{{end_date}}';\n" +
				"COMMIT;$",
		},
		{
			name: "truncate+insert",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT 1",
			want:  "BEGIN TRANSACTION;\nTRUNCATE TABLE my.asset;\nINSERT INTO my.asset SELECT 1;\nCOMMIT;",
		},
		{
			name: "view with append strategy is unsupported",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
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
