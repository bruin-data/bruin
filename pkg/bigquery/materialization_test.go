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
			name: "delete+insert with empty column type",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "somekey", Type: ""},
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
			name: "delete+insert with UNKNOWN column type",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "somekey", Type: "UNKNOWN"},
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
			name: "time_interval_timestampgranularity",
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
				"COMMIT TRANSACTION;$",
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
				"COMMIT TRANSACTION;$",
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

func TestBuildDDLQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		asset   *pipeline.Asset
		want    string
		wantErr bool
	}{
		{
			name: "basic table creation",
			asset: &pipeline.Asset{
				Name: "my_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "name", Type: "STRING", Description: "The name of the person"},
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table (\n  id INT64,\n  name STRING OPTIONS(description=\"The name of the person\")\n)",
		},
		{
			name: "table with partitioning",
			asset: &pipeline.Asset{
				Name: "my_partitioned_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "timestamp", Type: "TIMESTAMP", Description: "Event timestamp"},
				},
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					PartitionBy: "timestamp",
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_partitioned_table (\n  id INT64,\n  timestamp TIMESTAMP OPTIONS(description=\"Event timestamp\")\n)\nPARTITION BY timestamp",
		},
		{
			name: "table with clustering",
			asset: &pipeline.Asset{
				Name: "my_clustered_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "category", Type: "STRING", Description: "Category of the item"},
				},
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					ClusterBy: []string{"category"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_clustered_table (\n  id INT64,\n  category STRING OPTIONS(description=\"Category of the item\")\n)\nCLUSTER BY category",
		},
		{
			name: "table with partitioning and clustering",
			asset: &pipeline.Asset{
				Name: "my_partitioned_clustered_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "timestamp", Type: "TIMESTAMP", Description: "Event timestamp"},
					{Name: "category", Type: "STRING", Description: "Category of the item"},
				},
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					PartitionBy: "timestamp",
					ClusterBy:   []string{"category"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_partitioned_clustered_table (\n  id INT64,\n  timestamp TIMESTAMP OPTIONS(description=\"Event timestamp\"),\n  category STRING OPTIONS(description=\"Category of the item\")\n)\nPARTITION BY timestamp\nCLUSTER BY category",
		},
		{
			name: "table with primary key",
			asset: &pipeline.Asset{
				Name: "my_table_with_pk",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "name", Type: "STRING", Description: "The name of the person"},
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table_with_pk (\n  id INT64,\n  name STRING OPTIONS(description=\"The name of the person\"),\n  PRIMARY KEY (id) NOT ENFORCED\n)",
		},
		{
			name: "table with multiple primary keys",
			asset: &pipeline.Asset{
				Name: "my_table_with_multiple_pks",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "category", Type: "STRING", PrimaryKey: true},
					{Name: "name", Type: "STRING", Description: "The name of the person"},
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table_with_multiple_pks (\n  id INT64,\n  category STRING,\n  name STRING OPTIONS(description=\"The name of the person\"),\n  PRIMARY KEY (id, category) NOT ENFORCED\n)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := BuildDDLQuery(tt.asset, "")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
