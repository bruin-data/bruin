package duck

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
			want: `BEGIN TRANSACTION;
DROP TABLE IF EXISTS my.asset; 
CREATE TABLE my.asset AS SELECT 1;
COMMIT;`,
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
			want: `BEGIN TRANSACTION;
DROP TABLE IF EXISTS my.asset; 
CREATE TABLE my.asset AS SELECT 1;
COMMIT;`,
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
				"DELETE FROM my.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my.asset SELECT \\* FROM __bruin_tmp_.+;\n" +
				"DROP TABLE IF EXISTS __bruin_tmp_.+;\n" +
				"COMMIT;$",
		},

		{
			name: "delete+insert semicolon comment out ",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1 --this is a comment",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1 --this is a comment\n;\n" +
				"DELETE FROM my.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my.asset SELECT \\* FROM __bruin_tmp_.+;\n" +
				"DROP TABLE IF EXISTS __bruin_tmp_.+;\n" +
				"COMMIT;$",
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
			query:   "SELECT 1 as id, 'abc' as name",
			wantErr: true,
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
					{Name: "name", Type: "STRING"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table (\n  id INT64,\n  name STRING\n)",
		},
		{
			name: "table with primary key",
			asset: &pipeline.Asset{
				Name: "my_table_with_pk",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "name", Type: "STRING"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table_with_pk (\n  id INT64,\n  name STRING,\n  PRIMARY KEY (id)\n)",
		},
		{
			name: "table with multiple primary keys",
			asset: &pipeline.Asset{
				Name: "my_table_with_multiple_pks",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "category", Type: "STRING", PrimaryKey: true},
					{Name: "name", Type: "STRING"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table_with_multiple_pks (\n  id INT64,\n  category STRING,\n  name STRING,\n  PRIMARY KEY (id, category)\n)",
		},
		{
			name: "table with column comments",
			asset: &pipeline.Asset{
				Name: "my_table_with_comments",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", Description: "Identifier for the record"},
					{Name: "name", Type: "STRING", Description: "Name of the person"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table_with_comments (\n  id INT64,\n  name STRING\n);\n" +
				"COMMENT ON COLUMN my_table_with_comments.id IS \"Identifier for the record\";\n" +
				"COMMENT ON COLUMN my_table_with_comments.name IS \"Name of the person\";",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildDDLQuery(tt.asset, "")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
