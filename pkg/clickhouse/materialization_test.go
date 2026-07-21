package clickhouse

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
		want        []string
		wantErr     bool
		fullRefresh bool
	}{
		{
			name:  "no materialization, return raw query",
			task:  &pipeline.Asset{},
			query: "SELECT 1",
			want:  []string{"SELECT 1"},
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
			want:  []string{"CREATE OR REPLACE VIEW my.asset AS\nSELECT 1"},
		},
		{
			name: "materialize to a table, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
				Columns: []pipeline.Column{
					{
						Name:       "id",
						PrimaryKey: true,
					},
				},
			},
			query: "SELECT 1",
			want: []string{
				"CREATE OR REPLACE TABLE my.asset PRIMARY KEY (id) AS SELECT 1",
			},
		},
		{
			name: "materialize to a table, full refresh defaults to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{
						Name:       "id",
						PrimaryKey: true,
					},
				},
			},
			fullRefresh: true,
			query:       "SELECT 1",
			want: []string{
				"CREATE OR REPLACE TABLE my.asset PRIMARY KEY (id) AS SELECT 1",
			},
		},
		{
			name: "materialize to a table, full refresh with composite primary key falls back to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "dt", Type: "date", PrimaryKey: true},
					{Name: "name", Type: "string"},
				},
			},
			fullRefresh: true,
			query:       "SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
			want: []string{
				"CREATE OR REPLACE TABLE my.asset PRIMARY KEY (id, dt) AS SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
			},
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
			want:  []string{"INSERT INTO my.asset SELECT 1"},
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
				Columns: []pipeline.Column{
					{
						Name:       "id",
						PrimaryKey: true,
					},
				},
			},
			query: "SELECT 1",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi PRIMARY KEY id AS SELECT 1",
				"DELETE FROM my.asset WHERE dt in (SELECT DISTINCT dt FROM my.__bruin_tmp_abcefghi)",
				"INSERT INTO my.asset SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
			},
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
			name: "merge without primary key errors",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int"},
					{Name: "name", Type: "string"},
				},
			},
			query:   "SELECT 1 as id, 'a' as name",
			wantErr: true,
		},
		{
			name: "merge with primary key builds delete+insert on primary key",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "name", Type: "string"},
				},
			},
			query: "SELECT 1 as id, 'a' as name",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY (id) AS SELECT 1 as id, 'a' as name",
				"INSERT INTO my.asset SELECT * FROM my.__bruin_tmp_abcefghi LIMIT 0",
				"DELETE FROM my.asset WHERE id IN (SELECT id FROM my.__bruin_tmp_abcefghi)",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
			},
		},
		{
			name: "merge with composite primary key builds delete on all keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "dt", Type: "date", PrimaryKey: true},
					{Name: "name", Type: "string"},
				},
			},
			query: "SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY (id, dt) AS SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
				"INSERT INTO my.asset SELECT * FROM my.__bruin_tmp_abcefghi LIMIT 0",
				"DELETE FROM my.asset WHERE (id, dt) IN (SELECT id, dt FROM my.__bruin_tmp_abcefghi)",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
			},
		},
		{
			name: "merge with incremental_predicate appends to delete condition",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:                 pipeline.MaterializationTypeTable,
					Strategy:             pipeline.MaterializationStrategyMerge,
					IncrementalPredicate: "dt >= '2026-01-01'",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
				},
			},
			query: "SELECT 1 as id",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY (id) AS SELECT 1 as id",
				"INSERT INTO my.asset SELECT * FROM my.__bruin_tmp_abcefghi LIMIT 0",
				"DELETE FROM my.asset WHERE id IN (SELECT id FROM my.__bruin_tmp_abcefghi) AND (dt >= '2026-01-01')",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
			},
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
			want: []string{
				"DELETE FROM my.asset WHERE ts BETWEEN '{{ start_timestamp | date_format('%Y-%m-%dT%H:%M:%S.%f') }}' AND '{{ end_timestamp | date_format('%Y-%m-%dT%H:%M:%S.%f') }}'",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT ts, event_name from source_table where ts between '{{start_timestamp}}' AND '{{end_timestamp}}'",
			},
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
			want: []string{
				"DELETE FROM my.asset WHERE dt BETWEEN '{{start_date}}' AND '{{end_date}}'",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT dt, event_name from source_table where dt between '{{start_date}}' and '{{end_date}}'",
			},
		},
		{
			name: "empty table",
			task: &pipeline.Asset{
				Name: "empty_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS empty_table (\n" +
					"\n" +
					")",
			},
		},
		{
			name: "table with one column",
			task: &pipeline.Asset{
				Name: "one_col_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS one_col_table (\n" +
					"id INT64\n" +
					")",
			},
		},
		{
			name: "table with two columns",
			task: &pipeline.Asset{
				Name: "two_col_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "name", Type: "STRING", Description: "The name of the person"},
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS two_col_table (\n" +
					"id INT64,\n" +
					"name STRING COMMENT 'The name of the person'\n" +
					")",
			},
		},
		{
			name: "table with primary key",
			task: &pipeline.Asset{
				Name: "my_primary_key_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "category", Type: "STRING", Description: "Category of the item", PrimaryKey: false},
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_primary_key_table (\n" +
					"id INT64,\n" +
					"category STRING COMMENT 'Category of the item'\n" +
					")" +
					"\nPRIMARY KEY (id)",
			},
		},
		{
			name: "table with composite primary key",
			task: &pipeline.Asset{
				Name: "my_composite_primary_key_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "category", Type: "STRING", Description: "Category of the item", PrimaryKey: true},
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_composite_primary_key_table (\n" +
					"id INT64,\n" +
					"category STRING COMMENT 'Category of the item'\n" +
					")\n" +
					"PRIMARY KEY (id, category)",
			},
		},
		{
			name: "table with partitioning",
			task: &pipeline.Asset{
				Name: "my_partitioned_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "timestamp", Type: "TIMESTAMP", Description: "Event timestamp"},
				},
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyDDL,
					PartitionBy: "timestamp",
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_partitioned_table (\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp'\n" +
					")" +
					"\nPRIMARY KEY (id)" +
					"\nPARTITION BY (timestamp)",
			},
		},
		{
			name: "table with composite partition key",
			task: &pipeline.Asset{
				Name: "my_composite_partitioned_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "timestamp", Type: "TIMESTAMP", Description: "Event timestamp"},
					{Name: "location", Type: "STRING"},
				},
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyDDL,
					PartitionBy: "timestamp, location",
				},
			},
			fullRefresh: true,
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_composite_partitioned_table (\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp',\n" +
					"location STRING\n" +
					")" +
					"\nPRIMARY KEY (id)" +
					"\nPARTITION BY (timestamp, location)",
			},
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

			assert.Equal(t, tt.want, render)
		})
	}
}

func intp(i int) *int { return &i }

func TestColumnMetadataDDL(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name:            "orders",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDDL},
		Columns: []pipeline.Column{
			{Name: "amount", Type: "Decimal", Precision: intp(10), Scale: intp(2), Default: "0"},
		},
	}
	parts, err := NewMaterializer(false).Render(asset, "SELECT 1")
	require.NoError(t, err)
	require.NotEmpty(t, parts)
	createTable := parts[len(parts)-1]
	require.Contains(t, createTable, "Decimal(10, 2)")
	require.Contains(t, createTable, "DEFAULT 0")
	require.NotContains(t, createTable, "REFERENCES")
}
