package snowflake

import (
	"strings"
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
			name: "materialize to a table, no partition or cluster, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset  AS\nSELECT 1",
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
			want:        "CREATE OR REPLACE TABLE my.asset  AS\nSELECT 1",
		},
		{
			name: "materialize to a table with cluster, single field to cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyCreateReplace,
					ClusterBy: []string{"event_type"},
				},
			},
			query: "SELECT 1",
			want:  "^CREATE OR REPLACE TABLE my\\.asset CLUSTER BY \\(event_type\\) AS\nSELECT 1$",
		},
		{
			name: "materialize to a table with cluster, multiple fields to cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyCreateReplace,
					ClusterBy: []string{"event_type", "event_name"},
				},
			},
			query: "SELECT 1",
			want:  "^CREATE OR REPLACE TABLE my\\.asset CLUSTER BY \\(event_type, event_name\\) AS\nSELECT 1$",
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
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp_.+;\n" +
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
			query: "SELECT 1 as id, 'abc' as name",
			want: "^MERGE INTO my\\.asset target\n" +
				"USING \\(SELECT 1 as id, 'abc' as name\\) source ON target\\.id = source.id\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.name = source\\.name\n" +
				"WHEN NOT MATCHED THEN INSERT\\(id, name\\) VALUES\\(id, name\\);$",
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
			want: "CREATE TABLE IF NOT EXISTS empty_table \\(\n" +
				"\n" +
				"\\)",
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
			want: "CREATE TABLE IF NOT EXISTS one_col_table \\(\n" +
				"id INT64\n" +
				"\\)",
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
			want: "CREATE TABLE IF NOT EXISTS two_col_table \\(\n" +
				"id INT64,\n" +
				"name STRING COMMENT 'The name of the person'\n" +
				"\\)",
		},
		{
			name: "table with clustering",
			task: &pipeline.Asset{
				Name: "my_clustered_table",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyDDL,
					ClusterBy: []string{"category"},
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "category", Type: "STRING", Description: "Category of the item"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_clustered_table CLUSTER BY \\(category\\) \\(\n" +
				"id INT64,\n" +
				"category STRING COMMENT 'Category of the item'\n" +
				"\\)",
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
			want: "CREATE TABLE IF NOT EXISTS my_primary_key_table \\(\n" +
				"id INT64,\n" +
				"category STRING COMMENT 'Category of the item',\n" +
				"primary key \\(id\\)\n" +
				"\\)",
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
			want: "CREATE TABLE IF NOT EXISTS my_composite_primary_key_table \\(\n" +
				"id INT64,\n" +
				"category STRING COMMENT 'Category of the item',\n" +
				"primary key \\(id, category\\)\n" +
				"\\)",
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
				if !assert.Regexp(t, tt.want, render) {
					t.Logf("\nWant (regex): %s\nGot: %s", tt.want, render)
				}
			}
		})
	}
}

func TestBuildSCD2QueryByTime(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		asset       *pipeline.Asset
		query       string
		want        string
		wantErr     bool
		fullRefresh bool
	}{
		{
			name: "scd2_no_primary_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, event_name, ts from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_no_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByTime,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
				},
			},
			query:       "SELECT id, event_name from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_reserved_column_name_is_current",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "_is_current", Type: "BOOLEAN"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, _is_current from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_reserved_column_name_valid_from",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "_valid_from", Type: "TIMESTAMP"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, _valid_from from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_reserved_column_name_valid_until",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "_valid_until", Type: "TIMESTAMP"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, _valid_until from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_invalid_incremental_key_type",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "VARCHAR"},
				},
			},
			query:       "SELECT id, event_name, ts from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_table_exists_with_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "DATE"},
				},
			},
			query: "SELECT id, event_name, ts from source_table",
			want: "BEGIN TRANSACTION;\n\n" +
				"-- Capture timestamp once for consistency across all operations\n" +
				"SET current_scd2_ts = CURRENT_TIMESTAMP();\n\n" +
				"-- Step 1: Update expired records that are no longer in source\n" +
				"UPDATE my.asset AS target\n" +
				"SET _valid_until = $current_scd2_ts, _is_current = FALSE\n" +
				"WHERE target._is_current = TRUE\n" +
				"  AND NOT EXISTS (\n" +
				"    SELECT 1 FROM (SELECT id, event_name, ts from source_table) AS source \n" +
				"    WHERE target.id = source.id\n" +
				"  );\n\n" +
				"-- Step 2: Handle new and changed records\n" +
				"MERGE INTO my.asset AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, event_name, ts from source_table\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   my.asset AS t1 USING (id)\n" +
				"  WHERE  t1._valid_from < CAST(s1.ts AS TIMESTAMP) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target._is_current AND source._is_current\n\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < CAST(source.ts AS TIMESTAMP)\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    target._valid_until = CAST(source.ts AS TIMESTAMP),\n" +
				"    target._is_current  = FALSE\n\n" +
				"WHEN NOT MATCHED THEN\n" +
				"  INSERT (id, event_name, ts, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.id, source.event_name, source.ts, CAST(source.ts AS TIMESTAMP), TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TRUE);\n\n" +
				"COMMIT;",
		},
		{
			name: "scd2_multiple_primary_keys_with_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_type", PrimaryKey: true, Type: "VARCHAR"},
					{Name: "col1", Type: "VARCHAR"},
					{Name: "col2", Type: "VARCHAR"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query: "SELECT id, event_type, col1, col2, ts from source_table",
			want: "BEGIN TRANSACTION;\n\n" +
				"-- Capture timestamp once for consistency across all operations\n" +
				"SET current_scd2_ts = CURRENT_TIMESTAMP();\n\n" +
				"-- Step 1: Update expired records that are no longer in source\n" +
				"UPDATE my.asset AS target\n" +
				"SET _valid_until = $current_scd2_ts, _is_current = FALSE\n" +
				"WHERE target._is_current = TRUE\n" +
				"  AND NOT EXISTS (\n" +
				"    SELECT 1 FROM (SELECT id, event_type, col1, col2, ts from source_table) AS source \n" +
				"    WHERE target.id = source.id AND target.event_type = source.event_type\n" +
				"  );\n\n" +
				"-- Step 2: Handle new and changed records\n" +
				"MERGE INTO my.asset AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, event_type, col1, col2, ts from source_table\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   my.asset AS t1 USING (id, event_type)\n" +
				"  WHERE  t1._valid_from < CAST(s1.ts AS TIMESTAMP) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target.event_type = source.event_type AND target._is_current AND source._is_current\n\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < CAST(source.ts AS TIMESTAMP)\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    target._valid_until = CAST(source.ts AS TIMESTAMP),\n" +
				"    target._is_current  = FALSE\n\n" +
				"WHEN NOT MATCHED THEN\n" +
				"  INSERT (id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.id, source.event_type, source.col1, source.col2, source.ts, CAST(source.ts AS TIMESTAMP), TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TRUE);\n\n" +
				"COMMIT;",
		},
		{
			name: "scd2_full_refresh_with_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "DATE"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			want: "BEGIN TRANSACTION;\n" +
				"CREATE OR REPLACE TABLE my.asset CLUSTER BY (_is_current, id) (\n" +
				"id INTEGER,\n" +
				"event_name VARCHAR,\n" +
				"ts DATE,\n" +
				"_valid_from TIMESTAMP,\n" +
				"_valid_until TIMESTAMP,\n" +
				"_is_current BOOLEAN\n" +
				");\n" +
				"INSERT INTO my.asset (id, event_name, ts, _valid_from, _valid_until, _is_current)\n" +
				"SELECT\n" +
				"  src.id,\n" +
				"  src.event_name,\n" +
				"  src.ts,\n" +
				"  CAST(src.ts AS TIMESTAMP),\n" +
				"  TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),\n" +
				"  TRUE\n" +
				"FROM (\n" +
				"SELECT id, event_name, ts from source_table\n" +
				") AS src;\n" +
				"COMMIT;",
		},
		{
			name: "scd2_full_refresh_with_custom_clustering",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
					ClusterBy:      []string{"event_type", "id"},
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "DATE"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			want: "BEGIN TRANSACTION;\n" +
				"CREATE OR REPLACE TABLE my.asset CLUSTER BY (event_type, id) (\n" +
				"id INTEGER,\n" +
				"event_name VARCHAR,\n" +
				"ts DATE,\n" +
				"_valid_from TIMESTAMP,\n" +
				"_valid_until TIMESTAMP,\n" +
				"_is_current BOOLEAN\n" +
				");\n" +
				"INSERT INTO my.asset (id, event_name, ts, _valid_from, _valid_until, _is_current)\n" +
				"SELECT\n" +
				"  src.id,\n" +
				"  src.event_name,\n" +
				"  src.ts,\n" +
				"  CAST(src.ts AS TIMESTAMP),\n" +
				"  TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),\n" +
				"  TRUE\n" +
				"FROM (\n" +
				"SELECT id, event_name, ts from source_table\n" +
				") AS src;\n" +
				"COMMIT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.asset, tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, strings.TrimSpace(tt.want), render)
			}
		})
	}
}

func TestBuildSCD2ByColumnQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		asset       *pipeline.Asset
		query       string
		want        string
		wantErr     bool
		fullRefresh bool
	}{
		{
			name: "scd2_no_primary_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "date"},
				},
			},
			query:   "SELECT id, event_name, ts from source_table",
			wantErr: true,
		},
		{
			name: "scd2_reserved_column_name_is_current",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "_is_current", Type: "BOOLEAN"},
				},
			},
			query:   "SELECT id, _is_current from source_table",
			wantErr: true,
		},
		{
			name: "scd2_reserved_column_name_valid_from",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "_valid_from", Type: "TIMESTAMP"},
				},
			},
			query:   "SELECT id, _valid_from from source_table",
			wantErr: true,
		},
		{
			name: "scd2_reserved_column_name_valid_until",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "_valid_until", Type: "TIMESTAMP"},
				},
			},
			query:   "SELECT id, _valid_until from source_table",
			wantErr: true,
		},
		{
			name: "scd2_basic_column_change_detection",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "col1", Type: "VARCHAR"},
					{Name: "col2", Type: "VARCHAR"},
					{Name: "col3", Type: "VARCHAR"},
					{Name: "col4", Type: "VARCHAR"},
				},
			},
			query: "SELECT id, col1, col2, col3, col4 from source_table",
			want: "BEGIN TRANSACTION;\n\n" +
				"-- Capture timestamp once for consistency across all operations\n" +
				"SET current_scd2_ts = CURRENT_TIMESTAMP();\n\n" +
				"-- Step 1: Update expired records that are no longer in source\n" +
				"UPDATE my.asset AS target\n" +
				"SET _valid_until = $current_scd2_ts, _is_current = FALSE\n" +
				"WHERE target._is_current = TRUE\n" +
				"  AND NOT EXISTS (\n" +
				"    SELECT 1 FROM (SELECT id, col1, col2, col3, col4 from source_table) AS source \n" +
				"    WHERE target.id = source.id\n" +
				"  );\n\n" +
				"-- Step 2: Update existing records that have changes\n" +
				"UPDATE my.asset AS target\n" +
				"SET _valid_until = $current_scd2_ts, _is_current = FALSE\n" +
				"WHERE target._is_current = TRUE\n" +
				"  AND EXISTS (\n" +
				"    SELECT 1 FROM (SELECT id, col1, col2, col3, col4 from source_table) AS source\n" +
				"    WHERE target.id = source.id AND (target.col1 != source.col1 OR target.col2 != source.col2 OR target.col3 != source.col3 OR target.col4 != source.col4)\n" +
				"  );\n\n" +
				"-- Step 3: Insert new records and new versions of changed records\n" +
				"INSERT INTO my.asset (id, col1, col2, col3, col4, _valid_from, _valid_until, _is_current)\n" +
				"SELECT source.id, source.col1, source.col2, source.col3, source.col4, $current_scd2_ts, TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TRUE\n" +
				"FROM (SELECT id, col1, col2, col3, col4 from source_table) AS source\n" +
				"WHERE NOT EXISTS (\n" +
				"  SELECT 1 FROM my.asset AS target \n" +
				"  WHERE target.id = source.id AND target._is_current = TRUE\n" +
				")\n" +
				"OR EXISTS (\n" +
				"  SELECT 1 FROM my.asset AS target\n" +
				"  WHERE target.id = source.id AND target._is_current = FALSE AND target._valid_until = $current_scd2_ts\n" +
				");\n\n" +
				"COMMIT;",
		},
		{
			name: "scd2_multiple_primary_keys",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "category", PrimaryKey: true, Type: "VARCHAR"},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "DECIMAL"},
				},
			},
			query: "SELECT id, category, name, price from source_table",
			want: "BEGIN TRANSACTION;\n\n" +
				"-- Capture timestamp once for consistency across all operations\n" +
				"SET current_scd2_ts = CURRENT_TIMESTAMP();\n\n" +
				"-- Step 1: Update expired records that are no longer in source\n" +
				"UPDATE my.asset AS target\n" +
				"SET _valid_until = $current_scd2_ts, _is_current = FALSE\n" +
				"WHERE target._is_current = TRUE\n" +
				"  AND NOT EXISTS (\n" +
				"    SELECT 1 FROM (SELECT id, category, name, price from source_table) AS source \n" +
				"    WHERE target.id = source.id AND target.category = source.category\n" +
				"  );\n\n" +
				"-- Step 2: Update existing records that have changes\n" +
				"UPDATE my.asset AS target\n" +
				"SET _valid_until = $current_scd2_ts, _is_current = FALSE\n" +
				"WHERE target._is_current = TRUE\n" +
				"  AND EXISTS (\n" +
				"    SELECT 1 FROM (SELECT id, category, name, price from source_table) AS source\n" +
				"    WHERE target.id = source.id AND target.category = source.category AND (target.name != source.name OR target.price != source.price)\n" +
				"  );\n\n" +
				"-- Step 3: Insert new records and new versions of changed records\n" +
				"INSERT INTO my.asset (id, category, name, price, _valid_from, _valid_until, _is_current)\n" +
				"SELECT source.id, source.category, source.name, source.price, $current_scd2_ts, TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TRUE\n" +
				"FROM (SELECT id, category, name, price from source_table) AS source\n" +
				"WHERE NOT EXISTS (\n" +
				"  SELECT 1 FROM my.asset AS target \n" +
				"  WHERE target.id = source.id AND target.category = source.category AND target._is_current = TRUE\n" +
				")\n" +
				"OR EXISTS (\n" +
				"  SELECT 1 FROM my.asset AS target\n" +
				"  WHERE target.id = source.id AND target.category = source.category AND target._is_current = FALSE AND target._valid_until = $current_scd2_ts\n" +
				");\n\n" +
				"COMMIT;",
		},
		{
			name: "scd2_full_refresh_by_column",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "DECIMAL"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, name, price from source_table",
			want: "BEGIN TRANSACTION;\n" +
				"CREATE OR REPLACE TABLE my.asset CLUSTER BY (_is_current, id) (\n" +
				"_valid_from TIMESTAMP,\n" +
				"id INTEGER,\n" +
				"name VARCHAR,\n" +
				"price DECIMAL,\n" +
				"_valid_until TIMESTAMP,\n" +
				"_is_current BOOLEAN\n" +
				");\n" +
				"INSERT INTO my.asset (_valid_from, id, name, price, _valid_until, _is_current)\n" +
				"SELECT\n" +
				"  CURRENT_TIMESTAMP(),\n" +
				"  src.id,\n" +
				"  src.name,\n" +
				"  src.price,\n" +
				"  TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),\n" +
				"  TRUE\n" +
				"FROM (\n" +
				"SELECT id, name, price from source_table\n" +
				") AS src;\n" +
				"COMMIT;",
		},
		{
			name: "scd2_full_refresh_with_custom_clustering",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategySCD2ByColumn,
					ClusterBy: []string{"category", "id"},
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "DECIMAL"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, name, price from source_table",
			want: "BEGIN TRANSACTION;\n" +
				"CREATE OR REPLACE TABLE my.asset CLUSTER BY (category, id) (\n" +
				"_valid_from TIMESTAMP,\n" +
				"id INTEGER,\n" +
				"name VARCHAR,\n" +
				"price DECIMAL,\n" +
				"_valid_until TIMESTAMP,\n" +
				"_is_current BOOLEAN\n" +
				");\n" +
				"INSERT INTO my.asset (_valid_from, id, name, price, _valid_until, _is_current)\n" +
				"SELECT\n" +
				"  CURRENT_TIMESTAMP(),\n" +
				"  src.id,\n" +
				"  src.name,\n" +
				"  src.price,\n" +
				"  TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),\n" +
				"  TRUE\n" +
				"FROM (\n" +
				"SELECT id, name, price from source_table\n" +
				") AS src;\n" +
				"COMMIT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.asset, tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, strings.TrimSpace(tt.want), render)
			}
		})
	}
}