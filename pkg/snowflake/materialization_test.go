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

func TestBuildSCD2Query(t *testing.T) {
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
					{Name: "id"},
					{Name: "event_name"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, event_name, ts from source_table",
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
					{Name: "id", PrimaryKey: true},
					{Name: "_is_current"},
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
					{Name: "id", PrimaryKey: true},
					{Name: "_valid_from"},
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
					{Name: "id", PrimaryKey: true},
					{Name: "_valid_until"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, _valid_until from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_table_exists_with_incremental_key", // dim_input
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "event_name"},
					{Name: "ts", Type: "Date"},
				},
			},
			query: "SELECT id, event_name, ts from source_table",
			want: "MERGE INTO my.asset AS target\n" +
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
				"  WHERE  t1._valid_from < CAST (s1.ts AS TIMESTAMP) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < CAST (source.ts AS TIMESTAMP)\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    target._valid_until = CAST (source.ts AS TIMESTAMP),\n" +
				"    target._is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    target._valid_until = CURRENT_TIMESTAMP(),\n" +
				"    target._is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (id, event_name, ts, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.id, source.event_name, source.ts, CAST(source.ts AS TIMESTAMP), TIMESTAMP('9999-12-31'), TRUE);",
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
					{Name: "id", PrimaryKey: true},
					{Name: "event_type", PrimaryKey: true},
					{Name: "col1"},
					{Name: "col2"},
					{Name: "ts", Type: "DATE"},
				},
			},
			query: "SELECT id, event_type, col1, col2, ts from source_table",
			want: "MERGE INTO my.asset AS target\n" +
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
				"  WHERE  t1._valid_from < CAST (s1.ts AS TIMESTAMP) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target.event_type = source.event_type AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < CAST (source.ts AS TIMESTAMP)\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    target._valid_until = CAST (source.ts AS TIMESTAMP),\n" +
				"    target._is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    target._valid_until = CURRENT_TIMESTAMP(),\n" +
				"    target._is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.id, source.event_type, source.col1, source.col2, source.ts, CAST(source.ts AS TIMESTAMP), TIMESTAMP('9999-12-31'), TRUE);",
		},
		{
			name: "scd2_full_refresh_with_incremental_key", // dim_input
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "event_name"},
					{Name: "ts", Type: "DATE"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			want: "CREATE OR REPLACE TABLE my.asset\n" +
				"CLUSTER BY (_is_current, id) AS\n" +
				"SELECT\n" +
				"  CAST (ts AS TIMESTAMP) AS _valid_from,\n" +
				"  src.*,\n" +
				"  TO_TIMESTAMP('9999-12-31') AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
				"FROM (\n" +
				"SELECT id, event_name, ts from source_table\n" +
				") AS src;",
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
		name    string
		asset   *pipeline.Asset
		query   string
		want    string
		wantErr bool
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
					{Name: "id"},
					{Name: "event_name"},
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
					{Name: "id", PrimaryKey: true},
					{Name: "_is_current"},
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
					{Name: "id", PrimaryKey: true},
					{Name: "_valid_from"},
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
					{Name: "id", PrimaryKey: true},
					{Name: "_valid_until"},
				},
			},
			query:   "SELECT id, _valid_until from source_table",
			wantErr: true,
		},
		{
			name: "scd2_no_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "col1"},
					{Name: "col2"},
					{Name: "col3"},
					{Name: "col4"},
				},
			},
			query: "SELECT id, col1, col2, col3, col4 from source_table",
			want: "MERGE INTO my.asset AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, col1, col2, col3, col4 from source_table\n" +
				"  )\n" +
				"  SELECT *, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM   s1\n" +
				"  JOIN   my.asset AS t1 USING (id)\n" +
				"  WHERE  t1.col1 != s1.col1 OR t1.col2 != s1.col2 OR t1.col3 != s1.col3 OR t1.col4 != s1.col4\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target._is_current = source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"    target.col1 != source.col1 OR target.col2 != source.col2 OR target.col3 != source.col3 OR target.col4 != source.col4\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    target._valid_until = CURRENT_TIMESTAMP(),\n" +
				"    target._is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    target._valid_until = CURRENT_TIMESTAMP(),\n" +
				"    target._is_current  = FALSE\n" +
				"\n\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (id, col1, col2, col3, col4, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.id, source.col1, source.col2, source.col3, source.col4, CURRENT_TIMESTAMP(), TIMESTAMP('9999-12-31'), TRUE);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildSCD2ByColumnQuery(tt.asset, tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, strings.TrimSpace(tt.want), got)
			}
		})
	}
}
