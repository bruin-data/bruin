package athena

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
			name: "materialize to a table, no partition or cluster, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 1",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1",
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
			},
		},
		{
			name: "materialize to a table, with partition, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					PartitionBy: "some_column",
				},
			},
			query: "SELECT 1 as some_column",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['some_column']) AS SELECT 1 as some_column",
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
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
			},
			fullRefresh: true,
			query:       "SELECT 1",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1",
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
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
			name: "delete+insert",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1",
			want:  []string{"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1\n", "\nDELETE FROM my.asset WHERE dt in (SELECT DISTINCT dt FROM __bruin_tmp_abcefghi)", "INSERT INTO my.asset SELECT * FROM __bruin_tmp_abcefghi", "DROP TABLE IF EXISTS __bruin_tmp_abcefghi"},
		},
		{
			name: "delete+insert semicolon comment out",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1" +
				" --this is a comment",
			want: []string{"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1 --this is a comment\n", "\nDELETE FROM my.asset WHERE dt in (SELECT DISTINCT dt FROM __bruin_tmp_abcefghi)", "INSERT INTO my.asset SELECT * FROM __bruin_tmp_abcefghi", "DROP TABLE IF EXISTS __bruin_tmp_abcefghi"},
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
			want:  []string{"MERGE INTO my.asset target USING (SELECT 1 as id, 'abc' as name) source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name WHEN NOT MATCHED THEN INSERT(id, name) VALUES(source.id, source.name)"},
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
				"DELETE FROM my.asset WHERE ts BETWEEN timestamp '{{start_timestamp}}' AND timestamp '{{end_timestamp}}'",
				"INSERT INTO my.asset SELECT ts, event_name from source_table where ts between '{{start_timestamp}}' AND '{{end_timestamp}}'",
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
				"DELETE FROM my.asset WHERE dt BETWEEN date '{{start_date}}' AND date '{{end_date}}'",
				"INSERT INTO my.asset SELECT dt, event_name from source_table where dt between '{{start_date}}' and '{{end_date}}'",
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
					")\n" +
					"LOCATION 's3://bucket/empty_table'\n" +
					"TBLPROPERTIES('table_type'='ICEBERG')",
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
					")\n" +
					"LOCATION 's3://bucket/one_col_table'\n" +
					"TBLPROPERTIES('table_type'='ICEBERG')",
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
					")\n" +
					"LOCATION 's3://bucket/two_col_table'\n" +
					"TBLPROPERTIES('table_type'='ICEBERG')",
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
			fullRefresh: true,
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_partitioned_table (\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp'\n" +
					")" +
					"\nPARTITIONED BY (timestamp)" +
					"\nLOCATION 's3://bucket/my_partitioned_table'" +
					"\nTBLPROPERTIES('table_type'='ICEBERG')",
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
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_composite_partitioned_table (\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp',\n" +
					"location STRING\n" +
					")" +
					"\nPARTITIONED BY (timestamp, location)\n" +
					"LOCATION 's3://bucket/my_composite_partitioned_table'\n" +
					"TBLPROPERTIES('table_type'='ICEBERG')",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.task, tt.query, "s3://bucket")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, render)
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
		want        []string
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
			name: "scd2_basic_column_change_detection",
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
				},
			},
			query: "SELECT id, col1, col2 from source_table",
			want: []string{
				//nolint:dupword
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS\nWITH\ntime_now AS (\n  SELECT CURRENT_TIMESTAMP AS now\n),\nsource AS (\n  SELECT id, col1, col2 from source_table\n),\ntarget AS (\n  SELECT id, col1, col2, _valid_from, _valid_until, _is_current \t\n  FROM my.asset \n  WHERE _is_current = TRUE\n),\njoined AS (\n  SELECT t.id AS t_id,\n    t.col1 AS t_col1,\n    t.col2 AS t_col2,\n    t._valid_from,\n    t._valid_until,\n    t._is_current,\n    s.id AS s_id,\n    s.col1 AS s_col1,\n    s.col2 AS s_col2\n  FROM target t\n  LEFT JOIN source s ON t.id = s.id\n),\n-- Rows that are unchanged\nunchanged AS (\n  SELECT t_id AS id, t_col1 AS col1, t_col2 AS col2,\n  _valid_from,\n  _valid_until,\n  _is_current\n  FROM joined\n  WHERE s_id IS NOT NULL AND t_col1 = s_col1 AND t_col2 = s_col2\n),\n-- Rows that need to be expired (changed or missing in source)\nto_expire AS (\n  SELECT t_id AS id, t_col1 AS col1, t_col2 AS col2,\n  _valid_from,\n  (SELECT now FROM time_now) AS _valid_until,\n  FALSE AS _is_current\n  FROM joined\n  WHERE s_id IS NULL OR t_col1 != s_col1 OR t_col2 != s_col2\n),\n-- New/changed inserts from source\nto_insert AS (\n  SELECT s.id AS id, s.col1 AS col1, s.col2 AS col2,\n  (SELECT now FROM time_now) AS _valid_from,\n  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n  TRUE AS _is_current\n  FROM source s\n  LEFT JOIN target t ON t.id = s.id\n  WHERE t.id IS NULL OR t.col1 != s.col1 OR t.col2 != s.col2\n),\n-- Already expired historical rows (untouched)\nhistorical AS (\n  SELECT id, col1, col2, _valid_from, _valid_until, _is_current\n  FROM my.asset\n  WHERE _is_current = FALSE\n)\nSELECT id, col1, col2, _valid_from, _valid_until, _is_current FROM unchanged\nUNION ALL\nSELECT id, col1, col2, _valid_from, _valid_until, _is_current FROM to_expire\nUNION ALL\nSELECT id, col1, col2, _valid_from, _valid_until, _is_current FROM to_insert\nUNION ALL\nSELECT id, col1, col2, _valid_from, _valid_until, _is_current FROM historical",
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
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
					{Name: "id", PrimaryKey: true},
					{Name: "category", PrimaryKey: true},
					{Name: "name"},
				},
			},
			query: "SELECT id, category, name from source_table",
			want: []string{
				//nolint:dupword
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS\nWITH\ntime_now AS (\n  SELECT CURRENT_TIMESTAMP AS now\n),\nsource AS (\n  SELECT id, category, name from source_table\n),\ntarget AS (\n  SELECT id, category, name, _valid_from, _valid_until, _is_current \t\n  FROM my.asset \n  WHERE _is_current = TRUE\n),\njoined AS (\n  SELECT t.id AS t_id,\n    t.category AS t_category,\n    t.name AS t_name,\n    t._valid_from,\n    t._valid_until,\n    t._is_current,\n    s.id AS s_id,\n    s.category AS s_category,\n    s.name AS s_name\n  FROM target t\n  LEFT JOIN source s ON t.id = s.id AND t.category = s.category\n),\n-- Rows that are unchanged\nunchanged AS (\n  SELECT t_id AS id, t_category AS category, t_name AS name,\n  _valid_from,\n  _valid_until,\n  _is_current\n  FROM joined\n  WHERE s_id IS NOT NULL AND s_category IS NOT NULL AND t_name = s_name\n),\n-- Rows that need to be expired (changed or missing in source)\nto_expire AS (\n  SELECT t_id AS id, t_category AS category, t_name AS name,\n  _valid_from,\n  (SELECT now FROM time_now) AS _valid_until,\n  FALSE AS _is_current\n  FROM joined\n  WHERE s_id IS NULL AND s_category IS NULL OR t_name != s_name\n),\n-- New/changed inserts from source\nto_insert AS (\n  SELECT s.id AS id, s.category AS category, s.name AS name,\n  (SELECT now FROM time_now) AS _valid_from,\n  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n  TRUE AS _is_current\n  FROM source s\n  LEFT JOIN target t ON t.id = s.id AND t.category = s.category\n  WHERE t.id IS NULL AND t.category IS NULL OR t.name != s.name\n),\n-- Already expired historical rows (untouched)\nhistorical AS (\n  SELECT id, category, name, _valid_from, _valid_until, _is_current\n  FROM my.asset\n  WHERE _is_current = FALSE\n)\nSELECT id, category, name, _valid_from, _valid_until, _is_current FROM unchanged\nUNION ALL\nSELECT id, category, name, _valid_from, _valid_until, _is_current FROM to_expire\nUNION ALL\nSELECT id, category, name, _valid_from, _valid_until, _is_current FROM to_insert\nUNION ALL\nSELECT id, category, name, _valid_from, _valid_until, _is_current FROM historical",
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
		},
		{
			name: "scd2_full_refresh_by_column_with_no_primary_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER"},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "FLOAT"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, name, price from source_table",
			wantErr:     true,
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
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "FLOAT"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, name, price from source_table",
			want: []string{
				`CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS
SELECT
  src.id, src.name, src.price,
  CURRENT_TIMESTAMP AS _valid_from,
  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,
  TRUE AS _is_current
FROM (
SELECT id, name, price from source_table
) AS src`,
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
		},
		{
			name: "scd2_full_refresh_by_column_with_multiple_primary_keys",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "category", Type: "VARCHAR", PrimaryKey: true},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "FLOAT"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, category, name, price from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS\nSELECT\n  src.id, src.category, src.name, src.price,\n  CURRENT_TIMESTAMP AS _valid_from,\n  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, category, name, price from source_table\n) AS src",
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
		},
		{
			name: "scd2_full_refresh_with_custom_partitioning",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategySCD2ByColumn,
					PartitionBy: "category, id",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "name", Type: "VARCHAR"},
					{Name: "price", Type: "FLOAT"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, name, price from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['category, id']) AS\nSELECT\n  src.id, src.name, src.price,\n  CURRENT_TIMESTAMP AS _valid_from,\n  TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, name, price from source_table\n) AS src",
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.asset, tt.query, "s3://bucket")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, render)
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
		want        []string
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
			query:   "SELECT id, event_name, ts from source_table",
			wantErr: true,
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
			query:   "SELECT id, event_name from source_table",
			wantErr: true,
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
			query:   "SELECT id, _is_current from source_table",
			wantErr: true,
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
			query:   "SELECT id, _valid_from from source_table",
			wantErr: true,
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
			query:   "SELECT id, _valid_until from source_table",
			wantErr: true,
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
			query:   "SELECT id, event_name, ts from source_table",
			wantErr: true,
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
			want: []string{
				`CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS
WITH
source AS (
  SELECT id, event_name, ts from source_table
),
current_data AS (
  SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM my.asset WHERE _is_current = TRUE
),
historical_data AS (
  SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM my.asset WHERE _is_current = FALSE
),
t_new AS (
  SELECT 
    t.id,
    t.event_name,
    t.ts,
    t._valid_from,
    CASE WHEN s.id IS NULL OR (s.ts IS NOT NULL AND CAST(s.ts AS TIMESTAMP) > t._valid_from)
	THEN CAST(s.ts AS TIMESTAMP) 
	ELSE t._valid_until 
	END AS _valid_until,
    CASE WHEN s.id IS NULL OR (s.ts IS NOT NULL AND CAST(s.ts AS TIMESTAMP) > t._valid_from)
	THEN FALSE 
	ELSE t._is_current 
	END AS _is_current
  FROM current_data t
  LEFT JOIN source s ON t.id = s.id
),
insert_rows AS (
  SELECT 
    s.id,
    s.event_name,
    s.ts,
    CAST(s.ts AS TIMESTAMP) AS _valid_from,
    TIMESTAMP '9999-12-31' AS _valid_until,
    TRUE AS _is_current
  FROM source s
  LEFT JOIN current_data t ON t.id = s.id
  WHERE t.id IS NULL OR (t.id = s.id AND CAST(s.ts AS TIMESTAMP) > t._valid_from)
)
SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM t_new
UNION ALL
SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM insert_rows
UNION ALL
SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM historical_data`,
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
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
			want: []string{
				`CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS
WITH
source AS (
  SELECT id, event_type, col1, col2, ts from source_table
),
current_data AS (
  SELECT id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current FROM my.asset WHERE _is_current = TRUE
),
historical_data AS (
  SELECT id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current FROM my.asset WHERE _is_current = FALSE
),
t_new AS (
  SELECT 
    t.id,
    t.event_type,
    t.col1,
    t.col2,
    t.ts,
    t._valid_from,
    CASE WHEN s.id IS NULL AND s.event_type IS NULL OR (s.ts IS NOT NULL AND CAST(s.ts AS TIMESTAMP) > t._valid_from)
	THEN CAST(s.ts AS TIMESTAMP) 
	ELSE t._valid_until 
	END AS _valid_until,
    CASE WHEN s.id IS NULL AND s.event_type IS NULL OR (s.ts IS NOT NULL AND CAST(s.ts AS TIMESTAMP) > t._valid_from)
	THEN FALSE 
	ELSE t._is_current 
	END AS _is_current
  FROM current_data t
  LEFT JOIN source s ON t.id = s.id AND t.event_type = s.event_type
),
insert_rows AS (
  SELECT 
    s.id,
    s.event_type,
    s.col1,
    s.col2,
    s.ts,
    CAST(s.ts AS TIMESTAMP) AS _valid_from,
    TIMESTAMP '9999-12-31' AS _valid_until,
    TRUE AS _is_current
  FROM source s
  LEFT JOIN current_data t ON t.id = s.id AND t.event_type = s.event_type
  WHERE t.id IS NULL AND t.event_type IS NULL OR (t.id = s.id AND t.event_type = s.event_type AND CAST(s.ts AS TIMESTAMP) > t._valid_from)
)
SELECT id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current FROM t_new
UNION ALL
SELECT id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current FROM insert_rows
UNION ALL
SELECT id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current FROM historical_data`,
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
		},
		{
			name: "scd2_full_refresh_with_no_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByTime,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "DATE"},
				},
			},
			query:   "SELECT id, event_name, ts from source_table",
			wantErr: true,
		},
		{
			name: "scd2_full_refresh_with_no_primary_key",
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
					{Name: "ts", Type: "DATE"},
				},
			},
			query:   "SELECT id, event_name, ts from source_table",
			wantErr: true,
		},
		{
			name: "scd2_full_refresh_with_multiple_primary_keys",
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
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "DATE"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_type, event_name, ts from source_table",
			want: []string{
				`CREATE TABLE IF NOT EXISTS __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS
SELECT
  src.id, src.event_type, src.event_name, src.ts,		
  CAST(ts AS TIMESTAMP) AS _valid_from,
  TIMESTAMP '9999-12-31' AS _valid_until,
  TRUE AS _is_current
FROM (
SELECT id, event_type, event_name, ts from source_table
) AS src`,
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
			},
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
			want: []string{
				`CREATE TABLE IF NOT EXISTS __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS
SELECT
  src.id, src.event_name, src.ts,		
  CAST(ts AS TIMESTAMP) AS _valid_from,
  TIMESTAMP '9999-12-31' AS _valid_until,
  TRUE AS _is_current
FROM (
SELECT id, event_name, ts from source_table
) AS src`,
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
			},
		},
		{
			name: "scd2_full_refresh_with_custom_partitioning",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
					PartitionBy:    "event_type, id",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "DATE"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			want: []string{
				`CREATE TABLE IF NOT EXISTS __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['event_type, id']) AS
SELECT
  src.id, src.event_name, src.ts,		
  CAST(ts AS TIMESTAMP) AS _valid_from,
  TIMESTAMP '9999-12-31' AS _valid_until,
  TRUE AS _is_current
FROM (
SELECT id, event_name, ts from source_table
) AS src`,
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.asset, tt.query, "s3://bucket")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, render)
			}
		})
	}
}
