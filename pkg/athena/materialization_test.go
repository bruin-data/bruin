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
			want:  []string{"MERGE INTO my.asset target USING (SELECT pk, col1, col2, col3, col4 from input_table) source ON target.pk = source.pk WHEN MATCHED THEN UPDATE SET target.col1 = min(target.col1, source.col1), target.col2 = target.col1 - source.col1, target.col3 = source.col3 WHEN NOT MATCHED THEN INSERT(pk, col1, col2, col3, col4) VALUES(source.pk, source.col1, source.col2, source.col3, source.col4)"},
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
			want:  []string{"MERGE INTO my.asset target USING (SELECT id, value, count, status FROM source) source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.value = GREATEST(target.value, source.value), target.count = target.count + source.count WHEN NOT MATCHED THEN INSERT(id, value, count, status) VALUES(source.id, source.value, source.count, source.status)"},
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
			want:  []string{"MERGE INTO my.asset target USING (SELECT id, col1, col2, col3 FROM source) source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.col1 = COALESCE(source.col1, target.col1), target.col2 = source.col2 WHEN NOT MATCHED THEN INSERT(id, col1, col2, col3) VALUES(source.id, source.col1, source.col2, source.col3)"},
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
			want:  []string{"MERGE INTO my.asset target USING (SELECT 1 as id, 'abc' as name) source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name WHEN NOT MATCHED THEN INSERT(id, name) VALUES(source.id, source.name)"},
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
			name: "scd2_basic_column_change_detection and partitioning",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategySCD2ByColumn,
					PartitionBy: "id",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "col1"},
					{Name: "col2"},
				},
			},
			query: "SELECT id, col1, col2 from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['id']) AS\n" +
					"WITH\n" +
					"time_now AS (\n" +
					"\tSELECT CURRENT_TIMESTAMP AS now\n" +
					"),\n" +
					"source AS (\n" +
					"\tSELECT id, col1, col2,\n" +
					"\tTRUE as _matched_by_source\n" +
					"\tFROM (SELECT id, col1, col2 from source_table\n" +
					"\t)\n" +
					"),\n" +
					"target AS (\n" +
					"\tSELECT id, col1, col2, _valid_from, _valid_until, _is_current,\n" +
					"\tTRUE as _matched_by_target FROM my.asset\n" +
					"),\n" +
					"current_data AS (\n" +
					"\tSELECT id, col1, col2, _valid_from, _valid_until, _is_current, _matched_by_target\n" +
					"\tFROM target as t\n" +
					"\tWHERE _is_current = TRUE\n" +
					"),\n" +
					"--current or updated (expired) existing rows from target\n" +
					"to_keep AS (\n" +
					"\tSELECT t.id, t.col1, t.col2,\n" +
					"\tt._valid_from,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (t.col1 != s.col1 OR t.col2 != s.col2) THEN (SELECT now FROM time_now)\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)\n" +
					"\t\t\tELSE t._valid_until\n" +
					"\t\tEND AS _valid_until,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (t.col1 != s.col1 OR t.col2 != s.col2) THEN FALSE\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE\n" +
					"\t\t\tELSE t._is_current\n" +
					"\t\tEND AS _is_current\n" +
					"\tFROM target t\n" +
					"\tLEFT JOIN source s ON (t.id = s.id) AND t._is_current = TRUE\n" +
					"),\n" +
					"--new/updated rows from source\n" +
					"to_insert AS (\n" +
					"\tSELECT s.id AS id, s.col1 AS col1, s.col2 AS col2,\n" +
					"\t(SELECT now FROM time_now) AS _valid_from,\n" +
					"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"\tTRUE AS _is_current\n" +
					"\tFROM source s\n" +
					"\tLEFT JOIN current_data t ON (t.id = s.id)\n" +
					"\tWHERE (_matched_by_target IS NULL) OR (t.col1 != s.col1 OR t.col2 != s.col2)\n" +
					")\n" +
					"SELECT id, col1, col2, _valid_from, _valid_until, _is_current FROM to_keep\n" +
					"UNION ALL\n" +
					"SELECT id, col1, col2, _valid_from, _valid_until, _is_current FROM to_insert;",
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
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS\n" +
					"WITH\n" +
					"time_now AS (\n" +
					"\tSELECT CURRENT_TIMESTAMP AS now\n" +
					"),\n" +
					"source AS (\n" +
					"\tSELECT id, category, name,\n" +
					"\tTRUE as _matched_by_source\n" +
					"\tFROM (SELECT id, category, name from source_table\n" +
					"\t)\n" +
					"),\n" +
					"target AS (\n" +
					"\tSELECT id, category, name, _valid_from, _valid_until, _is_current,\n" +
					"\tTRUE as _matched_by_target FROM my.asset\n" +
					"),\n" +
					"current_data AS (\n" +
					"\tSELECT id, category, name, _valid_from, _valid_until, _is_current, _matched_by_target\n" +
					"\tFROM target as t\n" +
					"\tWHERE _is_current = TRUE\n" +
					"),\n" +
					"--current or updated (expired) existing rows from target\n" +
					"to_keep AS (\n" +
					"\tSELECT t.id, t.category, t.name,\n" +
					"\tt._valid_from,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (t.name != s.name) THEN (SELECT now FROM time_now)\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)\n" +
					"\t\t\tELSE t._valid_until\n" +
					"\t\tEND AS _valid_until,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (t.name != s.name) THEN FALSE\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE\n" +
					"\t\t\tELSE t._is_current\n" +
					"\t\tEND AS _is_current\n" +
					"\tFROM target t\n" +
					"\tLEFT JOIN source s ON (t.id = s.id AND t.category = s.category) AND t._is_current = TRUE\n" +
					"),\n" +
					"--new/updated rows from source\n" +
					"to_insert AS (\n" +
					"\tSELECT s.id AS id, s.category AS category, s.name AS name,\n" +
					"\t(SELECT now FROM time_now) AS _valid_from,\n" +
					"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"\tTRUE AS _is_current\n" +
					"\tFROM source s\n" +
					"\tLEFT JOIN current_data t ON (t.id = s.id AND t.category = s.category)\n" +
					"\tWHERE (_matched_by_target IS NULL) OR (t.name != s.name)\n" +
					")\n" +
					"SELECT id, category, name, _valid_from, _valid_until, _is_current FROM to_keep\n" +
					"UNION ALL\n" +
					"SELECT id, category, name, _valid_from, _valid_until, _is_current FROM to_insert;",
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

func TestBuildSCD2ByColumnFullRefreshQuery(t *testing.T) {
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
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['_valid_from']) AS\n" +
					"SELECT src.id, src.name, src.price,\n" +
					"CURRENT_TIMESTAMP AS _valid_from,\n" +
					"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"TRUE AS _is_current\n" +
					"FROM (SELECT id, name, price from source_table\n" +
					") AS src",
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
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['_valid_from']) AS\n" +
					"SELECT src.id, src.category, src.name, src.price,\n" +
					"CURRENT_TIMESTAMP AS _valid_from,\n" +
					"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"TRUE AS _is_current\n" +
					"FROM (SELECT id, category, name, price from source_table\n" +
					") AS src",
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
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['category, id']) AS\n" +
					"SELECT src.id, src.name, src.price,\n" +
					"CURRENT_TIMESTAMP AS _valid_from,\n" +
					"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"TRUE AS _is_current\n" +
					"FROM (SELECT id, name, price from source_table\n" +
					") AS src",
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

func TestBuildSCD2ByTimeQuery(t *testing.T) {
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
			name: "scd2_basic_time and partitioning",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
					PartitionBy:    "id",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "event_name"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query: "SELECT id, event_name, ts from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['id']) AS\n" +
					"WITH\n" +
					"time_now AS (\n" +
					"\tSELECT CURRENT_TIMESTAMP AS now\n" +
					"),\n" +
					"source AS (\n" +
					"\tSELECT id, event_name, ts,\n" +
					"\tTRUE as _matched_by_source\n" +
					"\tFROM (SELECT id, event_name, ts from source_table\n" +
					"\t)\n" +
					"),\n" +
					"target AS (\n" +
					"\tSELECT id, event_name, ts, _valid_from, _valid_until, _is_current,\n" +
					"\tTRUE as _matched_by_target FROM my.asset\n" +
					"),\n" +
					"current_data AS (\n" +
					"\tSELECT id, event_name, ts, _valid_from, _valid_until, _is_current, _matched_by_target\n" +
					"\tFROM target as t\n" +
					"\tWHERE _is_current = TRUE\n" +
					"),\n" +
					"--current or updated (expired) existing rows from target\n" +
					"to_keep AS (\n" +
					"\tSELECT t.id, t.event_name, t.ts,\n" +
					"\tt._valid_from,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (CAST(s.ts AS TIMESTAMP) > t._valid_from) THEN CAST(s.ts AS TIMESTAMP)\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)\n" +
					"\t\t\tELSE t._valid_until\n" +
					"\t\tEND AS _valid_until,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (CAST(s.ts AS TIMESTAMP) > t._valid_from) THEN FALSE\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE\n" +
					"\t\t\tELSE t._is_current\n" +
					"\t\tEND AS _is_current\n" +
					"\tFROM target t\n" +
					"\tLEFT JOIN source s ON (t.id = s.id) AND t._is_current = TRUE\n" +
					"),\n" +
					"--new/updated rows from source\n" +
					"to_insert AS (\n" +
					"\tSELECT s.id AS id, s.event_name AS event_name, s.ts AS ts,\n" +
					"\tCAST(s.ts AS TIMESTAMP) AS _valid_from,\n" +
					"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"\tTRUE AS _is_current\n" +
					"\tFROM source s\n" +
					"\tLEFT JOIN current_data t ON (t.id = s.id)\n" +
					"\tWHERE (_matched_by_target IS NULL) OR (CAST(s.ts AS TIMESTAMP) > t._valid_from)\n" +
					")\n" +
					"SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM to_keep\n" +
					"UNION ALL\n" +
					"SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM to_insert;",
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
		},
		{
			name: "scd2_multiple_primary_keys",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "event_name", PrimaryKey: true},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query: "SELECT id, event_name, ts from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS\n" +
					"WITH\n" +
					"time_now AS (\n" +
					"\tSELECT CURRENT_TIMESTAMP AS now\n" +
					"),\n" +
					"source AS (\n" +
					"\tSELECT id, event_name, ts,\n" +
					"\tTRUE as _matched_by_source\n" +
					"\tFROM (SELECT id, event_name, ts from source_table\n" +
					"\t)\n" +
					"),\n" +
					"target AS (\n" +
					"\tSELECT id, event_name, ts, _valid_from, _valid_until, _is_current,\n" +
					"\tTRUE as _matched_by_target FROM my.asset\n" +
					"),\n" +
					"current_data AS (\n" +
					"\tSELECT id, event_name, ts, _valid_from, _valid_until, _is_current, _matched_by_target\n" +
					"\tFROM target as t\n" +
					"\tWHERE _is_current = TRUE\n" +
					"),\n" +
					"--current or updated (expired) existing rows from target\n" +
					"to_keep AS (\n" +
					"\tSELECT t.id, t.event_name, t.ts,\n" +
					"\tt._valid_from,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (CAST(s.ts AS TIMESTAMP) > t._valid_from) THEN CAST(s.ts AS TIMESTAMP)\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN (SELECT now FROM time_now)\n" +
					"\t\t\tELSE t._valid_until\n" +
					"\t\tEND AS _valid_until,\n" +
					"\t\tCASE\n" +
					"\t\t\tWHEN _matched_by_source IS NOT NULL AND (CAST(s.ts AS TIMESTAMP) > t._valid_from) THEN FALSE\n" +
					"\t\t\tWHEN _matched_by_source IS NULL THEN FALSE\n" +
					"\t\t\tELSE t._is_current\n" +
					"\t\tEND AS _is_current\n" +
					"\tFROM target t\n" +
					"\tLEFT JOIN source s ON (t.id = s.id AND t.event_name = s.event_name) AND t._is_current = TRUE\n" +
					"),\n" +
					"--new/updated rows from source\n" +
					"to_insert AS (\n" +
					"\tSELECT s.id AS id, s.event_name AS event_name, s.ts AS ts,\n" +
					"\tCAST(s.ts AS TIMESTAMP) AS _valid_from,\n" +
					"\tTIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"\tTRUE AS _is_current\n" +
					"\tFROM source s\n" +
					"\tLEFT JOIN current_data t ON (t.id = s.id AND t.event_name = s.event_name)\n" +
					"\tWHERE (_matched_by_target IS NULL) OR (CAST(s.ts AS TIMESTAMP) > t._valid_from)\n" +
					")\n" +
					"SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM to_keep\n" +
					"UNION ALL\n" +
					"SELECT id, event_name, ts, _valid_from, _valid_until, _is_current FROM to_insert;",
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

func TestBuildSCD2ByTimeFullRefreshQuery(t *testing.T) {
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
			name: "scd2_full_refresh_by_time_with_no_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByTime,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "event_name", Type: "VARCHAR"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			wantErr:     true,
		},
		{
			name: "scd2_full_refresh_by_time_with_no_primary_key",
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
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			wantErr:     true,
		},

		{
			name: "scd2_full_refresh_by_time with incremental key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['_valid_from']) AS\n" +
					"SELECT src.id, src.event_name, src.ts,\n" +
					"CAST(src.ts AS TIMESTAMP) AS _valid_from,\n" +
					"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"TRUE AS _is_current\n" +
					"FROM (SELECT id, event_name, ts from source_table\n" +
					") AS src",
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
			},
		},
		{
			name: "scd2_full_refresh_by_time_with_multiple_primary_keys",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "category", Type: "VARCHAR", PrimaryKey: true},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, category, event_name, ts from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['_valid_from']) AS\n" +
					"SELECT src.id, src.category, src.event_name, src.ts,\n" +
					"CAST(src.ts AS TIMESTAMP) AS _valid_from,\n" +
					"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"TRUE AS _is_current\n" +
					"FROM (SELECT id, category, event_name, ts from source_table\n" +
					") AS src",
				"\nDROP TABLE IF EXISTS my.asset",
				"\nALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset;",
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
					PartitionBy:    "category, id",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "event_name", Type: "VARCHAR"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts from source_table",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['category, id']) AS\n" +
					"SELECT src.id, src.event_name, src.ts,\n" +
					"CAST(src.ts AS TIMESTAMP) AS _valid_from,\n" +
					"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
					"TRUE AS _is_current\n" +
					"FROM (SELECT id, event_name, ts from source_table\n" +
					") AS src",
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
