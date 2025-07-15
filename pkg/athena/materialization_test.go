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
					{Name: "col3"},
					{Name: "col4"},
				},
			},
			query: "SELECT id, col1, col2, col3, col4 from source_table",
			want: []string{
				"MERGE INTO my.asset AS target\nUSING (SELECT id, col1, col2, col3, col4 from source_table) AS source\nON target.id = source.id AND target._is_current = TRUE\n\nWHEN MATCHED AND (target.col1 != source.col1 OR target.col2 != source.col2 OR target.col3 != source.col3 OR target.col4 != source.col4) THEN\n  UPDATE SET\n    _valid_until = CURRENT_TIMESTAMP,\n    _is_current = FALSE\n\nWHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n  UPDATE SET \n    _valid_until = CURRENT_TIMESTAMP,\n    _is_current = FALSE\n\nWHEN NOT MATCHED THEN\n  INSERT (id, col1, col2, col3, col4, _valid_from, _valid_until, _is_current)\n  VALUES (source.id, source.col1, source.col2, source.col3, source.col4, CURRENT_TIMESTAMP, TIMESTAMP '9999-12-31', TRUE)",
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
					{Name: "price"},
				},
			},
			query: "SELECT id, category, name, price from source_table",
			want: []string{
				"MERGE INTO my.asset AS target\nUSING (SELECT id, category, name, price from source_table) AS source\nON target.id = source.id AND target.category = source.category AND target._is_current = TRUE\n\nWHEN MATCHED AND (target.name != source.name OR target.price != source.price) THEN\n  UPDATE SET\n    _valid_until = CURRENT_TIMESTAMP,\n    _is_current = FALSE\n\nWHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n  UPDATE SET \n    _valid_until = CURRENT_TIMESTAMP,\n    _is_current = FALSE\n\nWHEN NOT MATCHED THEN\n  INSERT (id, category, name, price, _valid_from, _valid_until, _is_current)\n  VALUES (source.id, source.category, source.name, source.price, CURRENT_TIMESTAMP, TIMESTAMP '9999-12-31', TRUE)",
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
				"CREATE TABLE IF NOT EXISTS my.asset WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/my.asset') AS\nSELECT\n  CURRENT_TIMESTAMP AS _valid_from,\n  src.*,\n  TIMESTAMP '9999-12-31' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, name, price from source_table\n) AS src",
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
				"CREATE TABLE IF NOT EXISTS my.asset WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/my.asset') AS\nSELECT\n  CURRENT_TIMESTAMP AS _valid_from,\n  src.*,\n  TIMESTAMP '9999-12-31' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, category, name, price from source_table\n) AS src",
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
				"CREATE TABLE IF NOT EXISTS my.asset WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/my.asset', partitioning = ARRAY['category, id']) AS\nSELECT\n  CURRENT_TIMESTAMP AS _valid_from,\n  src.*,\n  TIMESTAMP '9999-12-31' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, name, price from source_table\n) AS src",
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
				"MERGE INTO my.asset AS target\nUSING (SELECT id, event_name, ts from source_table) AS source\nON target.id = source.id AND target._is_current = TRUE\n\nWHEN MATCHED AND (target._valid_from < CAST(source.ts AS TIMESTAMP)) THEN\n  UPDATE SET\n    _valid_until = CAST(source.ts AS TIMESTAMP),\n    _is_current = FALSE\n\nWHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n  UPDATE SET \n    _valid_until = CURRENT_TIMESTAMP,\n    _is_current = FALSE\n\nWHEN NOT MATCHED THEN\n  INSERT (id, event_name, ts, _valid_from, _valid_until, _is_current)\n  VALUES (source.id, source.event_name, source.ts, CAST(source.ts AS TIMESTAMP), TIMESTAMP '9999-12-31', TRUE)",
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
				"MERGE INTO my.asset AS target\nUSING (SELECT id, event_type, col1, col2, ts from source_table) AS source\nON target.id = source.id AND target.event_type = source.event_type AND target._is_current = TRUE\n\nWHEN MATCHED AND (target._valid_from < CAST(source.ts AS TIMESTAMP)) THEN\n  UPDATE SET\n    _valid_until = CAST(source.ts AS TIMESTAMP),\n    _is_current = FALSE\n\nWHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n  UPDATE SET \n    _valid_until = CURRENT_TIMESTAMP,\n    _is_current = FALSE\n\nWHEN NOT MATCHED THEN\n  INSERT (id, event_type, col1, col2, ts, _valid_from, _valid_until, _is_current)\n  VALUES (source.id, source.event_type, source.col1, source.col2, source.ts, CAST(source.ts AS TIMESTAMP), TIMESTAMP '9999-12-31', TRUE)",
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
				"CREATE TABLE IF NOT EXISTS my.asset WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/my.asset') AS\nSELECT\n  CAST(ts AS TIMESTAMP) AS _valid_from,\n  src.*,\n  TIMESTAMP '9999-12-31' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, event_type, event_name, ts from source_table\n) AS src",
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
				"CREATE TABLE IF NOT EXISTS my.asset WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/my.asset') AS\nSELECT\n  CAST(ts AS TIMESTAMP) AS _valid_from,\n  src.*,\n  TIMESTAMP '9999-12-31' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, event_name, ts from source_table\n) AS src",
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
				"CREATE TABLE IF NOT EXISTS my.asset WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/my.asset', partitioning = ARRAY['event_type, id']) AS\nSELECT\n  CAST(ts AS TIMESTAMP) AS _valid_from,\n  src.*,\n  TIMESTAMP '9999-12-31' AS _valid_until,\n  TRUE AS _is_current\nFROM (\nSELECT id, event_name, ts from source_table\n) AS src",
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
