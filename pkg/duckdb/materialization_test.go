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
			query: "SELECT 1 as id, 'abc' as name",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE OR REPLACE TABLE my\\.asset AS WITH source_data AS \\(" +
				"SELECT 1 as id, 'abc' as name\\) SELECT \\* FROM source_data UNION ALL SELECT dt\\.\\* FROM my\\.asset AS dt LEFT JOIN source_data AS sd USING\\(id\\) WHERE sd.id IS NULL;\n" +
				"COMMIT;$",
		},
		{
			name: "merge with composite primary keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "category", Type: "varchar", PrimaryKey: true},
					{Name: "name", Type: "varchar", UpdateOnMerge: true},
				},
			},
			query: "SELECT 1 as id, 'A' as category, 'abc' as name",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE OR REPLACE TABLE my\\.asset AS WITH source_data AS \\(" +
				"SELECT 1 as id, 'A' as category, 'abc' as name\\) SELECT \\* FROM source_data UNION ALL SELECT dt\\.\\* FROM my\\.asset AS dt LEFT JOIN source_data AS sd USING\\(id, category\\) WHERE sd.id IS NULL;\n" +
				"COMMIT;$",
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
				"COMMENT ON COLUMN my_table_with_comments.id IS 'Identifier for the record';\n" +
				"COMMENT ON COLUMN my_table_with_comments.name IS 'Name of the person';",
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
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
		want        string
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
				"SELECT src.id, src.name, src.price,\n" +
				"CURRENT_TIMESTAMP AS _valid_from,\n" +
				"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
				"TRUE AS _is_current\n" +
				"FROM (SELECT id, name, price from source_table\n" +
				") AS src;",
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
				"SELECT src.id, src.category, src.name, src.price,\n" +
				"CURRENT_TIMESTAMP AS _valid_from,\n" +
				"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
				"TRUE AS _is_current\n" +
				"FROM (SELECT id, category, name, price from source_table\n" +
				") AS src;",
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
				"SELECT src.id, src.name, src.price,\n" +
				"CURRENT_TIMESTAMP AS _valid_from,\n" +
				"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
				"TRUE AS _is_current\n" +
				"FROM (SELECT id, name, price from source_table\n" +
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
		want        string
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
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "event_name"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query: "SELECT id, event_name, ts from source_table",
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
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
		want        string
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
				"SELECT src.id, src.event_name, src.ts,\n" +
				"CAST(src.ts AS TIMESTAMP) AS _valid_from,\n" +
				"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
				"TRUE AS _is_current\n" +
				"FROM (SELECT id, event_name, ts from source_table\n" +
				") AS src;",
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
				"SELECT src.id, src.category, src.event_name, src.ts,\n" +
				"CAST(src.ts AS TIMESTAMP) AS _valid_from,\n" +
				"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
				"TRUE AS _is_current\n" +
				"FROM (SELECT id, category, event_name, ts from source_table\n" +
				") AS src;",
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
			want: "CREATE OR REPLACE TABLE my.asset AS\n" +
				"SELECT src.id, src.event_name, src.ts,\n" +
				"CAST(src.ts AS TIMESTAMP) AS _valid_from,\n" +
				"TIMESTAMP '9999-12-31 23:59:59' AS _valid_until,\n" +
				"TRUE AS _is_current\n" +
				"FROM (SELECT id, event_name, ts from source_table\n" +
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
				assert.Equal(t, tt.want, render)
			}
		})
	}
}
