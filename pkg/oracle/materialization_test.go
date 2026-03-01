package oracle

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
			want:  "CREATE OR REPLACE VIEW my.asset AS\nSELECT 1",
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
			want: `BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE my.asset PURGE';
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE != -942 THEN
            RAISE;
         END IF;
   END;
   EXECUTE IMMEDIATE 'CREATE TABLE my.asset AS SELECT 1';
END;`,
		},
		{
			name: "create+replace with single-quoted query content",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 'hello' FROM dual",
			want: `BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE my.asset PURGE';
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE != -942 THEN
            RAISE;
         END IF;
   END;
   EXECUTE IMMEDIATE 'CREATE TABLE my.asset AS SELECT ''hello'' FROM dual';
END;`,
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
			name: "delete+insert builds a proper PL/SQL block",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1 as dt",
			want: `BEGIN
   DELETE FROM my.asset t WHERE EXISTS (
      SELECT 1 FROM (SELECT 1 as dt) s WHERE s.dt = t.dt
   );
   INSERT INTO my.asset
SELECT 1 as dt
;
END;`,
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
			want:  "MERGE INTO my.asset target\nUSING (\nSELECT 1 as id, 'abc' as name\n) source ON ((target.id = source.id OR (target.id IS NULL AND source.id IS NULL)))\nWHEN MATCHED THEN UPDATE SET target.name = source.name\nWHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);",
		},
		{
			name: "merge with custom MergeSQL expression",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "name", Type: "varchar", UpdateOnMerge: true, MergeSQL: "COALESCE(source.name, target.name)"},
				},
			},
			query: "SELECT 1 as id, 'abc' as name",
			want:  "MERGE INTO my.asset target\nUSING (\nSELECT 1 as id, 'abc' as name\n) source ON ((target.id = source.id OR (target.id IS NULL AND source.id IS NULL)))\nWHEN MATCHED THEN UPDATE SET target.name = COALESCE(source.name, target.name)\nWHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);",
		},
		{
			name: "merge insert-only, no UpdateOnMerge columns",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "name", Type: "varchar"},
				},
			},
			query: "SELECT 1 as id, 'abc' as name",
			want:  "MERGE INTO my.asset target\nUSING (\nSELECT 1 as id, 'abc' as name\n) source ON ((target.id = source.id OR (target.id IS NULL AND source.id IS NULL)))\nWHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);",
		},
		{
			name: "semicolon trimming and schema-qualified table name",
			task: &pipeline.Asset{
				Name: "my_schema.my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT 1 FROM dual ;  ",
			want:  "INSERT INTO my_schema.my_table SELECT 1 FROM dual",
		},
		{
			name: "invalid identifier is rejected",
			task: &pipeline.Asset{
				Name: "my;table--drop",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "view with append strategy is rejected",
			task: &pipeline.Asset{
				Name: "my.view",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "truncate+insert builds PL/SQL block",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT 1",
			want: `BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE my.asset';
   INSERT INTO my.asset
SELECT 1
;
END;`,
		},
		{
			name: "time_interval with timestamp granularity",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "ts",
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			},
			query: "SELECT 1 as ts",
			want: `BEGIN
   DELETE FROM my.asset WHERE ts BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}';
   INSERT INTO my.asset
SELECT 1 as ts
;
END;`,
		},
		{
			name: "time_interval with date granularity",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "dt",
					TimeGranularity: pipeline.MaterializationTimeGranularityDate,
				},
			},
			query: "SELECT 1 as dt",
			want: `BEGIN
   DELETE FROM my.asset WHERE dt BETWEEN '{{start_date}}' AND '{{end_date}}';
   INSERT INTO my.asset
SELECT 1 as dt
;
END;`,
		},
		{
			name: "time_interval missing incremental_key",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					TimeGranularity: pipeline.MaterializationTimeGranularityDate,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "time_interval missing time_granularity",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey: "ts",
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
				if !assert.Equal(t, tt.want, render) {
					t.Logf("\nWant:\n%s\nGot:\n%s", tt.want, render)
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
			want: `BEGIN
UPDATE my.asset target
SET bruin_valid_until = LOCALTIMESTAMP, bruin_is_current = 0
WHERE target.bruin_is_current = 1
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT id, event_name, ts from source_table) source
    WHERE (target.id = source.id OR (target.id IS NULL AND source.id IS NULL))
  )
  AND EXISTS (SELECT 1 FROM (SELECT id, event_name, ts from source_table) source_exists);

MERGE INTO (SELECT * FROM my.asset WHERE bruin_is_current = 1) target
USING (
  WITH s1 AS (
    SELECT id, event_name, ts from source_table
  )
  SELECT s1.*, 1 AS bruin_is_current_src
  FROM s1
  UNION ALL
  SELECT s1.*, 0 AS bruin_is_current_src
  FROM s1
  JOIN my.asset t1 ON ((t1.id = s1.id OR (t1.id IS NULL AND s1.id IS NULL)))
  WHERE t1.bruin_valid_from < CAST(s1.ts AS TIMESTAMP) AND t1.bruin_is_current = 1
) source
ON ((target.id = source.id OR (target.id IS NULL AND source.id IS NULL)) AND source.bruin_is_current_src = 1)
WHEN MATCHED THEN
  UPDATE SET
    target.bruin_valid_until = CAST(source.ts AS TIMESTAMP),
    target.bruin_is_current  = 0
  WHERE target.bruin_valid_from < CAST(source.ts AS TIMESTAMP)
WHEN NOT MATCHED THEN
  INSERT (id, event_name, ts, bruin_valid_from, bruin_valid_until, bruin_is_current)
  VALUES (source.id, source.event_name, source.ts, CAST(source.ts AS TIMESTAMP), TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), 1);
END;`,
		},
		{
			name: "scd2_reserved_columns",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "bruin_valid_from", Type: "TIMESTAMP"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, _valid_from, ts from source_table",
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
					{Name: "ts", Type: "VARCHAR"},
				},
			},
			query:       "SELECT id, ts from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_incremental_key_not_in_columns",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "missing_col",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "ts", Type: "TIMESTAMP"},
				},
			},
			query:       "SELECT id, ts from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_timestamp_with_timezone_rejected",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "ts", Type: "TIMESTAMP WITH TIME ZONE"},
				},
			},
			query:       "SELECT id, ts from source_table",
			wantErr:     true,
			fullRefresh: false,
		},
		{
			name: "scd2_timestamp_with_local_timezone_rejected",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true, Type: "INTEGER"},
					{Name: "ts", Type: "TIMESTAMP WITH LOCAL TIME ZONE"},
				},
			},
			query:       "SELECT id, ts from source_table",
			wantErr:     true,
			fullRefresh: false,
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
