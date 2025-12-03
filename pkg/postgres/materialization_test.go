package postgres

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
			want:  "CREATE OR REPLACE VIEW \"my\".\"asset\" AS\nSELECT 1",
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
DROP TABLE IF EXISTS "my"."asset"; 
CREATE TABLE "my"."asset" AS SELECT 1;
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
DROP TABLE IF EXISTS "my"."asset"; 
CREATE TABLE "my"."asset" AS SELECT 1;
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
			want:  "INSERT INTO \"my\".\"asset\" SELECT 1",
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
				`DELETE FROM "my"\."asset" WHERE "dt" in \(SELECT DISTINCT "dt" FROM __bruin_tmp_.+\);` + "\n" +
				`INSERT INTO "my"\."asset" SELECT \* FROM __bruin_tmp_.+;` + "\n" +
				"DROP TABLE IF EXISTS __bruin_tmp_.+;\n" +
				"COMMIT;$",
		},
		{
			name: "delete+insert with case-sensitive incremental key",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "eventTime",
				},
			},
			query: "SELECT 1, NOW() as \"eventTime\"",
			want: "^BEGIN TRANSACTION;\n" +
				`CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1, NOW\(\) as "eventTime"\n;\n` +
				`DELETE FROM "my"\."asset" WHERE "eventTime" in \(SELECT DISTINCT "eventTime" FROM __bruin_tmp_.+\);` + "\n" +
				`INSERT INTO "my"\."asset" SELECT \* FROM __bruin_tmp_.+;` + "\n" +
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
			name: "truncate+insert materialization",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT 1 as id, 'test' as name",
			want: `BEGIN TRANSACTION;
TRUNCATE TABLE "my"."asset";
INSERT INTO "my"."asset" SELECT 1 as id, 'test' as name;
COMMIT;`,
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
			want: `^MERGE INTO "my"\."asset" target
USING \(SELECT 1 as id, 'abc' as name\) source ON target\."id" = source\."id"
WHEN MATCHED THEN UPDATE SET "name" = source\."name"
WHEN NOT MATCHED THEN INSERT\("id", "name"\) VALUES\("id", "name"\);$`,
		},
		{
			name: "redshift merge with primary keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Type: "rs.sql",
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
			want: `^MERGE INTO "my"\."asset"
USING \(SELECT 1 as id, 'abc' as name\) source ON "my"\."asset"\."id" = source\."id"
WHEN MATCHED THEN UPDATE SET "name" = source\."name"
WHEN NOT MATCHED THEN INSERT\("id", "name"\) VALUES\(source."id", source."name"\);$`,
		},
		{
			name: "merge with case-sensitive fields",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "code", Type: "varchar", PrimaryKey: true},
					{Name: "iLeft", Type: "int", PrimaryKey: false, UpdateOnMerge: true},
					{Name: "iRight", Type: "int", PrimaryKey: false, UpdateOnMerge: true},
					{Name: "translation", Type: "varchar", PrimaryKey: false, UpdateOnMerge: true},
				},
			},
			query: `SELECT 'ABC' as code, 1 as "iLeft", 2 as "iRight", 'test' as translation`,
			want: `^MERGE INTO "my"\."asset" target
USING \(SELECT 'ABC' as code, 1 as "iLeft", 2 as "iRight", 'test' as translation\) source ON target\."code" = source\."code"
WHEN MATCHED THEN UPDATE SET "iLeft" = source\."iLeft", "iRight" = source\."iRight", "translation" = source\."translation"
WHEN NOT MATCHED THEN INSERT\("code", "iLeft", "iRight", "translation"\) VALUES\("code", "iLeft", "iRight", "translation"\);$`,
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
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "col_a", Type: "int", MergeSQL: "GREATEST(target.col_a, source.col_a)"},
					{Name: "col_b", Type: "int", MergeSQL: "target.col_b + source.col_b"},
					{Name: "col_c", Type: "varchar", UpdateOnMerge: true},
				},
			},
			query: "SELECT 1 as id, 15 as col_a, 50 as col_b, 'updated' as col_c",
			want: `^MERGE INTO "my"\."asset" target
USING \(SELECT 1 as id, 15 as col_a, 50 as col_b, 'updated' as col_c\) source ON target\."id" = source\."id"
WHEN MATCHED THEN UPDATE SET "col_a" = GREATEST\(target\.col_a, source\.col_a\), "col_b" = target\.col_b \+ source\.col_b, "col_c" = source\."col_c"
WHEN NOT MATCHED THEN INSERT\("id", "col_a", "col_b", "col_c"\) VALUES\("id", "col_a", "col_b", "col_c"\);$`,
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
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "col_a", Type: "int", MergeSQL: "LEAST(target.col_a, source.col_a)"},
				},
			},
			query: "SELECT 1 as id, 15 as col_a",
			want: `^MERGE INTO "my"\."asset" target
USING \(SELECT 1 as id, 15 as col_a\) source ON target\."id" = source\."id"
WHEN MATCHED THEN UPDATE SET "col_a" = LEAST\(target\.col_a, source\.col_a\)
WHEN NOT MATCHED THEN INSERT\("id", "col_a"\) VALUES\("id", "col_a"\);$`,
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
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "col_a", Type: "int", MergeSQL: "COALESCE(source.col_a, target.col_a)", UpdateOnMerge: true},
				},
			},
			query: "SELECT 1 as id, 15 as col_a",
			want: `^MERGE INTO "my"\."asset" target
USING \(SELECT 1 as id, 15 as col_a\) source ON target\."id" = source\."id"
WHEN MATCHED THEN UPDATE SET "col_a" = COALESCE\(source\.col_a, target\.col_a\)
WHEN NOT MATCHED THEN INSERT\("id", "col_a"\) VALUES\("id", "col_a"\);$`,
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
			want: `^BEGIN TRANSACTION;\s*` +
				`DELETE FROM "my"\."asset" WHERE "ts" BETWEEN '\{\{start_timestamp\}\}' AND '\{\{end_timestamp\}\}';\s*` +
				`INSERT INTO "my"\."asset" SELECT ts, event_name from source_table where ts between '\{\{start_timestamp\}\}' AND '\{\{end_timestamp\}\}';\s*` +
				`COMMIT;$`,
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
			want: `^BEGIN TRANSACTION;\s*` +
				`DELETE FROM "my"\."asset" WHERE "dt" BETWEEN '\{\{start_date\}\}' AND '\{\{end_date\}\}';\s*` +
				`INSERT INTO "my"\."asset" SELECT dt, event_name from source_table where dt between '\{\{start_date\}\}' and '\{\{end_date\}\}';\s*` +
				`COMMIT;$`,
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
			want: "CREATE TABLE IF NOT EXISTS \"empty_table\" \\(\n" +
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
			want: "CREATE TABLE IF NOT EXISTS \"one_col_table\" \\(\n" +
				`\"id\" INT64\n` +
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
			want: `CREATE TABLE IF NOT EXISTS "two_col_table" \(\s*"id" INT64,\s*"name" STRING\s*\);\s*COMMENT ON COLUMN "two_col_table"\."name" IS 'The name of the person';`,
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
			want: `CREATE TABLE IF NOT EXISTS "my_primary_key_table" \(\s*"id" INT64,\s*"category" STRING,\s*primary key \("id"\)\s*\);\s*COMMENT ON COLUMN "my_primary_key_table"\."category" IS 'Category of the item';`,
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
			want: `CREATE TABLE IF NOT EXISTS "my_composite_primary_key_table" \(\s*"id" INT64,\s*"category" STRING,\s*primary key \("id", "category"\)\s*\);\s*COMMENT ON COLUMN "my_composite_primary_key_table"\."category" IS 'Category of the item';`,
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
		//nolint:dupword
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
			want: "MERGE INTO \"my\".\"asset\" AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, event_name, ts from source_table\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   \"my\".\"asset\" AS t1 USING (id)\n" +
				"  WHERE  t1._valid_from < s1.\"ts\" AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.\"id\" = source.\"id\" AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < source.\"ts\"\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = source.\"ts\",\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (\"id\", \"event_name\", \"ts\", _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.\"id\", source.\"event_name\", source.\"ts\", source.\"ts\", '9999-12-31 00:00:00', TRUE);",
		},
		//nolint:dupword
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
			want: "MERGE INTO \"my\".\"asset\" AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, event_type, col1, col2, ts from source_table\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   \"my\".\"asset\" AS t1 USING (id, event_type)\n" +
				"  WHERE  t1._valid_from < s1.\"ts\" AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.\"id\" = source.\"id\" AND target.\"event_type\" = source.\"event_type\" AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < source.\"ts\"\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = source.\"ts\",\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (\"id\", \"event_type\", \"col1\", \"col2\", \"ts\", _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.\"id\", source.\"event_type\", source.\"col1\", source.\"col2\", source.\"ts\", source.\"ts\", '9999-12-31 00:00:00', TRUE);",
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
				"DROP TABLE IF EXISTS \"my\".\"asset\";\n" +
				"CREATE TABLE \"my\".\"asset\" AS\n" +
				"SELECT\n" +
				"  \"ts\" AS _valid_from,\n" +
				"  src.*,\n" +
				"  '9999-12-31 00:00:00'::TIMESTAMP AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
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
			want: "MERGE INTO \"my\".\"asset\" AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, col1, col2, col3, col4 from source_table\n" +
				"  )\n" +
				"  SELECT *, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM   s1\n" +
				"  JOIN   \"my\".\"asset\" AS t1 USING (id)\n" +
				"  WHERE  (t1.col1 != s1.col1 OR t1.col2 != s1.col2 OR t1.col3 != s1.col3 OR t1.col4 != s1.col4) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"    target.col1 != source.col1 OR target.col2 != source.col2 OR target.col3 != source.col3 OR target.col4 != source.col4\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (id, col1, col2, col3, col4, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.id, source.col1, source.col2, source.col3, source.col4, CURRENT_TIMESTAMP, '9999-12-31 00:00:00'::TIMESTAMP, TRUE);",
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
			want: "MERGE INTO \"my\".\"asset\" AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, category, name, price from source_table\n" +
				"  )\n" +
				"  SELECT *, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM   s1\n" +
				"  JOIN   \"my\".\"asset\" AS t1 USING (id, category)\n" +
				"  WHERE  (t1.name != s1.name OR t1.price != s1.price) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target.category = source.category AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"    target.name != source.name OR target.price != source.price\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (id, category, name, price, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.id, source.category, source.name, source.price, CURRENT_TIMESTAMP, '9999-12-31 00:00:00'::TIMESTAMP, TRUE);",
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
			want: "BEGIN TRANSACTION;\n" +
				"DROP TABLE IF EXISTS \"my\".\"asset\";\n" +
				"CREATE TABLE \"my\".\"asset\" AS\n" +
				"SELECT\n" +
				"  CURRENT_TIMESTAMP AS _valid_from,\n" +
				"  src.*,\n" +
				"  '9999-12-31 00:00:00'::TIMESTAMP AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
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

func TestBuildRedshiftSCD2QueryByTime(t *testing.T) {
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
			want: "^BEGIN TRANSACTION;\n\n" +
				"-- Create temp table with source data\n" +
				"CREATE TEMP TABLE __bruin_scd2_time_tmp_.+ AS \n" +
				"SELECT \\*, TRUE AS _is_current FROM \\(SELECT id, event_name, ts from source_table\\) AS src;\n\n" +
				"-- Update existing records where source timestamp is newer\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = source\\.\"ts\", _is_current = FALSE\n" +
				"FROM __bruin_scd2_time_tmp_.+ AS source\n" +
				"WHERE target\\.\"id\" = source\\.\"id\"\n" +
				"  AND target\\._is_current = TRUE\n" +
				"  AND target\\._valid_from < source\\.\"ts\";\n\n" +
				"-- Update records that are no longer in source \\(expired\\)\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = CURRENT_TIMESTAMP, _is_current = FALSE\n" +
				"WHERE target\\._is_current = TRUE\n" +
				"  AND NOT EXISTS \\(\n" +
				"    SELECT 1 FROM __bruin_scd2_time_tmp_.+ AS source\n" +
				"    WHERE target\\.\"id\" = source\\.\"id\"\n" +
				"  \\);\n\n" +
				"-- Insert new records and new versions of changed records\n" +
				"INSERT INTO \"my\"\\.\"asset\" \\(\"id\", \"event_name\", \"ts\", _valid_from, _valid_until, _is_current\\)\n" +
				"SELECT source\\.\"id\", source\\.\"event_name\", source\\.\"ts\", source\\.\"ts\", TIMESTAMP '9999-12-31 00:00:00', TRUE\n" + //nolint:dupword
				"FROM __bruin_scd2_time_tmp_.+ AS source\n" +
				"WHERE NOT EXISTS \\(\n" +
				"  SELECT 1 FROM \"my\"\\.\"asset\" AS target \n" +
				"  WHERE target\\.\"id\" = source\\.\"id\" AND target\\._is_current = TRUE\n" +
				"\\)\n" +
				"OR EXISTS \\(\n" +
				"  SELECT 1 FROM \"my\"\\.\"asset\" AS target\n" +
				"  WHERE target\\.\"id\" = source\\.\"id\" AND target\\._is_current = FALSE \n" +
				"  AND target\\._valid_until = source\\.\"ts\"\n" +
				"\\);\n\n" +
				"DROP TABLE __bruin_scd2_time_tmp_.+;\n" +
				"COMMIT;$",
		},
		{
			name: "scd2_multiple_primary_keys_with_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Type: pipeline.AssetTypeRedshiftQuery,
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
			want: "^BEGIN TRANSACTION;\n\n" +
				"-- Create temp table with source data\n" +
				"CREATE TEMP TABLE __bruin_scd2_time_tmp_.+ AS \n" +
				"SELECT \\*, TRUE AS _is_current FROM \\(SELECT id, event_type, col1, col2, ts from source_table\\) AS src;\n\n" +
				"-- Update existing records where source timestamp is newer\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = source\\.\"ts\", _is_current = FALSE\n" +
				"FROM __bruin_scd2_time_tmp_.+ AS source\n" +
				"WHERE target\\.\"id\" = source\\.\"id\" AND target\\.\"event_type\" = source\\.\"event_type\"\n" +
				"  AND target\\._is_current = TRUE\n" +
				"  AND target\\._valid_from < source\\.\"ts\";\n\n" +
				"-- Update records that are no longer in source \\(expired\\)\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = CURRENT_TIMESTAMP, _is_current = FALSE\n" +
				"WHERE target\\._is_current = TRUE\n" +
				"  AND NOT EXISTS \\(\n" +
				"    SELECT 1 FROM __bruin_scd2_time_tmp_.+ AS source\n" +
				"    WHERE target\\.\"id\" = source\\.\"id\" AND target\\.\"event_type\" = source\\.\"event_type\"\n" +
				"  \\);\n\n" +
				"-- Insert new records and new versions of changed records\n" +
				"INSERT INTO \"my\"\\.\"asset\" \\(\"id\", \"event_type\", \"col1\", \"col2\", \"ts\", _valid_from, _valid_until, _is_current\\)\n" +
				"SELECT source\\.\"id\", source\\.\"event_type\", source\\.\"col1\", source\\.\"col2\", source\\.\"ts\", source\\.\"ts\", TIMESTAMP '9999-12-31 00:00:00', TRUE\n" + //nolint:dupword
				"FROM __bruin_scd2_time_tmp_.+ AS source\n" +
				"WHERE NOT EXISTS \\(\n" +
				"  SELECT 1 FROM \"my\"\\.\"asset\" AS target \n" +
				"  WHERE target\\.\"id\" = source\\.\"id\" AND target\\.\"event_type\" = source\\.\"event_type\" AND target\\._is_current = TRUE\n" +
				"\\)\n" +
				"OR EXISTS \\(\n" +
				"  SELECT 1 FROM \"my\"\\.\"asset\" AS target\n" +
				"  WHERE target\\.\"id\" = source\\.\"id\" AND target\\.\"event_type\" = source\\.\"event_type\" AND target\\._is_current = FALSE \n" +
				"  AND target\\._valid_until = source\\.\"ts\"\n" +
				"\\);\n\n" +
				"DROP TABLE __bruin_scd2_time_tmp_.+;\n" +
				"COMMIT;$",
		},
		{
			name: "scd2_full_refresh_with_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Type: pipeline.AssetTypeRedshiftQuery,
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
				"DROP TABLE IF EXISTS \"my\".\"asset\";\n" +
				"CREATE TABLE \"my\".\"asset\" AS\n" +
				"SELECT\n" +
				"  \"ts\" AS _valid_from,\n" +
				"  src.*,\n" +
				"  TIMESTAMP '9999-12-31 00:00:00' AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
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
				if strings.HasSuffix(tt.want, "$") {
					// Use regex matching for complex queries with temp table names
					assert.Regexp(t, tt.want, render)
				} else {
					assert.Equal(t, strings.TrimSpace(tt.want), render)
				}
			}
		})
	}
}

func TestBuildRedshiftSCD2ByColumnQuery(t *testing.T) {
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
				Type: pipeline.AssetTypeRedshiftQuery,
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
			want: "^BEGIN TRANSACTION;\n\n" +
				"-- Capture the timestamp once for the entire transaction\n" +
				"CREATE TEMP TABLE _ts AS \n" +
				"SELECT CURRENT_TIMESTAMP AS session_timestamp;\n\n" +
				"-- Create temp table with source data\n" +
				"CREATE TEMP TABLE __bruin_scd2_tmp_.+ AS \n" +
				"SELECT \\*, TRUE AS _is_current FROM \\(SELECT id, col1, col2, col3, col4 from source_table\\) AS src;\n\n" +
				"-- Update existing records that have changes\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = \\(SELECT session_timestamp FROM _ts\\), _is_current = FALSE\n" +
				"WHERE target\\._is_current = TRUE\n" +
				"  AND EXISTS \\(\n" +
				"    SELECT 1 FROM __bruin_scd2_tmp_.+ AS source\n" +
				"    WHERE target\\.\"id\" = source\\.\"id\" AND \\(target\\.\"col1\" != source\\.\"col1\" OR target\\.\"col2\" != source\\.\"col2\" OR target\\.\"col3\" != source\\.\"col3\" OR target\\.\"col4\" != source\\.\"col4\"\\)\n" +
				"  \\);\n\n" +
				"-- Update records that are no longer in source \\(expired\\)\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = \\(SELECT session_timestamp FROM _ts\\), _is_current = FALSE\n" +
				"WHERE target\\._is_current = TRUE\n" +
				"  AND NOT EXISTS \\(\n" +
				"    SELECT 1 FROM __bruin_scd2_tmp_.+ AS source\n" +
				"    WHERE target\\.\"id\" = source\\.\"id\"\n" +
				"  \\);\n\n" +
				"-- Insert new records and new versions of changed records\n" +
				"INSERT INTO \"my\"\\.\"asset\" \\(\"id\", \"col1\", \"col2\", \"col3\", \"col4\", _valid_from, _valid_until, _is_current\\)\n" +
				"SELECT source\\.\"id\", source\\.\"col1\", source\\.\"col2\", source\\.\"col3\", source\\.\"col4\", \\(SELECT session_timestamp FROM _ts\\), TIMESTAMP '9999-12-31 00:00:00', TRUE\n" +
				"FROM __bruin_scd2_tmp_.+ AS source\n" +
				"WHERE NOT EXISTS \\(\n" +
				"  SELECT 1 FROM \"my\"\\.\"asset\" AS target \n" +
				"  WHERE target\\.\"id\" = source\\.\"id\" AND target\\._is_current = TRUE\n" +
				"\\);\n\n" +
				"DROP TABLE __bruin_scd2_tmp_.+;\n" +
				"COMMIT;$",
		},
		{
			name: "scd2_multiple_primary_keys",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Type: pipeline.AssetTypeRedshiftQuery,
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
			want: "^BEGIN TRANSACTION;\n\n" +
				"-- Capture the timestamp once for the entire transaction\n" +
				"CREATE TEMP TABLE _ts AS \n" +
				"SELECT CURRENT_TIMESTAMP AS session_timestamp;\n\n" +
				"-- Create temp table with source data\n" +
				"CREATE TEMP TABLE __bruin_scd2_tmp_.+ AS \n" +
				"SELECT \\*, TRUE AS _is_current FROM \\(SELECT id, category, name, price from source_table\\) AS src;\n\n" +
				"-- Update existing records that have changes\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = \\(SELECT session_timestamp FROM _ts\\), _is_current = FALSE\n" +
				"WHERE target\\._is_current = TRUE\n" +
				"  AND EXISTS \\(\n" +
				"    SELECT 1 FROM __bruin_scd2_tmp_.+ AS source\n" +
				"    WHERE target\\.\"id\" = source\\.\"id\" AND target\\.\"category\" = source\\.\"category\" AND \\(target\\.\"name\" != source\\.\"name\" OR target\\.\"price\" != source\\.\"price\"\\)\n" +
				"  \\);\n\n" +
				"-- Update records that are no longer in source \\(expired\\)\n" +
				"UPDATE \"my\"\\.\"asset\" AS target\n" +
				"SET _valid_until = \\(SELECT session_timestamp FROM _ts\\), _is_current = FALSE\n" +
				"WHERE target\\._is_current = TRUE\n" +
				"  AND NOT EXISTS \\(\n" +
				"    SELECT 1 FROM __bruin_scd2_tmp_.+ AS source\n" +
				"    WHERE target\\.\"id\" = source\\.\"id\" AND target\\.\"category\" = source\\.\"category\"\n" +
				"  \\);\n\n" +
				"-- Insert new records and new versions of changed records\n" +
				"INSERT INTO \"my\"\\.\"asset\" \\(\"id\", \"category\", \"name\", \"price\", _valid_from, _valid_until, _is_current\\)\n" +
				"SELECT source\\.\"id\", source\\.\"category\", source\\.\"name\", source\\.\"price\", \\(SELECT session_timestamp FROM _ts\\), TIMESTAMP '9999-12-31 00:00:00', TRUE\n" +
				"FROM __bruin_scd2_tmp_.+ AS source\n" +
				"WHERE NOT EXISTS \\(\n" +
				"  SELECT 1 FROM \"my\"\\.\"asset\" AS target \n" +
				"  WHERE target\\.\"id\" = source\\.\"id\" AND target\\.\"category\" = source\\.\"category\" AND target\\._is_current = TRUE\n" +
				"\\);\n\n" +
				"DROP TABLE __bruin_scd2_tmp_.+;\n" +
				"COMMIT;$",
		},
		{
			name: "scd2_full_refresh_by_column",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Type: pipeline.AssetTypeRedshiftQuery,
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
			want: "BEGIN TRANSACTION;\n" +
				"DROP TABLE IF EXISTS \"my\".\"asset\";\n" +
				"CREATE TABLE \"my\".\"asset\" AS\n" +
				"SELECT\n" +
				"  CURRENT_TIMESTAMP AS _valid_from,\n" +
				"  src.*,\n" +
				"  TIMESTAMP '9999-12-31 00:00:00' AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
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
				if strings.HasSuffix(tt.want, "$") {
					// Use regex matching for complex queries with temp table names
					assert.Regexp(t, tt.want, render)
				} else {
					assert.Equal(t, strings.TrimSpace(tt.want), render)
				}
			}
		})
	}
}
