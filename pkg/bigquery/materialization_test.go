package bigquery

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
		exactMatch  bool
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
			want:  "CREATE OR REPLACE TABLE my.asset   AS\nSELECT 1",
		},
		{
			name: "materialize to a table, no partition or cluster, full refresh results in create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			fullRefresh: true,
			query:       "SELECT 1",
			want:        "CREATE OR REPLACE TABLE my.asset   AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition, no cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset PARTITION BY dt  AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition and cluster, single field to cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
					ClusterBy:   []string{"event_type"},
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset PARTITION BY dt CLUSTER BY event_type AS\nSELECT 1",
		},
		{
			name: "materialize to a table with partition and cluster, multiple fields to cluster",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "dt",
					ClusterBy:   []string{"event_type", "event_name"},
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE TABLE my.asset PARTITION BY dt CLUSTER BY event_type, event_name AS\nSELECT 1",
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
			name: "truncate+insert builds proper queries without transactions",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT 1 as id, 'test' as name",
			want: `TRUNCATE TABLE my.asset;
INSERT INTO my.asset SELECT 1 as id, 'test' as name`,
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
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1;\n" +
				"DELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "delete+insert with empty column type",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "somekey", Type: ""},
				},
			},
			query: "SELECT 1",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1;\n" +
				"DELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "delete+insert with UNKNOWN column type",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "somekey", Type: "UNKNOWN"},
				},
			},
			query: "SELECT 1",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1;\n" +
				"DELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "delete+insert builds a proper transaction where columns are defined",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "somekey",
				},
				Columns: []pipeline.Column{
					{Name: "somekey", Type: "date"},
				},
			},
			query: "SELECT 1",
			want: "^DECLARE distinct_keys.+ array<date>;\n" +
				"BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1;\n" +
				"SET distinct_keys_.+ = \\(SELECT array_agg\\(distinct somekey\\) FROM __bruin_tmp_.+\\);\n" +
				"DELETE FROM my\\.asset WHERE somekey in unnest\\(distinct_keys.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "delete+insert comment out",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1\n -- This is a comment",
			want: "^BEGIN TRANSACTION;\n" +
				"CREATE TEMP TABLE __bruin_tmp_.+ AS SELECT 1\n -- This is a comment;\n" +
				"DELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);\n" +
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp.+;\n" +
				"COMMIT TRANSACTION;$",
		},
		{
			name: "merge with no columns defined fails",
			task: &pipeline.Asset{
				Name:    "my.asset",
				Columns: []pipeline.Column{},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "merge with no primary key fails",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "dt"},
					{Name: "event_type"},
					{Name: "value"},
					{Name: "value2"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "merge with no columns to update",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "dt", PrimaryKey: true},
					{Name: "event_type", PrimaryKey: true},
					{Name: "value"},
					{Name: "value2"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query: "SELECT 1",
			want: "MERGE my\\.asset target\n" +
				"USING \\(SELECT 1\\) source\n" +
				"ON \\(\\(source\\.dt = target\\.dt OR \\(source\\.dt IS NULL and target\\.dt IS NULL\\)\\) AND \\(source\\.event_type = target\\.event_type OR \\(source\\.event_type IS NULL and target\\.event_type IS NULL\\)\\)\\)\n" +
				"\n" +
				"WHEN NOT MATCHED THEN INSERT\\(dt, event_type, value, value2\\) VALUES\\(dt, event_type, value, value2\\);",
		},
		{
			name: "merge with some columns to update",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "dt", PrimaryKey: true},
					{Name: "event_type", PrimaryKey: true},
					{Name: "value", UpdateOnMerge: true},
					{Name: "value2"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query: "SELECT 1;",
			want: "MERGE my\\.asset target\n" +
				"USING \\(SELECT 1\\) source\n" +
				"ON \\(\\(source\\.dt = target\\.dt OR \\(source\\.dt IS NULL and target\\.dt IS NULL\\)\\) AND \\(source\\.event_type = target\\.event_type OR \\(source\\.event_type IS NULL and target\\.event_type IS NULL\\)\\)\\)\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.value = source\\.value\n" +
				"WHEN NOT MATCHED THEN INSERT\\(dt, event_type, value, value2\\) VALUES\\(dt, event_type, value, value2\\);",
		},
		{
			name: "merge with merge_sql custom expressions",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "pk", PrimaryKey: true},
					{Name: "col1", MergeSQL: "min(target.col1, source.col1)"},
					{Name: "col2", MergeSQL: "target.col1 - source.col1"},
					{Name: "col3", UpdateOnMerge: true},
					{Name: "col4"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query: "SELECT pk, col1, col2, col3, col4 from input_table",
			want: "MERGE my.asset target\n" +
				"USING (SELECT pk, col1, col2, col3, col4 from input_table) source\n" +
				"ON ((source.pk = target.pk OR (source.pk IS NULL and target.pk IS NULL)))\n" +
				"WHEN MATCHED THEN UPDATE SET target.col1 = min(target.col1, source.col1), target.col2 = target.col1 - source.col1, target.col3 = source.col3\n" +
				"WHEN NOT MATCHED THEN INSERT(pk, col1, col2, col3, col4) VALUES(pk, col1, col2, col3, col4);",
			exactMatch: true,
		},
		{
			name: "merge with only merge_sql no update_on_merge",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "value", MergeSQL: "GREATEST(target.value, source.value)"},
					{Name: "count", MergeSQL: "target.count + source.count"},
					{Name: "status"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query: "SELECT id, value, count, status FROM source",
			want: "MERGE my.asset target\n" +
				"USING (SELECT id, value, count, status FROM source) source\n" +
				"ON ((source.id = target.id OR (source.id IS NULL and target.id IS NULL)))\n" +
				"WHEN MATCHED THEN UPDATE SET target.value = GREATEST(target.value, source.value), target.count = target.count + source.count\n" +
				"WHEN NOT MATCHED THEN INSERT(id, value, count, status) VALUES(id, value, count, status);",
			exactMatch: true,
		},
		{
			name: "merge with both merge_sql and update_on_merge prioritizes merge_sql",
			task: &pipeline.Asset{
				Name: "my.asset",
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "col1", MergeSQL: "COALESCE(source.col1, target.col1)", UpdateOnMerge: true},
					{Name: "col2", UpdateOnMerge: true},
					{Name: "col3"},
				},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			query: "SELECT id, col1, col2, col3 FROM source",
			want: "MERGE my.asset target\n" +
				"USING (SELECT id, col1, col2, col3 FROM source) source\n" +
				"ON ((source.id = target.id OR (source.id IS NULL and target.id IS NULL)))\n" +
				"WHEN MATCHED THEN UPDATE SET target.col1 = COALESCE(source.col1, target.col1), target.col2 = source.col2\n" +
				"WHEN NOT MATCHED THEN INSERT(id, col1, col2, col3) VALUES(id, col1, col2, col3);",
			exactMatch: true,
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
				"COMMIT TRANSACTION;$",
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
				"COMMIT TRANSACTION;$",
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

			if tt.exactMatch {
				assert.Equal(t, tt.want, render)
			} else {
				assert.Regexp(t, tt.want, render)
			}
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
					{Name: "name", Type: "STRING", Description: "The name of the person"},
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table (\n  id INT64,\n  name STRING OPTIONS(description=\"The name of the person\")\n)",
		},
		{
			name: "table with partitioning",
			asset: &pipeline.Asset{
				Name: "my_partitioned_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "timestamp", Type: "TIMESTAMP", Description: "Event timestamp"},
				},
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					PartitionBy: "timestamp",
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_partitioned_table (\n  id INT64,\n  timestamp TIMESTAMP OPTIONS(description=\"Event timestamp\")\n)\nPARTITION BY timestamp",
		},
		{
			name: "table with clustering",
			asset: &pipeline.Asset{
				Name: "my_clustered_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "category", Type: "STRING", Description: "Category of the item"},
				},
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					ClusterBy: []string{"category"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_clustered_table (\n  id INT64,\n  category STRING OPTIONS(description=\"Category of the item\")\n)\nCLUSTER BY category",
		},
		{
			name: "table with partitioning and clustering",
			asset: &pipeline.Asset{
				Name: "my_partitioned_clustered_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "timestamp", Type: "TIMESTAMP", Description: "Event timestamp"},
					{Name: "category", Type: "STRING", Description: "Category of the item"},
				},
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					PartitionBy: "timestamp",
					ClusterBy:   []string{"category"},
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_partitioned_clustered_table (\n  id INT64,\n  timestamp TIMESTAMP OPTIONS(description=\"Event timestamp\"),\n  category STRING OPTIONS(description=\"Category of the item\")\n)\nPARTITION BY timestamp\nCLUSTER BY category",
		},
		{
			name: "table with primary key",
			asset: &pipeline.Asset{
				Name: "my_table_with_pk",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "name", Type: "STRING", Description: "The name of the person"},
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table_with_pk (\n  id INT64,\n  name STRING OPTIONS(description=\"The name of the person\"),\n  PRIMARY KEY (id) NOT ENFORCED\n)",
		},
		{
			name: "table with multiple primary keys",
			asset: &pipeline.Asset{
				Name: "my_table_with_multiple_pks",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64", PrimaryKey: true},
					{Name: "category", Type: "STRING", PrimaryKey: true},
					{Name: "name", Type: "STRING", Description: "The name of the person"},
				},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			want: "CREATE TABLE IF NOT EXISTS my_table_with_multiple_pks (\n  id INT64,\n  category STRING,\n  name STRING OPTIONS(description=\"The name of the person\"),\n  PRIMARY KEY (id, category) NOT ENFORCED\n)",
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
			want: "MERGE INTO `my.asset` AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, event_name, ts from source_table\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   `my.asset` AS t1 USING (id)\n" +
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
			want: "MERGE INTO `my.asset` AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, event_type, col1, col2, ts from source_table\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   `my.asset` AS t1 USING (id, event_type)\n" +
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
			want: "CREATE OR REPLACE TABLE `my.asset`\n" +
				"PARTITION BY DATE(_valid_from)\n" +
				"CLUSTER BY _is_current, id AS\n" +
				"SELECT\n" +
				"  CAST (ts AS TIMESTAMP) AS _valid_from,\n" +
				"  src.*,\n" +
				"  TIMESTAMP('9999-12-31') AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
				"FROM (\n" +
				"SELECT id, event_name, ts from source_table\n" +
				") AS src;",
		},
		{
			name: "scd2_by_time_full_refresh_with_custom_partitioning",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "ts",
					PartitionBy:    "DATE(created_at)",
					ClusterBy:      []string{"id", "event_type"},
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "event_name"},
					{Name: "ts", Type: "DATE"},
					{Name: "created_at", Type: "TIMESTAMP"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, event_name, ts, created_at from source_table",
			want: "CREATE OR REPLACE TABLE `my.asset`\n" +
				"PARTITION BY DATE(created_at)\n" +
				"CLUSTER BY id, event_type AS\n" +
				"SELECT\n" +
				"  CAST (ts AS TIMESTAMP) AS _valid_from,\n" +
				"  src.*,\n" +
				"  TIMESTAMP('9999-12-31') AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
				"FROM (\n" +
				"SELECT id, event_name, ts, created_at from source_table\n" +
				") AS src;",
		},
		{
			name: "scd2_by_column_full_refresh_with_custom_partitioning",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategySCD2ByColumn,
					PartitionBy: "DATE(created_at)",
					ClusterBy:   []string{"id", "status"},
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "name"},
					{Name: "status"},
					{Name: "created_at", Type: "TIMESTAMP"},
				},
			},
			fullRefresh: true,
			query:       "SELECT id, name, status, created_at from source_table",
			want: "CREATE OR REPLACE TABLE `my.asset`\n" +
				"PARTITION BY DATE(created_at)\n" +
				"CLUSTER BY id, status AS\n" +
				"SELECT\n" +
				"  CURRENT_TIMESTAMP() AS _valid_from,\n" +
				"  src.*,\n" +
				"  TIMESTAMP('9999-12-31') AS _valid_until,\n" +
				"  TRUE                    AS _is_current\n" +
				"FROM (\n" +
				"SELECT id, name, status, created_at from source_table\n" +
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
			want: "MERGE INTO `my.asset` AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT id, col1, col2, col3, col4 from source_table\n" +
				"  )\n" +
				"  SELECT *, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM   s1\n" +
				"  JOIN   `my.asset` AS t1 USING (id)\n" +
				"  WHERE  (t1.col1 != s1.col1 OR t1.col2 != s1.col2 OR t1.col3 != s1.col3 OR t1.col4 != s1.col4) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.id = source.id AND target._is_current AND source._is_current\n" +
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
