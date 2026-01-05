package databricks

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
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
			want:  []string{"^DROP TABLE IF EXISTS my\\.asset", "CREATE OR REPLACE VIEW my\\.asset AS SELECT 1$"},
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
				"CREATE TABLE my\\.__bruin_tmp_.+ AS SELECT 1;",
				"DROP TABLE IF EXISTS my\\.asset;",
				"ALTER TABLE my\\.__bruin_tmp_.+ RENAME TO my\\.asset;",
			},
		},
		{
			name: "materialize to a table, no partition or cluster, full refresh defaults to create+replace",
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
				"CREATE TABLE my\\.__bruin_tmp_.+ AS SELECT 1;",
				"DROP TABLE IF EXISTS my\\.asset;",
				"ALTER TABLE my\\.__bruin_tmp_.+ RENAME TO my\\.asset;",
			},
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
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "materialize to a table with cluster is unsupported",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyCreateReplace,
					ClusterBy: []string{"event_type", "event_name"},
				},
			},
			query:   "SELECT 1",
			wantErr: true,
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
			name: "materialize to a table with truncate+insert",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyTruncateInsert,
				},
			},
			query: "SELECT 1",
			want: []string{
				"TRUNCATE TABLE my.asset",
				"INSERT INTO my.asset SELECT 1",
			},
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
			name: "delete+insert comment out ",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1 --this is a comment ",
			want: []string{
				"CREATE TEMPORARY VIEW __bruin_tmp_.+ AS SELECT 1 --this is a comment\n",
				"\nDELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\)",
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp_.+",
				"DROP VIEW IF EXISTS __bruin_tmp_.+",
			},
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
			want: []string{
				"CREATE TEMPORARY VIEW __bruin_tmp_.+ AS SELECT 1\n",
				"\nDELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\)",
				"INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp_.+",
				"DROP VIEW IF EXISTS __bruin_tmp_.+",
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
			want: []string{
				"MERGE INTO my\\.asset target\n" +
					"USING \\(SELECT 1 as id, 'abc' as name\\) source ON target\\.id = source\\.id\n" +
					"WHEN MATCHED THEN UPDATE SET name = source\\.name\n" +
					"WHEN NOT MATCHED THEN INSERT\\(id, name\\) VALUES\\(id, name\\)",
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
				"DELETE FROM my.asset WHERE ts BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'",
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
				"DELETE FROM my.asset WHERE dt BETWEEN '{{start_date}}' AND '{{end_date}}'",
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
				"CREATE TABLE IF NOT EXISTS empty_table \\(\n" +
					"\n" +
					"\\)",
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
				"CREATE TABLE IF NOT EXISTS one_col_table \\(\n" +
					"id INT64\n" +
					"\\)",
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
					{Name: "name", Type: "STRING", Description: "The name of the person", PrimaryKey: true},
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS two_col_table \\(\n" +
					"id INT64,\n" +
					"name STRING PRIMARY KEY COMMENT \\'The name of the person\\'\n" +
					"\\)",
			},
		},
		{
			name: "table with clustering",
			task: &pipeline.Asset{
				Name: "my_clustered_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
					{Name: "timestamp", Type: "TIMESTAMP", Description: "Event timestamp"},
				},
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyDDL,
					ClusterBy: []string{"timestamp, id"},
				},
			},
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_clustered_table \\(\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp'\n" +
					"\\)" +
					"\nCLUSTER BY \\(timestamp, id\\)",
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
				"CREATE TABLE IF NOT EXISTS my_partitioned_table \\(\n" +
					"id INT64 PRIMARY KEY,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp'\n" +
					"\\)" +
					"\nPARTITIONED BY \\(timestamp\\)",
			},
		},
		{
			name: "table with composite partitioning key",
			task: &pipeline.Asset{
				Name: "my_composite_partitioned_table",
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT64"},
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
				"CREATE TABLE IF NOT EXISTS my_composite_partitioned_table \\(\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp',\n" +
					"location STRING\n" +
					"\\)" +
					"\nPARTITIONED BY \\(timestamp, location\\)",
			},
		},
		// SCD2 by column tests
		{
			name: "scd2_by_column without primary keys",
			task: &pipeline.Asset{
				Name: "test.menu",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT"},
					{Name: "name", Type: "VARCHAR"},
				},
			},
			query:   "SELECT 1 as id, 'test' as name",
			wantErr: true,
		},
		{
			name: "scd2_by_column with reserved column name",
			task: &pipeline.Asset{
				Name: "test.menu",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "_is_current", Type: "BOOLEAN"},
				},
			},
			query:   "SELECT 1 as id, true as _is_current",
			wantErr: true,
		},
		{
			name: "scd2_by_column with primary keys",
			task: &pipeline.Asset{
				Name: "test.menu",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "ID", Type: "INT", PrimaryKey: true},
					{Name: "Name", Type: "VARCHAR", PrimaryKey: true},
					{Name: "Price", Type: "INT"},
				},
			},
			query: "SELECT 1 AS ID, 'Cola' AS Name, 399 AS Price",
			want: []string{
				"MERGE INTO test\\.menu AS target",
				"WITH s1 AS",
				"SELECT \\*, TRUE AS _is_current",
				"UNION ALL",
				"JOIN   test\\.menu AS t1 USING \\(ID, Name\\)",
				"ON  target\\.ID = source\\.ID AND target\\.Name = source\\.Name AND target\\._is_current AND source\\._is_current",
				"WHEN MATCHED AND",
				"target\\.Price != source\\.Price",
				"UPDATE SET",
				"_valid_until = CURRENT_TIMESTAMP\\(\\)",
				"_is_current  = FALSE",
				"WHEN NOT MATCHED THEN",
				"WHEN NOT MATCHED BY SOURCE AND target\\._is_current = TRUE THEN",
				"INSERT \\(ID, Name, Price, _valid_from, _valid_until, _is_current\\)",
				"VALUES \\(source\\.ID, source\\.Name, source\\.Price, CURRENT_TIMESTAMP\\(\\), TIMESTAMP '9999-12-31 00:00:00', TRUE\\)",
			},
		},
		{
			name: "scd2_by_column full refresh",
			task: &pipeline.Asset{
				Name: "test.menu",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "ID", Type: "INT", PrimaryKey: true},
					{Name: "Name", Type: "VARCHAR"},
				},
			},
			fullRefresh: true,
			query:       "SELECT 1 AS ID, 'Cola' AS Name",
			want: []string{
				"CREATE OR REPLACE TABLE test\\.menu AS",
				"SELECT",
				"CURRENT_TIMESTAMP\\(\\) AS _valid_from",
				"src\\.\\*",
				"TIMESTAMP '9999-12-31 00:00:00' AS _valid_until",
				"TRUE AS _is_current",
				"FROM \\(",
			},
		},
		// SCD2 by time tests
		{
			name: "scd2_by_time without incremental_key",
			task: &pipeline.Asset{
				Name: "test.products",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByTime,
				},
				Columns: []pipeline.Column{
					{Name: "product_id", Type: "INT", PrimaryKey: true},
					{Name: "product_name", Type: "VARCHAR"},
				},
			},
			query:   "SELECT 1 as product_id, 'test' as product_name",
			wantErr: true,
		},
		{
			name: "scd2_by_time without primary keys",
			task: &pipeline.Asset{
				Name: "test.products",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "product_id", Type: "INT"},
					{Name: "dt", Type: "DATE"},
				},
			},
			query:   "SELECT 1 as product_id, '2024-01-01' as dt",
			wantErr: true,
		},
		{
			name: "scd2_by_time with invalid incremental_key type",
			task: &pipeline.Asset{
				Name: "test.products",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "product_id", Type: "INT", PrimaryKey: true},
					{Name: "dt", Type: "STRING"},
				},
			},
			query:   "SELECT 1 as product_id, '2024-01-01' as dt",
			wantErr: true,
		},
		{
			name: "scd2_by_time with reserved column name",
			task: &pipeline.Asset{
				Name: "test.products",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "product_id", Type: "INT", PrimaryKey: true},
					{Name: "dt", Type: "DATE"},
					{Name: "_valid_from", Type: "TIMESTAMP"},
				},
			},
			query:   "SELECT 1 as product_id, '2024-01-01' as dt, current_timestamp() as _valid_from",
			wantErr: true,
		},
		{
			name: "scd2_by_time with primary keys",
			task: &pipeline.Asset{
				Name: "test.products",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "product_id", Type: "INT", PrimaryKey: true},
					{Name: "product_name", Type: "VARCHAR", PrimaryKey: true},
					{Name: "dt", Type: "DATE"},
					{Name: "stock", Type: "INT"},
				},
			},
			query: "SELECT 1 AS product_id, 'Laptop' AS product_name, DATE '2025-04-02' AS dt, 100 AS stock",
			want: []string{
				"MERGE INTO test\\.products AS target",
				"WITH s1 AS",
				"SELECT s1\\.\\*, TRUE AS _is_current",
				"UNION ALL",
				"JOIN   test\\.products AS t1 USING \\(product_id, product_name\\)",
				"WHERE  t1\\._valid_from < s1\\.dt AND t1\\._is_current",
				"ON  target\\.product_id = source\\.product_id AND target\\.product_name = source\\.product_name AND target\\._is_current AND source\\._is_current",
				"WHEN MATCHED AND",
				"target\\._valid_from < source\\.dt",
				"UPDATE SET",
				"_valid_until = source\\.dt",
				"_is_current  = FALSE",
				"WHEN NOT MATCHED THEN",
				"WHEN NOT MATCHED BY SOURCE AND target\\._is_current = TRUE THEN",
				"INSERT \\(product_id, product_name, dt, stock, _valid_from, _valid_until, _is_current\\)",
				"VALUES \\(source\\.product_id, source\\.product_name, source\\.dt, source\\.stock, source\\.dt, TIMESTAMP '9999-12-31 00:00:00', TRUE\\)",
			},
		},
		{
			name: "scd2_by_time full refresh",
			task: &pipeline.Asset{
				Name: "test.products",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{Name: "product_id", Type: "INT", PrimaryKey: true},
					{Name: "product_name", Type: "VARCHAR"},
					{Name: "dt", Type: "DATE"},
				},
			},
			fullRefresh: true,
			query:       "SELECT 1 AS product_id, 'Laptop' AS product_name, DATE '2025-04-02' AS dt",
			want: []string{
				"CREATE OR REPLACE TABLE test\\.products AS",
				"SELECT",
				"dt AS _valid_from",
				"src\\.\\*",
				"TIMESTAMP '9999-12-31 00:00:00' AS _valid_until",
				"TRUE AS _is_current",
				"FROM \\(",
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
				// Join all rendered queries for pattern matching
				fullOutput := strings.Join(render, "\n")
				for _, want := range tt.want {
					require.Regexp(t, want, fullOutput, "Pattern %q not found in output:\n%s", want, fullOutput)
				}
			}
		})
	}
}
