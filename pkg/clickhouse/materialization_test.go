package clickhouse

import (
	"encoding/json"
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
			name: "materialize to a table, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
				Columns: []pipeline.Column{
					{
						Name:       "id",
						PrimaryKey: true,
					},
				},
			},
			query: "SELECT 1",
			want: []string{
				"CREATE OR REPLACE TABLE my.asset PRIMARY KEY (id) AS SELECT 1",
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
				Columns: []pipeline.Column{
					{
						Name:       "id",
						PrimaryKey: true,
					},
				},
			},
			fullRefresh: true,
			query:       "SELECT 1",
			want: []string{
				"CREATE OR REPLACE TABLE my.asset PRIMARY KEY (id) AS SELECT 1",
			},
		},
		{
			name: "materialize to a table, full refresh parameter defaults to create+replace",
			task: &pipeline.Asset{
				Name:       "my.asset",
				Parameters: pipeline.ParameterMap{"full_refresh": true},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{
						Name:       "id",
						PrimaryKey: true,
					},
				},
			},
			query: "SELECT 1",
			want: []string{
				"CREATE OR REPLACE TABLE my.asset PRIMARY KEY (id) AS SELECT 1",
			},
		},
		{
			name: "materialize to a table, full refresh with composite primary key falls back to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "dt", Type: "date", PrimaryKey: true},
					{Name: "name", Type: "string"},
				},
			},
			fullRefresh: true,
			query:       "SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
			want: []string{
				"CREATE OR REPLACE TABLE my.asset PRIMARY KEY (id, dt) AS SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
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
			name: "delete+insert builds a proper transaction",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
				Columns: []pipeline.Column{
					{
						Name:       "id",
						PrimaryKey: true,
					},
				},
			},
			query: "SELECT 1",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY id AS SELECT 1",
				"DELETE FROM my.asset WHERE dt in (SELECT DISTINCT dt FROM my.__bruin_tmp_abcefghi)",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
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
			name: "merge without primary key errors",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int"},
					{Name: "name", Type: "string"},
				},
			},
			query:   "SELECT 1 as id, 'a' as name",
			wantErr: true,
		},
		{
			name: "merge with primary key builds delete+insert on primary key",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "name", Type: "string"},
				},
			},
			query: "SELECT 1 as id, 'a' as name",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY (id) AS SELECT 1 as id, 'a' as name",
				"INSERT INTO my.asset SELECT * FROM my.__bruin_tmp_abcefghi LIMIT 0",
				"DELETE FROM my.asset WHERE id IN (SELECT id FROM my.__bruin_tmp_abcefghi)",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
			},
		},
		{
			name: "merge with composite primary key builds delete on all keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "dt", Type: "date", PrimaryKey: true},
					{Name: "name", Type: "string"},
				},
			},
			query: "SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY (id, dt) AS SELECT 1 as id, '2026-01-01' as dt, 'a' as name",
				"INSERT INTO my.asset SELECT * FROM my.__bruin_tmp_abcefghi LIMIT 0",
				"DELETE FROM my.asset WHERE (id, dt) IN (SELECT id, dt FROM my.__bruin_tmp_abcefghi)",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
			},
		},
		{
			name: "merge with incremental_predicate appends to delete condition",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:                 pipeline.MaterializationTypeTable,
					Strategy:             pipeline.MaterializationStrategyMerge,
					IncrementalPredicate: "dt >= '2026-01-01'",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
				},
			},
			query: "SELECT 1 as id",
			want: []string{
				"CREATE TABLE my.__bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY (id) AS SELECT 1 as id",
				"INSERT INTO my.asset SELECT * FROM my.__bruin_tmp_abcefghi LIMIT 0",
				"DELETE FROM my.asset WHERE id IN (SELECT id FROM my.__bruin_tmp_abcefghi) AND (dt >= '2026-01-01')",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT * FROM my.__bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS my.__bruin_tmp_abcefghi",
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
				"DELETE FROM my.asset WHERE ts BETWEEN toDateTime64('{{ start_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6) AND toDateTime64('{{ end_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6)",
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT ts, event_name from source_table where ts between '{{start_timestamp}}' AND '{{end_timestamp}}'",
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
				"INSERT INTO my.asset SETTINGS insert_deduplicate = 0 SELECT dt, event_name from source_table where dt between '{{start_date}}' and '{{end_date}}'",
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
					")",
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
					")",
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
					")",
			},
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
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_primary_key_table (\n" +
					"id INT64,\n" +
					"category STRING COMMENT 'Category of the item'\n" +
					")" +
					"\nPRIMARY KEY (id)",
			},
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
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_composite_primary_key_table (\n" +
					"id INT64,\n" +
					"category STRING COMMENT 'Category of the item'\n" +
					")\n" +
					"PRIMARY KEY (id, category)",
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
				"CREATE TABLE IF NOT EXISTS my_partitioned_table (\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp'\n" +
					")" +
					"\nPARTITION BY (timestamp)" +
					"\nPRIMARY KEY (id)",
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
			fullRefresh: true,
			want: []string{
				"CREATE TABLE IF NOT EXISTS my_composite_partitioned_table (\n" +
					"id INT64,\n" +
					"timestamp TIMESTAMP COMMENT 'Event timestamp',\n" +
					"location STRING\n" +
					")" +
					"\nPARTITION BY (timestamp, location)" +
					"\nPRIMARY KEY (id)",
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
			}

			assert.Equal(t, tt.want, render)
		})
	}
}

func intp(i int) *int { return &i }

func TestColumnMetadataDDL(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name:            "orders",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDDL},
		Columns: []pipeline.Column{
			{Name: "amount", Type: "Decimal", Precision: intp(10), Scale: intp(2), Default: "0"},
		},
	}
	parts, err := NewMaterializer(false).Render(asset, "SELECT 1")
	require.NoError(t, err)
	require.NotEmpty(t, parts)
	createTable := parts[len(parts)-1]
	require.Contains(t, createTable, "Decimal(10, 2)")
	require.Contains(t, createTable, "DEFAULT 0")
	require.NotContains(t, createTable, "REFERENCES")
}

func TestMaterializer_TableDefinitionOptions(t *testing.T) {
	t.Parallel()
	strategies := []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyNone,
		pipeline.MaterializationStrategyCreateReplace,
		pipeline.MaterializationStrategyDDL,
	}
	tests := []struct {
		name        string
		options     pipeline.ClickHouseConfig
		partitionBy string
		clauses     []string
	}{
		{
			name:    "primary key only",
			clauses: []string{"PRIMARY KEY (id)"},
		},
		{
			name:    "engine with arguments",
			options: pipeline.ClickHouseConfig{Engine: "ReplacingMergeTree(version)"},
			clauses: []string{"ENGINE = ReplacingMergeTree(version)", "PRIMARY KEY (id)"},
		},
		{
			name:    "summing engine",
			options: pipeline.ClickHouseConfig{Engine: "SummingMergeTree()"},
			clauses: []string{"ENGINE = SummingMergeTree()", "PRIMARY KEY (id)"},
		},
		{
			name:    "sorting key with expression",
			options: pipeline.ClickHouseConfig{OrderBy: []string{"id", "toStartOfInterval(ts, INTERVAL 1 HOUR)"}},
			clauses: []string{"PRIMARY KEY (id)", "ORDER BY (id, toStartOfInterval(ts, INTERVAL 1 HOUR))"},
		},
		{
			name:    "ttl",
			options: pipeline.ClickHouseConfig{TTL: "ts + INTERVAL 30 DAY DELETE"},
			clauses: []string{"PRIMARY KEY (id)", "TTL ts + INTERVAL 30 DAY DELETE"},
		},
		{
			name: "settings sorted with SQL literals preserved",
			options: pipeline.ClickHouseConfig{Settings: map[string]string{
				"storage_policy": "'default'", "index_granularity": "4096",
			}},
			clauses: []string{"PRIMARY KEY (id)", "SETTINGS index_granularity = 4096, storage_policy = 'default'"},
		},
		{
			name:        "partitioning without clickhouse block",
			partitionBy: "toYYYYMM(ts), region",
			clauses:     []string{"PARTITION BY (toYYYYMM(ts), region)", "PRIMARY KEY (id)"},
		},
		{
			name: "all options in clause order",
			options: pipeline.ClickHouseConfig{
				Engine: "ReplacingMergeTree(version)", OrderBy: []string{"id", "ts"},
				TTL: "ts + INTERVAL 30 DAY", Settings: map[string]string{"index_granularity": "4096", "allow_nullable_key": "1"},
			},
			partitionBy: "toYYYYMM(ts)",
			clauses: []string{
				"ENGINE = ReplacingMergeTree(version)", "PARTITION BY (toYYYYMM(ts))",
				"PRIMARY KEY (id)", "ORDER BY (id, ts)", "TTL ts + INTERVAL 30 DAY",
				"SETTINGS allow_nullable_key = 1, index_granularity = 4096",
			},
		},
	}
	for _, strategy := range strategies {
		for _, tt := range tests {
			t.Run(string(strategy)+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name: "events", Type: pipeline.AssetTypeClickHouse, ClickHouse: tt.options,
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable, Strategy: strategy, PartitionBy: tt.partitionBy,
					},
					Columns: []pipeline.Column{
						{Name: "id", Type: "UInt64", PrimaryKey: true},
						{Name: "ts", Type: "DateTime"},
						{Name: "version", Type: "UInt64"},
						{Name: "region", Type: "String"},
					},
				}
				query := "SELECT id, ts, version, region FROM source"
				want := "CREATE OR REPLACE TABLE events " + strings.Join(tt.clauses, " ") + " AS " + query
				if strategy == pipeline.MaterializationStrategyDDL {
					want = "CREATE TABLE IF NOT EXISTS events (\nid UInt64,\nts DateTime,\nversion UInt64,\nregion String\n)\n" + strings.Join(tt.clauses, "\n")
				}
				actual, err := NewMaterializer(false).Render(asset, query+";\n")
				require.NoError(t, err)
				assert.Equal(t, []string{want}, actual)
			})
		}
	}
}

func TestMaterializer_SortingAndPrimaryKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		columns []pipeline.Column
		engine  string
		orderBy []string
		want    string
		wantErr string
	}{
		{
			name: "engine without keys", engine: "Memory()",
			want: "ENGINE = Memory()",
		},
		{
			name: "order by without column metadata", orderBy: []string{"id", "ts"},
			want: "ORDER BY (id, ts)",
		},
		{
			name: "empty sorting key expression", orderBy: []string{"tuple()"},
			want: "ORDER BY (tuple())",
		},
		{
			name: "columns without primary key", columns: []pipeline.Column{{Name: "id", Type: "UInt64"}},
			orderBy: []string{"id"}, want: "ORDER BY (id)",
		},
		{
			name:    "composite prefix",
			columns: []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "ts", PrimaryKey: true}},
			orderBy: []string{"id", "ts", "version"}, want: "PRIMARY KEY (id, ts) ORDER BY (id, ts, version)",
		},
		{
			name: "quoted prefix", columns: []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "ts", PrimaryKey: true}},
			orderBy: []string{" `id` ", `"ts"`}, want: "PRIMARY KEY (id, ts) ORDER BY ( `id` , \"ts\")",
		},
		{
			name: "primary key not leading", columns: []pipeline.Column{{Name: "id", PrimaryKey: true}},
			orderBy: []string{"ts", "id"}, wantErr: "primary key columns must be a prefix",
		},
		{
			name:    "sorting key shorter than primary key",
			columns: []pipeline.Column{{Name: "id", PrimaryKey: true}, {Name: "ts", PrimaryKey: true}},
			orderBy: []string{"id"}, wantErr: "primary key columns must be a prefix",
		},
		{
			name: "expression does not match primary key", columns: []pipeline.Column{{Name: "id", PrimaryKey: true}},
			orderBy: []string{"toString(id)"}, wantErr: "primary key columns must be a prefix",
		},
	}
	for _, strategy := range []pipeline.MaterializationStrategy{pipeline.MaterializationStrategyNone, pipeline.MaterializationStrategyCreateReplace, pipeline.MaterializationStrategyDDL} {
		for _, tt := range tests {
			t.Run(string(strategy)+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name: "events", Columns: tt.columns,
					Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy},
					ClickHouse:      pipeline.ClickHouseConfig{Engine: tt.engine, OrderBy: tt.orderBy},
				}
				actual, err := NewMaterializer(false).Render(asset, "SELECT 1 AS id")
				if tt.wantErr != "" {
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
				require.Len(t, actual, 1)
				assert.Contains(t, strings.ReplaceAll(actual[0], "\n", " "), tt.want)
				if len(asset.ColumnNamesWithPrimaryKey()) == 0 {
					assert.NotContains(t, actual[0], "PRIMARY KEY")
				}
			})
		}
	}
}

func TestMaterializer_IncrementalTableOptions(t *testing.T) {
	t.Parallel()
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyAppend, pipeline.MaterializationStrategyTruncateInsert,
		pipeline.MaterializationStrategyDeleteInsert, pipeline.MaterializationStrategyMerge, pipeline.MaterializationStrategyTimeInterval,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name: "events", Columns: []pipeline.Column{{Name: "id", PrimaryKey: true}},
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable, Strategy: strategy,
					IncrementalKey: "ts", TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			}
			query := "SELECT id, ts, version FROM source"
			withoutOptions, err := NewMaterializer(false).Render(asset, query)
			require.NoError(t, err)
			asset.ClickHouse = pipeline.ClickHouseConfig{
				Engine: "ReplacingMergeTree(version)", OrderBy: []string{"id", "ts"},
				TTL: "ts + INTERVAL 30 DAY", Settings: map[string]string{"index_granularity": "4096"},
			}
			asset.Materialization.PartitionBy = "toYYYYMM(ts)"
			withOptions, err := NewMaterializer(false).Render(asset, query)
			require.NoError(t, err)
			assert.Equal(t, withoutOptions, withOptions)
			if strategy == pipeline.MaterializationStrategyDeleteInsert || strategy == pipeline.MaterializationStrategyMerge {
				assert.Contains(t, withOptions[0], "ENGINE = MergeTree()")
			}
			want := []string{"CREATE OR REPLACE TABLE events ENGINE = ReplacingMergeTree(version) PARTITION BY (toYYYYMM(ts)) PRIMARY KEY (id) ORDER BY (id, ts) TTL ts + INTERVAL 30 DAY SETTINGS index_granularity = 4096 AS " + query}
			fullRefresh, err := NewMaterializer(true).Render(asset, query)
			require.NoError(t, err)
			assert.Equal(t, want, fullRefresh)
			asset.Parameters = pipeline.ParameterMap{"full_refresh": true}
			fullRefresh, err = NewMaterializer(false).Render(asset, query)
			require.NoError(t, err)
			assert.Equal(t, want, fullRefresh)
		})
	}
}

func TestMaterializer_ClusterStrategies(t *testing.T) {
	t.Parallel()
	const engine = "ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')"
	const query = "SELECT id, ts FROM source"
	tests := []struct {
		name        string
		matType     pipeline.MaterializationType
		strategy    pipeline.MaterializationStrategy
		granularity pipeline.MaterializationTimeGranularity
		local       []string
		cluster     []string
		clusterErr  string
	}{
		{
			name: "raw", matType: pipeline.MaterializationTypeNone,
			local: []string{query + ";\n"}, cluster: []string{query + ";\n"},
		},
		{
			name: "view", matType: pipeline.MaterializationTypeView,
			local:   []string{"CREATE OR REPLACE VIEW events AS\n" + query},
			cluster: []string{"CREATE OR REPLACE VIEW events ON CLUSTER `analytics` AS\n" + query},
		},
		{
			name: "default table", matType: pipeline.MaterializationTypeTable,
			local: []string{"CREATE OR REPLACE TABLE events ENGINE = " + engine + " PRIMARY KEY (id) AS " + query},
			cluster: []string{
				"DROP TABLE IF EXISTS events ON CLUSTER `analytics` SYNC",
				"CREATE TABLE events ON CLUSTER `analytics` ENGINE = " + engine + " PRIMARY KEY (id) EMPTY AS " + query,
				"INSERT INTO events " + query,
			},
		},
		{
			name: "create+replace", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyCreateReplace,
			local: []string{"CREATE OR REPLACE TABLE events ENGINE = " + engine + " PRIMARY KEY (id) AS " + query},
			cluster: []string{
				"DROP TABLE IF EXISTS events ON CLUSTER `analytics` SYNC",
				"CREATE TABLE events ON CLUSTER `analytics` ENGINE = " + engine + " PRIMARY KEY (id) EMPTY AS " + query,
				"INSERT INTO events " + query,
			},
		},
		{
			name: "append", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyAppend,
			local: []string{"INSERT INTO events " + query}, cluster: []string{"INSERT INTO events " + query},
		},
		{
			name: "truncate+insert", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyTruncateInsert,
			local:   []string{"TRUNCATE TABLE events", "INSERT INTO events " + query},
			cluster: []string{"TRUNCATE TABLE events ON CLUSTER `analytics` SYNC", "INSERT INTO events " + query},
		},
		{
			name: "delete+insert", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyDeleteInsert,
			local: []string{
				"CREATE TABLE __bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY id AS " + query,
				"DELETE FROM events WHERE ts in (SELECT DISTINCT ts FROM __bruin_tmp_abcefghi)",
				"INSERT INTO events SETTINGS insert_deduplicate = 0 SELECT * FROM __bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS __bruin_tmp_abcefghi",
			},
			clusterErr: "staging",
		},
		{
			name: "merge", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyMerge,
			local: []string{
				"CREATE TABLE __bruin_tmp_abcefghi ENGINE = MergeTree() PRIMARY KEY (id) AS " + query,
				"INSERT INTO events SELECT * FROM __bruin_tmp_abcefghi LIMIT 0",
				"DELETE FROM events WHERE id IN (SELECT id FROM __bruin_tmp_abcefghi)",
				"INSERT INTO events SETTINGS insert_deduplicate = 0 SELECT * FROM __bruin_tmp_abcefghi",
				"DROP TABLE IF EXISTS __bruin_tmp_abcefghi",
			},
			clusterErr: "staging",
		},
		{
			name: "time_interval date", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyTimeInterval,
			granularity: pipeline.MaterializationTimeGranularityDate,
			local: []string{
				"DELETE FROM events WHERE ts BETWEEN '{{start_date}}' AND '{{end_date}}'",
				"INSERT INTO events SETTINGS insert_deduplicate = 0 " + query,
			},
			cluster: []string{
				"ALTER TABLE events ON CLUSTER `analytics` DELETE WHERE ts BETWEEN '{{start_date}}' AND '{{end_date}}' SETTINGS mutations_sync = 2",
				"INSERT INTO events SETTINGS insert_deduplicate = 0 " + query,
			},
		},
		{
			name: "time_interval timestamp", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyTimeInterval,
			granularity: pipeline.MaterializationTimeGranularityTimestamp,
			local: []string{
				"DELETE FROM events WHERE ts BETWEEN toDateTime64('{{ start_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6) AND toDateTime64('{{ end_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6)",
				"INSERT INTO events SETTINGS insert_deduplicate = 0 " + query,
			},
			cluster: []string{
				"ALTER TABLE events ON CLUSTER `analytics` DELETE WHERE ts BETWEEN toDateTime64('{{ start_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6) AND toDateTime64('{{ end_timestamp | date_format('%Y-%m-%d %H:%M:%S.%f') }}', 6) SETTINGS mutations_sync = 2",
				"INSERT INTO events SETTINGS insert_deduplicate = 0 " + query,
			},
		},
		{
			name: "ddl", matType: pipeline.MaterializationTypeTable, strategy: pipeline.MaterializationStrategyDDL,
			local:   []string{"CREATE TABLE IF NOT EXISTS events (\nid UInt64,\nts DateTime\n)\nENGINE = " + engine + "\nPRIMARY KEY (id)"},
			cluster: []string{"CREATE TABLE IF NOT EXISTS events ON CLUSTER `analytics` (\nid UInt64,\nts DateTime\n)\nENGINE = " + engine + "\nPRIMARY KEY (id)"},
		},
	}
	for _, tt := range tests {
		for _, cluster := range []string{"", "analytics"} {
			t.Run(tt.name+"/cluster="+cluster, func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name: "events",
					Materialization: pipeline.Materialization{
						Type: tt.matType, Strategy: tt.strategy, IncrementalKey: "ts", TimeGranularity: tt.granularity,
					},
					Columns:    []pipeline.Column{{Name: "id", Type: "UInt64", PrimaryKey: true}, {Name: "ts", Type: "DateTime"}},
					ClickHouse: pipeline.ClickHouseConfig{Engine: engine},
				}
				before, err := json.Marshal(asset)
				require.NoError(t, err)
				actual, cleanup, err := NewMaterializer(false, cluster).RenderWithCleanup(asset, query+";\n")
				if cluster != "" && tt.clusterErr != "" {
					require.ErrorContains(t, err, tt.clusterErr)
					assert.Contains(t, err.Error(), string(tt.strategy))
					assert.Empty(t, actual)
					assert.Empty(t, cleanup)
				} else {
					require.NoError(t, err)
					want := tt.local
					if cluster != "" {
						want = tt.cluster
					}
					assert.Equal(t, want, actual)
					if cluster == "" && (tt.strategy == pipeline.MaterializationStrategyMerge || tt.strategy == pipeline.MaterializationStrategyDeleteInsert) {
						assert.Equal(t, []string{"DROP TABLE IF EXISTS __bruin_tmp_abcefghi"}, cleanup)
					} else {
						assert.Empty(t, cleanup)
					}
				}
				after, err := json.Marshal(asset)
				require.NoError(t, err)
				assert.JSONEq(t, string(before), string(after), "rendering must not modify the asset")
			})
		}
	}
}

func TestMaterializer_ClusterFullRefresh(t *testing.T) {
	t.Parallel()
	const engine = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{uuid}', '{replica}', version)"
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyNone, pipeline.MaterializationStrategyCreateReplace,
		pipeline.MaterializationStrategyAppend, pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyMerge, pipeline.MaterializationStrategyTruncateInsert,
		pipeline.MaterializationStrategyTimeInterval, pipeline.MaterializationStrategyDDL,
	} {
		for _, global := range []bool{false, true} {
			name := "asset parameter"
			if global {
				name = "global flag"
			}
			t.Run(string(strategy)+"/"+name, func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name:            "events",
					Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy},
					Columns:         []pipeline.Column{{Name: "id", Type: "UInt64", PrimaryKey: true}},
					ClickHouse:      pipeline.ClickHouseConfig{Engine: engine},
				}
				if !global {
					asset.Parameters = pipeline.ParameterMap{"full_refresh": true}
				}
				actual, cleanup, err := NewMaterializer(global, "analytics").RenderWithCleanup(asset, "SELECT id FROM source;\n")
				require.NoError(t, err)
				assert.Empty(t, cleanup)
				if strategy == pipeline.MaterializationStrategyDDL {
					assert.Equal(t, []string{"CREATE TABLE IF NOT EXISTS events ON CLUSTER `analytics` (\nid UInt64\n)\nENGINE = " + engine + "\nPRIMARY KEY (id)"}, actual)
				} else {
					assert.Equal(t, []string{
						"DROP TABLE IF EXISTS events ON CLUSTER `analytics` SYNC",
						"CREATE TABLE events ON CLUSTER `analytics` ENGINE = " + engine + " PRIMARY KEY (id) EMPTY AS SELECT id FROM source",
						"INSERT INTO events SELECT id FROM source",
					}, actual)
				}
				assert.Equal(t, strategy, asset.Materialization.Strategy)
			})
		}
	}
}

func TestMaterializer_ClusterEngineRequirements(t *testing.T) {
	t.Parallel()
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyNone, pipeline.MaterializationStrategyCreateReplace,
		pipeline.MaterializationStrategyDDL, pipeline.MaterializationStrategyAppend,
		pipeline.MaterializationStrategyTruncateInsert, pipeline.MaterializationStrategyTimeInterval,
	} {
		for _, engine := range []string{"", "MergeTree()", "SharedMergeTree()", "Distributed(analytics, default, events_local, rand())", "ReplicatedMergeTree('/tables/{shard}/events', '{replica}')", "ReplicatedReplacingMergeTree()"} {
			t.Run(string(strategy)+"/engine="+engine, func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name: "events",
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable, Strategy: strategy,
						IncrementalKey: "ts", TimeGranularity: pipeline.MaterializationTimeGranularityDate,
					},
					Columns:    []pipeline.Column{{Name: "id", Type: "UInt64", PrimaryKey: true}},
					ClickHouse: pipeline.ClickHouseConfig{Engine: engine},
				}
				mustReject := false
				wantErr := "engine"
				switch strategy {
				case pipeline.MaterializationStrategyNone, pipeline.MaterializationStrategyCreateReplace:
					mustReject = !strings.HasPrefix(engine, "Replicated")
				case pipeline.MaterializationStrategyDDL:
					mustReject = engine == ""
				case pipeline.MaterializationStrategyTruncateInsert, pipeline.MaterializationStrategyTimeInterval:
					mustReject = engine != "" && !strings.HasPrefix(engine, "Replicated")
					wantErr = "Replicated"
				}
				actual, err := NewMaterializer(false, "analytics").Render(asset, "SELECT id FROM source")
				if mustReject {
					require.ErrorContains(t, err, wantErr)
					assert.Empty(t, actual)
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, actual)
				if strategy == pipeline.MaterializationStrategyDDL || strategy == pipeline.MaterializationStrategyNone || strategy == pipeline.MaterializationStrategyCreateReplace {
					assert.Contains(t, strings.Join(actual, "\n"), "ENGINE = "+engine)
				}
			})
		}
	}
}

func TestMaterializer_ClusterTableOptions(t *testing.T) {
	t.Parallel()
	const engine = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/events', '{replica}', version)"
	for _, strategy := range []pipeline.MaterializationStrategy{pipeline.MaterializationStrategyCreateReplace, pipeline.MaterializationStrategyDDL} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()
			asset := &pipeline.Asset{
				Name:            "events",
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy, PartitionBy: "toYYYYMM(ts)"},
				Columns:         []pipeline.Column{{Name: "id", Type: "UInt64", PrimaryKey: true}, {Name: "ts", Type: "DateTime"}},
				ClickHouse: pipeline.ClickHouseConfig{
					Engine: engine, OrderBy: []string{"id", "ts"}, TTL: "ts + INTERVAL 30 DAY",
					Settings: map[string]string{"index_granularity": "4096", "allow_nullable_key": "1"},
				},
			}
			actual, err := NewMaterializer(false, "analytics").Render(asset, "SELECT id, ts FROM source")
			require.NoError(t, err)
			clauses := []string{
				"ENGINE = " + engine, "PARTITION BY (toYYYYMM(ts))", "PRIMARY KEY (id)",
				"ORDER BY (id, ts)", "TTL ts + INTERVAL 30 DAY", "SETTINGS allow_nullable_key = 1, index_granularity = 4096",
			}
			if strategy == pipeline.MaterializationStrategyDDL {
				assert.Equal(t, []string{"CREATE TABLE IF NOT EXISTS events ON CLUSTER `analytics` (\nid UInt64,\nts DateTime\n)\n" + strings.Join(clauses, "\n")}, actual)
			} else {
				assert.Equal(t, []string{
					"DROP TABLE IF EXISTS events ON CLUSTER `analytics` SYNC",
					"CREATE TABLE events ON CLUSTER `analytics` " + strings.Join(clauses, " ") + " EMPTY AS SELECT id, ts FROM source",
					"INSERT INTO events SELECT id, ts FROM source",
				}, actual)
			}
			asset.ClickHouse.OrderBy = []string{"ts", "id"}
			actual, err = NewMaterializer(false, "analytics").Render(asset, "SELECT id, ts FROM source")
			require.ErrorContains(t, err, "primary key columns must be a prefix")
			assert.Empty(t, actual)
		})
	}
}

func TestRenderer_ClusterIdentifier(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name: "events", Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
	}
	actual, err := NewRenderer(false, "analytics\\` ; DROP TABLE events; --").Render(asset, "SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, "CREATE OR REPLACE VIEW events ON CLUSTER `analytics\\\\\\` ; DROP TABLE events; --` AS\nSELECT 1", actual)
}

func TestMaterializer_ClusterTimeIntervalValidation(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		key         string
		granularity pipeline.MaterializationTimeGranularity
		wantErr     string
	}{
		{name: "missing incremental key", granularity: pipeline.MaterializationTimeGranularityDate, wantErr: "incremental_key is required"},
		{name: "missing granularity", key: "ts", wantErr: "time_granularity is required"},
		{name: "invalid granularity", key: "ts", granularity: "hour", wantErr: "time_granularity must be either"},
	} {
		for _, cluster := range []string{"", "analytics"} {
			t.Run(tt.name+"/cluster="+cluster, func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name: "events",
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTimeInterval,
						IncrementalKey: tt.key, TimeGranularity: tt.granularity,
					},
				}
				actual, cleanup, err := NewMaterializer(false, cluster).RenderWithCleanup(asset, "SELECT ts FROM source")
				require.ErrorContains(t, err, tt.wantErr)
				assert.Empty(t, actual)
				assert.Empty(t, cleanup)
			})
		}
	}
}

func TestMaterializer_ClusterFullRefreshRequiresReplicatedEngine(t *testing.T) {
	t.Parallel()
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyAppend, pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyMerge, pipeline.MaterializationStrategyTruncateInsert,
		pipeline.MaterializationStrategyTimeInterval,
	} {
		for _, engine := range []string{"", "MergeTree()"} {
			t.Run(string(strategy)+"/engine="+engine, func(t *testing.T) {
				t.Parallel()
				asset := &pipeline.Asset{
					Name:            "events",
					Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy},
					Columns:         []pipeline.Column{{Name: "id", Type: "UInt64", PrimaryKey: true}},
					ClickHouse:      pipeline.ClickHouseConfig{Engine: engine},
				}
				actual, cleanup, err := NewMaterializer(true, "analytics").RenderWithCleanup(asset, "SELECT id FROM source")
				require.ErrorContains(t, err, "Replicated")
				assert.Empty(t, actual)
				assert.Empty(t, cleanup)
				assert.Equal(t, strategy, asset.Materialization.Strategy)
			})
		}
	}
}

func TestMaterializer_ClusterIsolation(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name: "events", Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
	}
	first := NewMaterializer(false, "first")
	second := NewMaterializer(false, "second")
	local := NewMaterializer(false)
	for _, tt := range []struct {
		materializer *Materializer
		want         string
	}{
		{first, "CREATE OR REPLACE VIEW events ON CLUSTER `first` AS\nSELECT 1"},
		{second, "CREATE OR REPLACE VIEW events ON CLUSTER `second` AS\nSELECT 1"},
		{local, "CREATE OR REPLACE VIEW events AS\nSELECT 1"},
		{first, "CREATE OR REPLACE VIEW events ON CLUSTER `first` AS\nSELECT 1"},
	} {
		actual, err := tt.materializer.Render(asset, "SELECT 1")
		require.NoError(t, err)
		assert.Equal(t, []string{tt.want}, actual)
	}
}

func TestMaterializer_ClusterRefreshRestricted(t *testing.T) {
	t.Parallel()
	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyAppend,
		pipeline.MaterializationStrategyMerge,
		pipeline.MaterializationStrategyDeleteInsert,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()
			restricted := true
			asset := &pipeline.Asset{
				Name:              "events",
				Materialization:   pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: strategy},
				RefreshRestricted: &restricted,
			}
			actual, cleanup, err := NewMaterializer(true, "analytics").RenderWithCleanup(asset, "SELECT 1")
			assert.Empty(t, cleanup)
			if strategy == pipeline.MaterializationStrategyAppend {
				require.NoError(t, err)
				assert.Equal(t, []string{"INSERT INTO events SELECT 1"}, actual)
			} else {
				require.ErrorContains(t, err, "staging")
				assert.Empty(t, actual)
			}
		})
	}
}

func TestMaterializer_ClusterReplacementRequiresSortingKey(t *testing.T) {
	t.Parallel()
	asset := &pipeline.Asset{
		Name:            "events",
		Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
		ClickHouse:      pipeline.ClickHouseConfig{Engine: "ReplicatedMergeTree()"},
	}
	mat := NewMaterializer(false, "analytics")
	actual, err := mat.Render(asset, "SELECT 1 AS id")
	require.ErrorContains(t, err, "primary_key columns or clickhouse.order_by")
	assert.Empty(t, actual, "invalid sorting keys must not return a DROP statement")
	asset.ClickHouse.OrderBy = []string{"tuple()"}
	actual, err = mat.Render(asset, "SELECT 1 AS id")
	require.NoError(t, err)
	require.Len(t, actual, 3)
	assert.Contains(t, actual[1], "ORDER BY (tuple()) EMPTY AS SELECT 1 AS id")
}
