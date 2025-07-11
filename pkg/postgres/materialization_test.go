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
				"WHEN MATCHED THEN UPDATE SET name = source\\.name\n" +
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
				"name STRING\n" +
				"\\);\n" +
				"COMMENT ON COLUMN two_col_table\\.name IS \\'The name of the person\\';",
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
				"category STRING,\n" +
				"primary key \\(id\\)\n" +
				"\\);\n" +
				"COMMENT ON COLUMN my_primary_key_table\\.category IS \\'Category of the item\\';",
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
				"category STRING,\n" +
				"primary key \\(id, category\\)\n" +
				"\\);\n" +
				"COMMENT ON COLUMN my_composite_primary_key_table\\.category IS \\'Category of the item\\';",
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
		name    string
		asset   *pipeline.Asset
		query   string
		want    string
		wantErr bool
	}{
		{
			name: "scd2_by_time_no_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByTime,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "event_name"},
				},
			},
			query:   "SELECT id, event_name FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_time_no_primary_key",
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
			query:   "SELECT id, event_name, ts FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_time_reserved_column_name_is_current",
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
			query:   "SELECT id, _is_current, ts FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_time_reserved_column_name_valid_from",
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
			query:   "SELECT id, _valid_from, ts FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_time_reserved_column_name_valid_until",
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
			query:   "SELECT id, _valid_until, ts FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_time_invalid_incremental_key_type",
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
					{Name: "ts", Type: "VARCHAR"},
				},
			},
			query:   "SELECT id, event_name, ts FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_time_simple_case_timestamp",
			asset: &pipeline.Asset{
				Name: "customer_dim",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "updated_at",
				},
				Columns: []pipeline.Column{
					{Name: "customer_id", PrimaryKey: true},
					{Name: "customer_name"},
					{Name: "email"},
					{Name: "updated_at", Type: "TIMESTAMP"},
				},
			},
			query: "SELECT customer_id, customer_name, email, updated_at FROM customers",
			want: "MERGE INTO customer_dim AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT customer_id, customer_name, email, updated_at FROM customers\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   customer_dim AS t1 USING (customer_id)\n" +
				"  WHERE  t1._valid_from < s1.updated_at AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.customer_id = source.customer_id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < source.updated_at\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = source.updated_at,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (customer_id, customer_name, email, updated_at, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.customer_id, source.customer_name, source.email, source.updated_at, source.updated_at, '9999-12-31 00:00:00', TRUE);",
		},
		{
			name: "scd2_by_time_simple_case_date",
			asset: &pipeline.Asset{
				Name: "product_dim",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "updated_date",
				},
				Columns: []pipeline.Column{
					{Name: "product_id", PrimaryKey: true},
					{Name: "product_name"},
					{Name: "price"},
					{Name: "updated_date", Type: "DATE"},
				},
			},
			query: "SELECT product_id, product_name, price, updated_date FROM products",
			want: "MERGE INTO product_dim AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT product_id, product_name, price, updated_date FROM products\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   product_dim AS t1 USING (product_id)\n" +
				"  WHERE  t1._valid_from < s1.updated_date AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.product_id = source.product_id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < source.updated_date\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = source.updated_date,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (product_id, product_name, price, updated_date, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.product_id, source.product_name, source.price, source.updated_date, source.updated_date, '9999-12-31 00:00:00', TRUE);",
		},
		{
			name: "scd2_by_time_composite_primary_key",
			asset: &pipeline.Asset{
				Name: "employee_assignment_dim",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "assignment_date",
				},
				Columns: []pipeline.Column{
					{Name: "employee_id", PrimaryKey: true},
					{Name: "department_id", PrimaryKey: true},
					{Name: "role"},
					{Name: "salary"},
					{Name: "assignment_date", Type: "DATE"},
				},
			},
			query: "SELECT employee_id, department_id, role, salary, assignment_date FROM employee_assignments",
			want: "MERGE INTO employee_assignment_dim AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT employee_id, department_id, role, salary, assignment_date FROM employee_assignments\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   employee_assignment_dim AS t1 USING (employee_id, department_id)\n" +
				"  WHERE  t1._valid_from < s1.assignment_date AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.employee_id = source.employee_id AND target.department_id = source.department_id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < source.assignment_date\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = source.assignment_date,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (employee_id, department_id, role, salary, assignment_date, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.employee_id, source.department_id, source.role, source.salary, source.assignment_date, source.assignment_date, '9999-12-31 00:00:00', TRUE);",
		},
		{
			name: "scd2_by_time_complex_query_with_joins",
			asset: &pipeline.Asset{
				Name: "order_dimension",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "last_modified",
				},
				Columns: []pipeline.Column{
					{Name: "order_id", PrimaryKey: true},
					{Name: "customer_name"},
					{Name: "total_amount"},
					{Name: "status"},
					{Name: "last_modified", Type: "TIMESTAMP"},
				},
			},
			query: `SELECT 
				o.order_id,
				c.customer_name,
				o.total_amount,
				o.status,
				o.last_modified
			FROM orders o
			JOIN customers c ON o.customer_id = c.id
			WHERE o.last_modified >= '2024-01-01'`,
			want: "MERGE INTO order_dimension AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT \n" +
				"\t\t\t\to.order_id,\n" +
				"\t\t\t\tc.customer_name,\n" +
				"\t\t\t\to.total_amount,\n" +
				"\t\t\t\to.status,\n" +
				"\t\t\t\to.last_modified\n" +
				"\t\t\tFROM orders o\n" +
				"\t\t\tJOIN customers c ON o.customer_id = c.id\n" +
				"\t\t\tWHERE o.last_modified >= '2024-01-01'\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   order_dimension AS t1 USING (order_id)\n" +
				"  WHERE  t1._valid_from < s1.last_modified AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.order_id = source.order_id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < source.last_modified\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = source.last_modified,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (order_id, customer_name, total_amount, status, last_modified, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.order_id, source.customer_name, source.total_amount, source.status, source.last_modified, source.last_modified, '9999-12-31 00:00:00', TRUE);",
		},
		{
			name: "scd2_by_time_query_with_semicolon",
			asset: &pipeline.Asset{
				Name: "location_dim",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "updated_ts",
				},
				Columns: []pipeline.Column{
					{Name: "location_id", PrimaryKey: true},
					{Name: "address"},
					{Name: "city"},
					{Name: "updated_ts", Type: "TIMESTAMP"},
				},
			},
			query: "SELECT location_id, address, city, updated_ts FROM locations;",
			want: "MERGE INTO location_dim AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT location_id, address, city, updated_ts FROM locations\n" +
				"  )\n" +
				"  SELECT s1.*, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM s1\n" +
				"  JOIN   location_dim AS t1 USING (location_id)\n" +
				"  WHERE  t1._valid_from < s1.updated_ts AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.location_id = source.location_id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"  target._valid_from < source.updated_ts\n" +
				") THEN\n" +
				"  UPDATE SET\n" +
				"    _valid_until = source.updated_ts,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY SOURCE AND target._is_current = TRUE THEN\n" +
				"  UPDATE SET \n" +
				"    _valid_until = CURRENT_TIMESTAMP,\n" +
				"    _is_current  = FALSE\n" +
				"\n" +
				"WHEN NOT MATCHED BY TARGET THEN\n" +
				"  INSERT (location_id, address, city, updated_ts, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.location_id, source.address, source.city, source.updated_ts, source.updated_ts, '9999-12-31 00:00:00', TRUE);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildSCD2QueryByTime(tt.asset, tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, strings.TrimSpace(tt.want), got)
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
			name: "scd2_by_column_no_primary_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id"},
					{Name: "name"},
				},
			},
			query:   "SELECT id, name FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_column_reserved_column_name",
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
			query:   "SELECT id, _is_current FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_column_simple_case",
			asset: &pipeline.Asset{
				Name: "customer_dim",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "customer_id", PrimaryKey: true},
					{Name: "customer_name"},
					{Name: "email"},
				},
			},
			query: "SELECT customer_id, customer_name, email FROM customers",
			want: "MERGE INTO customer_dim AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT customer_id, customer_name, email FROM customers\n" +
				"  )\n" +
				"  SELECT *, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM   s1\n" +
				"  JOIN   customer_dim AS t1 USING (customer_id)\n" +
				"  WHERE  (t1.customer_name != s1.customer_name OR t1.email != s1.email) AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.customer_id = source.customer_id AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"    target.customer_name != source.customer_name OR target.email != source.email\n" +
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
				"  INSERT (customer_id, customer_name, email, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.customer_id, source.customer_name, source.email, CURRENT_TIMESTAMP, '9999-12-31 00:00:00'::TIMESTAMP, TRUE);",
		},
		{
			name: "scd2_by_column_only_primary_keys",
			asset: &pipeline.Asset{
				Name: "lookup_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "code", PrimaryKey: true},
				},
			},
			query: "SELECT code FROM codes",
			want: "MERGE INTO lookup_table AS target\n" +
				"USING (\n" +
				"  WITH s1 AS (\n" +
				"    SELECT code FROM codes\n" +
				"  )\n" +
				"  SELECT *, TRUE AS _is_current\n" +
				"  FROM   s1\n" +
				"  UNION ALL\n" +
				"  SELECT s1.*, FALSE AS _is_current\n" +
				"  FROM   s1\n" +
				"  JOIN   lookup_table AS t1 USING (code)\n" +
				"  WHERE  FALSE AND t1._is_current\n" +
				") AS source\n" +
				"ON  target.code = source.code AND target._is_current AND source._is_current\n" +
				"\n" +
				"WHEN MATCHED AND (\n" +
				"    FALSE\n" +
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
				"  INSERT (code, _valid_from, _valid_until, _is_current)\n" +
				"  VALUES (source.code, CURRENT_TIMESTAMP, '9999-12-31 00:00:00'::TIMESTAMP, TRUE);",
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

func TestBuildSCD2ByTimefullRefresh(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		asset   *pipeline.Asset
		query   string
		want    string
		wantErr bool
	}{
		{
			name: "scd2_full_refresh_no_incremental_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByTime,
				},
				Columns: []pipeline.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "name"},
				},
			},
			query:   "SELECT id, name FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_full_refresh_no_primary_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "updated_at",
				},
				Columns: []pipeline.Column{
					{Name: "id"},
					{Name: "name"},
					{Name: "updated_at"},
				},
			},
			query:   "SELECT id, name, updated_at FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_full_refresh_success",
			asset: &pipeline.Asset{
				Name: "customer_dim",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategySCD2ByTime,
					IncrementalKey: "updated_at",
				},
				Columns: []pipeline.Column{
					{Name: "customer_id", PrimaryKey: true},
					{Name: "customer_name"},
					{Name: "email"},
					{Name: "updated_at"},
				},
			},
			query: "SELECT customer_id, customer_name, email, updated_at FROM customers",
			want: "BEGIN TRANSACTION;\n" +
				"DROP TABLE IF EXISTS customer_dim;\n" +
				"CREATE TABLE customer_dim AS\n" +
				"SELECT\n" +
				"  updated_at AS _valid_from,\n" +
				"  src.*,\n" +
				"  '9999-12-31 00:00:00'::TIMESTAMP AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
				"FROM (\n" +
				"SELECT customer_id, customer_name, email, updated_at FROM customers\n" +
				") AS src;\n" +
				"COMMIT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildSCD2ByTimefullRefresh(tt.asset, tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, strings.TrimSpace(tt.want), got)
			}
		})
	}
}

func TestBuildSCD2ByColumnfullRefresh(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		asset   *pipeline.Asset
		query   string
		want    string
		wantErr bool
	}{
		{
			name: "scd2_by_column_full_refresh_no_primary_key",
			asset: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id"},
					{Name: "name"},
				},
			},
			query:   "SELECT id, name FROM source_table",
			wantErr: true,
		},
		{
			name: "scd2_by_column_full_refresh_success",
			asset: &pipeline.Asset{
				Name: "customer_dim",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "customer_id", PrimaryKey: true},
					{Name: "customer_name"},
					{Name: "email"},
				},
			},
			query: "SELECT customer_id, customer_name, email FROM customers",
			want: "BEGIN TRANSACTION;\n" +
				"DROP TABLE IF EXISTS customer_dim;\n" +
				"CREATE TABLE customer_dim AS\n" +
				"SELECT\n" +
				"  CURRENT_TIMESTAMP AS _valid_from,\n" +
				"  src.*,\n" +
				"  '9999-12-31 00:00:00'::TIMESTAMP AS _valid_until,\n" +
				"  TRUE AS _is_current\n" +
				"FROM (\n" +
				"SELECT customer_id, customer_name, email FROM customers\n" +
				") AS src;\n" +
				"COMMIT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildSCD2ByColumnfullRefresh(tt.asset, tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, strings.TrimSpace(tt.want), got)
			}
		})
	}
}
