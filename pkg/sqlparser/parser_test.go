package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetLineageForRunner(t *testing.T) {
	lineage, err := NewSQLParser(true)
	require.NoError(t, err)
	require.NoError(t, lineage.Start())

	tests := []struct {
		name    string
		sql     string
		dialect string
		schema  Schema
		want    *Lineage
		wantErr bool
	}{
		{
			name: "nested subqueries",
			sql: `
		    select *
		    from table1
		    join (
		        select *
		        from (
		            select *
		            from table2
		        ) t2
		    ) t3
		        using(a)
		`,
			schema: Schema{
				"table1": {"a": "str", "b": "int64"},
				"table2": {"a": "str", "c": "int64"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "a",
						Upstream: []UpstreamColumn{
							{Column: "a", Table: "table1"},
							{Column: "a", Table: "table2"},
						},
						Type: "TEXT",
					},
					{
						Name: "b",
						Upstream: []UpstreamColumn{
							{Column: "b", Table: "table1"},
						},
						Type: "BIGINT",
					},
					{
						Name: "c",
						Upstream: []UpstreamColumn{
							{Column: "c", Table: "table2"},
						},
						Type: "BIGINT",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "a",
						Upstream: []UpstreamColumn{
							{
								Column: "a",
								Table:  "table1",
							},
							{
								Column: "a",
								Table:  "table2",
							},
						},
						Type: "",
					},
				},
			},
		},
		{
			name: "case-when",
			sql: `
				SELECT
					items.item_id as item_id,
					CASE
						WHEN price > 1000 AND t2.somecol < 250 THEN 'high'
						WHEN price > 100 THEN 'medium'
						ELSE 'low'
					END as price_category
				FROM items
					JOIN orders as t2 on items.item_id = t2.item_id
				WHERE in_stock = true
			`,
			schema: Schema{
				"items":  {"item_id": "str", "price": "int64", "in_stock": "bool"},
				"orders": {"item_id": "str", "somecol": "int64"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "item_id",
						Upstream: []UpstreamColumn{
							{Column: "item_id", Table: "items"},
						},
						Type: "TEXT",
					},
					{
						Name: "price_category",
						Upstream: []UpstreamColumn{
							{Column: "price", Table: "items"},
							{Column: "somecol", Table: "orders"},
						},
						Type: "VARCHAR",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "in_stock",
						Upstream: []UpstreamColumn{
							{
								Column: "in_stock",
								Table:  "items",
							},
						},
						Type: "",
					},
					{
						Name: "item_id",
						Upstream: []UpstreamColumn{
							{
								Column: "item_id",
								Table:  "items",
							},
							{
								Column: "item_id",
								Table:  "orders",
							},
						},
						Type: "",
					},
				},
			},
		},
		{
			name: "simple join",
			sql: `
				SELECT t1.col1, t2.col2
				FROM table1 t1
				JOIN table2 t2 ON t1.id = t2.id
			`,
			schema: Schema{
				"table1": {"id": "str", "col1": "int64"},
				"table2": {"id": "str", "col2": "int64"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "col1",
						Upstream: []UpstreamColumn{
							{Column: "col1", Table: "table1"},
						},
						Type: "BIGINT",
					},
					{
						Name: "col2",
						Upstream: []UpstreamColumn{
							{Column: "col2", Table: "table2"},
						},
						Type: "BIGINT",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "id",
						Upstream: []UpstreamColumn{
							{
								Column: "id",
								Table:  "table1",
							},
							{
								Column: "id",
								Table:  "table2",
							},
						},
						Type: "",
					},
				},
			},
		},
		{
			name: "aggregate function",
			sql: `
				SELECT customer_id as cid, COUNT(order_id) as order_count
				FROM orders
				GROUP BY customer_id
			`,
			schema: Schema{
				"orders": {"customer_id": "str", "order_id": "int64"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "cid",
						Upstream: []UpstreamColumn{
							{Column: "customer_id", Table: "orders"},
						},
						Type: "TEXT",
					},
					{
						Name: "order_count",
						Upstream: []UpstreamColumn{
							{Column: "order_id", Table: "orders"},
						},
						Type: "BIGINT",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "customer_id",
						Upstream: []UpstreamColumn{
							{
								Column: "customer_id",
								Table:  "orders",
							},
						},
						Type: "",
					},
				},
			},
		},
		{
			name: "subquery in select",
			sql: `
				SELECT
					emp_id,
					(SELECT AVG(salary) FROM salaries WHERE salaries.emp_id = employees.emp_id) as avg_salary
				FROM employees
			`,
			schema: Schema{
				"employees": {"emp_id": "str"},
				"salaries":  {"emp_id": "str", "salary": "int64"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "avg_salary",
						Upstream: []UpstreamColumn{
							{Column: "salary", Table: "salaries"},
						},
						Type: "DOUBLE",
					},
					{
						Name: "emp_id",
						Upstream: []UpstreamColumn{
							{Column: "emp_id", Table: "employees"},
						},
						Type: "TEXT",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "emp_id",
						Upstream: []UpstreamColumn{
							{Column: "emp_id", Table: "employees"},
							{Column: "emp_id", Table: "salaries"},
						},
						Type: "",
					},
				},
			},
		},
		{
			name: "union all",
			sql: `
				SELECT id, name FROM customers
				UNION ALL
				SELECT id, name FROM employees
			`,
			schema: Schema{
				"customers": {"id": "str", "name": "str"},
				"employees": {"id": "str", "name": "str"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "id",
						Upstream: []UpstreamColumn{
							{Column: "id", Table: "customers"},
							{Column: "id", Table: "employees"},
						},
						Type: "TEXT",
					},
					{
						Name: "name",
						Upstream: []UpstreamColumn{
							{Column: "name", Table: "customers"},
							{Column: "name", Table: "employees"},
						},
						Type: "TEXT",
					},
				},
				NonSelectedColumns: []ColumnLineage{},
			},
		},
		{
			name: "self join",
			sql: `
				SELECT e1.id, e2.manager_id
				FROM employees e1
				JOIN employees e2 ON e1.manager_id = e2.id
			`,
			schema: Schema{
				"employees": {"id": "str", "manager_id": "str"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "id",
						Upstream: []UpstreamColumn{
							{Column: "id", Table: "employees"},
						},
						Type: "TEXT",
					},
					{
						Name: "manager_id",
						Upstream: []UpstreamColumn{
							{Column: "manager_id", Table: "employees"},
						},
						Type: "TEXT",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "id",
						Upstream: []UpstreamColumn{
							{
								Column: "id",
								Table:  "employees",
							},
						},
						Type: "",
					},
					{
						Name: "manager_id",
						Upstream: []UpstreamColumn{
							{
								Column: "manager_id",
								Table:  "employees",
							},
						},
						Type: "",
					},
				},
			},
		},
		{
			name: "complex case-when",
			sql: `
		SELECT
			sales.id,
			CASE
				WHEN sales.amount > 500 THEN 'large'
				WHEN sales.amount > 100 THEN 'medium'
				ELSE 'small'
			END as sale_size,
			CASE
				WHEN regions.name = 'North' THEN 'N'
				WHEN regions.name = 'South' THEN 'S'
				ELSE 'Other'
			END as region_abbr,
		    'fixed' as fixed
		FROM sales
		JOIN regions ON sales.region_id = regions.id
			`,
			schema: Schema{
				"sales":   {"id": "str", "amount": "int64", "region_id": "str"},
				"regions": {"id": "str", "name": "str"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name:     "fixed",
						Upstream: []UpstreamColumn{},
						Type:     "VARCHAR",
					},
					{
						Name: "id",
						Upstream: []UpstreamColumn{
							{Column: "id", Table: "sales"},
						},
						Type: "TEXT",
					},
					{
						Name: "region_abbr",
						Upstream: []UpstreamColumn{
							{Column: "name", Table: "regions"},
						},
						Type: "VARCHAR",
					},
					{
						Name: "sale_size",
						Upstream: []UpstreamColumn{
							{Column: "amount", Table: "sales"},
						},
						Type: "VARCHAR",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "id",
						Upstream: []UpstreamColumn{
							{Column: "id", Table: "regions"},
						},
					},
					{
						Name: "region_id",
						Upstream: []UpstreamColumn{
							{Column: "region_id", Table: "sales"},
						},
					},
				},
			},
		},
		{
			name: "cte",
			sql: `with t1 as (
				select *
				from table1
				join table2
					using(a)
			),
			t2 as (
				select *
				from table2
				left join table1
					using(a)
			)
			select t1.*, t2.b as b2, t2.c as c2, now() as updated_at
			from t1
			join t2
				using(a)`,
			schema: Schema{
				"table1": {"a": "str", "b": "int64"},
				"table2": {"a": "str", "c": "str"},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "a",
						Upstream: []UpstreamColumn{
							{Column: "a", Table: "table1"},
							{Column: "a", Table: "table2"},
						},
						Type: "TEXT",
					},
					{
						Name: "b",
						Upstream: []UpstreamColumn{
							{Column: "b", Table: "table1"},
						},
						Type: "BIGINT",
					},
					{
						Name: "b2",
						Upstream: []UpstreamColumn{
							{Column: "b", Table: "table1"},
						},
						Type: "BIGINT",
					},
					{
						Name: "c",
						Upstream: []UpstreamColumn{
							{Column: "c", Table: "table2"},
						},
						Type: "TEXT",
					},
					{
						Name: "c2",
						Upstream: []UpstreamColumn{
							{Column: "c", Table: "table2"},
						},
						Type: "TEXT",
					},
					{
						Name:     "updated_at",
						Upstream: []UpstreamColumn{},
						Type:     "UNKNOWN",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "a",
						Upstream: []UpstreamColumn{
							{Column: "a", Table: "table1"},
							{Column: "a", Table: "table2"},
						},
						Type: "",
					},
				},
			},
		},
		{
			name:    "snowflake cte",
			dialect: "snowflake",
			sql: `WITH ufd AS (
    SELECT
        user_id,
        MIN(date_utc) as my_date_col
    FROM fact.some_daily_metrics
    GROUP BY 1
),
user_retention AS (
    SELECT
        d.user_id,
        MAX(CASE WHEN DATEDIFF(day, f.my_date_col, d.date_utc) = 1 THEN 1 ELSE 0 END) as some_day1_metric,
    FROM fact.some_daily_metrics d
    INNER JOIN ufd f ON d.user_id = f.user_id
    GROUP BY 1
)
SELECT
    d.user_id, 
    DATEDIFF(day, MAX(d.date_utc), CURRENT_DATE()) as recency,
    COUNT(DISTINCT d.date_utc) as active_days, 
    MIN_BY(d.first_device_type, d.first_activity_timestamp) as first_device_type, 
    AVG(NULLIF(d.estimated_session_duration, 0)) as avg_session_duration, 
    SUM(d.event_start) as total_event_start, 
    MAX(r.some_day1_metric) as some_day1_metric, 
    case when sum(d.event_start) > 0 then 'Player' else 'Visitor' end as user_type, 
FROM fact.some_daily_metrics d
LEFT JOIN user_retention r ON d.user_id = r.user_id
GROUP BY 1`,
			schema: Schema{
				"fact.some_daily_metrics": {
					"user_id":                    "integer",
					"date_utc":                   "date",
					"first_device_type":          "string",
					"first_activity_timestamp":   "timestamp",
					"estimated_session_duration": "float",
					"event_start":                "integer",
				},
			},
			want: &Lineage{
				Columns: []ColumnLineage{
					{
						Name: "ACTIVE_DAYS",
						Upstream: []UpstreamColumn{
							{Column: "DATE_UTC", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "BIGINT",
					},
					{
						Name: "AVG_SESSION_DURATION",
						Upstream: []UpstreamColumn{
							{Column: "ESTIMATED_SESSION_DURATION", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "DOUBLE",
					},
					{
						Name: "FIRST_DEVICE_TYPE",
						Upstream: []UpstreamColumn{
							{Column: "FIRST_ACTIVITY_TIMESTAMP", Table: "FACT.SOME_DAILY_METRICS"},
							{Column: "FIRST_DEVICE_TYPE", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "UNKNOWN",
					},
					{
						Name: "RECENCY",
						Upstream: []UpstreamColumn{
							{Column: "DATE_UTC", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "INT",
					},
					{
						Name: "SOME_DAY1_METRIC",
						Upstream: []UpstreamColumn{
							{Column: "DATE_UTC", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "INT",
					},
					{
						Name: "TOTAL_EVENT_START",
						Upstream: []UpstreamColumn{
							{Column: "EVENT_START", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "BIGINT",
					},
					{
						Name: "USER_ID",
						Upstream: []UpstreamColumn{
							{Column: "USER_ID", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "INT",
					},
					{
						Name: "USER_TYPE",
						Upstream: []UpstreamColumn{
							{Column: "EVENT_START", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "VARCHAR",
					},
				},
				NonSelectedColumns: []ColumnLineage{
					{
						Name: "USER_ID",
						Upstream: []UpstreamColumn{
							{Column: "USER_ID", Table: "FACT.SOME_DAILY_METRICS"},
						},
						Type: "",
					},
				},
			},
		},
	}

	t.Run("blocking group", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				got, err := lineage.ColumnLineage(tt.sql, tt.dialect, tt.schema)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				require.Equal(t, tt.want.Columns, got.Columns)
				require.Equal(t, tt.want.NonSelectedColumns, got.NonSelectedColumns)
			})
		}
	})
}

func TestSqlParser_GetTables(t *testing.T) {
	s, err := NewSQLParser(true)
	require.NoError(t, err)

	err = s.Start()
	require.NoError(t, err)

	tests := []struct {
		name    string
		sql     string
		want    []string
		wantErr bool
	}{
		{
			name: "nested subqueries",
			sql: `
            select *
            from table1
            join (
                select *
                from (
                    select *
                    from table2
                ) t2
            ) t3
                using(a)
        `,
			want: []string{"table1", "table2"},
		},
		{
			name: "nested subqueries with repeated aliases",
			sql: `
					select *
			from table1
			join (
				select *
				from (
					select *
					from table2
				) t2
			) t2
				using(a)
			join (
				select *
				from (
					select *
					from table3
				) t2
			) t3
				using(b)`,
			want: []string{"table1", "table2", "table3"},
		},
		{
			name: "unions",
			sql: `
					select * from table1
        union all
        select * from table2
        union all
        select * from table3`,
			want: []string{"table1", "table2", "table3"},
		},
		{
			name: "multiple nested joins",
			sql: `with t1 as (
        select *
        from table1
        join table2
            using(a)
    ),
    t2 as (
        select *
        from table2
        left join table1
            using(a)
    )
    select t1.*, t2.b as b2, t2.c as c2
    from t1
    join t2
        using(a)`,
			want: []string{"table1", "table2"},
		},
		{
			name: "multiple joins",
			sql: `SELECT *
from raw.Bookings as bookings
    inner join raw.Sessions as sessions on bookings.SessionId = sessions.Id
    inner join dashboard.users as coaches on Coaches.Id = bookings.CoachId
    inner join raw.Languages as languages on bookings.LanguageId = languages.Id
    inner join raw.Programmes as programmes on Bookings.ProgrammeId = Programmes.Id
    inner join dashboard.organizations as organizations on Programmes.OrganizationId = Organizations.Id
    left join dashboard.users as users on Users.Id = bookings.UserId
    left join raw.Teams teams on teams.Id = bookings.TeamId`,
			want: []string{
				"dashboard.organizations",
				"dashboard.users",
				"raw.Bookings",
				"raw.Languages",
				"raw.Programmes",
				"raw.Sessions",
				"raw.Teams",
			},
		},
	}

	t.Run("blocking group", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				got, err := s.UsedTables(tt.sql, "bigquery")
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				require.Equal(t, tt.want, got)
			})
		}
	})

	// wg.Wait()
	s.Close()
	require.NoError(t, err)
}
