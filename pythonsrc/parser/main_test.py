import pytest
from sqlglot import parse_one
from sqlglot.optimizer import optimize

from .main import (
    get_column_lineage,
    extract_non_selected_columns,
    Column,
    get_tables,
    add_limit,
)

SCHEMA = {
    "orders": {
        "id": "bigint",
        "order_number": "string",
        "customer_id": "bigint",
        "shipping_country": "string",
    },
    "customers": {
        "id": "bigint",
        "name": "string",
        "age": "bigint",
        "country": "string",
    },
}

test_cases_non_selected_columns = [
    {
        "name": "Select all from orders",
        "dialect": "bigquery",
        "query": """
	        select * from orders
	    """,
        "schema": SCHEMA,
        "expected": [],
    },
    {
        "name": "Select orders with id greater than 10",
        "dialect": "bigquery",
        "query": """
	        select * from orders where id > 10
	    """,
        "schema": SCHEMA,
        "expected": [Column("id", "orders")],
    },
    {
        "name": "Join orders and customers with id filter",
        "dialect": "bigquery",
        "query": """
	        select * from orders join customers on customers.id = orders.customer_id where orders.id > 10;
	    """,
        "schema": SCHEMA,
        "expected": [
            Column(name="customer_id", table="orders"),
            Column(name="id", table="customers"),
            Column(name="id", table="orders"),
        ],
    },
    {
        "name": "Join orders and customers with country filter",
        "dialect": "bigquery",
        "query": """
	        select * from orders join customers on customers.id = orders.customer_id where orders.id > 10 and customers.country = "UK";
	    """,
        "schema": SCHEMA,
        "expected": [
            Column(name="country", table="customers"),
            Column(name="customer_id", table="orders"),
            Column(name="id", table="customers"),
            Column(name="id", table="orders"),
        ],
    },
    {
        "name": "Join with additional condition on shipping country",
        "dialect": "bigquery",
        "query": """
	        select * from orders join customers on customers.id = orders.customer_id where orders.id > 10 and customers.country = "UK" and concat(customers.country, orders.shipping_country)="UKUK";
	    """,
        "schema": SCHEMA,
        "expected": [
            Column(name="country", table="customers"),
            Column(name="customer_id", table="orders"),
            Column(name="id", table="customers"),
            Column(name="id", table="orders"),
            Column(name="shipping_country", table="orders"),
        ],
    },
    {
        "name": "CTE with cross join",
        "dialect": "bigquery",
        "query": """
	        with t1 as (
	            select col1, count(*) as cnt1 from table1 group by col1
	        ), t2 as (
	            select avg(col3) as col3_avg from table1 group by col1
	        )
	        select col1, cnt1, col3_avg from t1 cross join t2
	    """,
        "schema": {
            "table1": {"col3": "int", "col1": "int", "col2": "int"},
        },
        "expected": [
            Column(name="col1", table="table1"),
        ],
    },
    {
        "name": "CTE with cross",
        "dialect": "bigquery",
        "query": """
            SELECT t1.col1, t2.col2 				
            FROM table1 t1 				
            JOIN table2 t2 
                ON t1.id = t2.id
	""",
        "schema": {
            "table1": {"id": "str", "col1": "int64"},
            "table2": {"id": "str", "col2": "int64"},
        },
        "expected": [
            Column(name="id", table="table1"),
            Column(name="id", table="table2"),
        ],
    },
    {
        "name": "CTE with cross test 2 ",
        "dialect": "bigquery",
        "query": """
	   SELECT emp_id, (SELECT AVG(salary) FROM salaries WHERE salaries.emp_id = employees.emp_id) as avg_salary FROM employees
	""",
        "schema": {
            "employees": {"emp_id": "str"},
            "salaries": {"emp_id": "str", "salary": "int64"},
        },
        "expected": [
            Column(name="emp_id", table="employees"),
            Column(name="emp_id", table="salaries"),
        ],
    },
    {
        "name": "complex CTEs with aliases",
        "dialect": "snowflake",
        "query": """
WITH ufd AS (
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
GROUP BY 1
	""",
        "schema": {
            "fact": {
                "some_daily_metrics": {
                    "user_id": "STRING",
                    "date_utc": "DATE",
                    "first_device_type": "STRING",
                    "first_activity_timestamp": "TIMESTAMP",
                    "estimated_session_duration": "INT64",
                    "event_start": "INT64",
                }
            }
        },
        "expected": [
            Column(name="USER_ID", table="FACT.SOME_DAILY_METRICS"),
        ],
    },
    {
        "name": "dashboard.report",
        "dialect": "bigquery",
        "query": """
        SELECT
            t1.col1,
            t1.col2,
            t1.col3,
            t1.col4,
            t1.col5,
            t1.col6,
            t1.col7 is not null as is_refunded,
            1 as ai_credits,
            if(t2.col1 is not null, 1, 0) as ai_credits_used,
            t3.col1 as TeamName,
            t3.col2 as TeamId,
            t4.col1,
            t4.col2,
            t5.col1 as Organization,
            t5.col2 as OrganizationId,
            t4.col3,
            t4.col4,
            t6.col1 as ProgramName,
            t5.col3,
            t5.col4
        FROM `dataset1.table1` as t1
        INNER JOIN `dataset2.table2` as t6
            ON t1.col3 = t6.col1
        INNER JOIN `dataset3.table3` as t5
            ON t6.col2 = t5.col2
        LEFT JOIN `dataset4.table4` as t7
            ON t7.col1 = t1.col4
        LEFT JOIN `dataset5.table5` as t2
            ON t1.col1 = t2.col2
        LEFT JOIN `dataset6.table6` as t3
            ON t3.col2 = cast(t2.col3 as int64)
        LEFT JOIN `dataset7.table7` as t4
            ON t4.col5 = safe_cast(t1.col5 as int64)
    """,
        "schema": {
            "dataset1.table1": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
                "col6": "STRING",
                "col7": "STRING",
            },
            "dataset2.table2": {"col1": "STRING", "col2": "STRING"},
            "dataset3.table3": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
            },
            "dataset4.table4": {"col1": "STRING"},
            "dataset5.table5": {"col1": "STRING", "col2": "STRING", "col3": "STRING"},
            "dataset6.table6": {"col1": "STRING", "col2": "STRING"},
            "dataset7.table7": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
            },
        },
        "expected": [
            Column(name="col1", table="dataset1.table1"),
            Column(name="col1", table="dataset2.table2"),
            Column(name="col1", table="dataset4.table4"),
            Column(name="col2", table="dataset2.table2"),
            Column(name="col2", table="dataset3.table3"),
            Column(name="col2", table="dataset5.table5"),
            Column(name="col2", table="dataset6.table6"),
            Column(name="col3", table="dataset1.table1"),
            Column(name="col3", table="dataset5.table5"),
            Column(name="col4", table="dataset1.table1"),
            Column(name="col5", table="dataset1.table1"),
            Column(name="col5", table="dataset7.table7"),
        ],
    },
    {
        "name": "project_report",
        "dialect": "bigquery",
        "query": """
       SELECT
           p1.col1,
           p1.col2,
           p1.col3,
           p1.col4,
           p1.col5,
           p1.col6,
           p1.col7 is not null as is_active,
           1 as project_credits,
           if(p2.col1 is not null, 1, 0) as credits_used,
           p3.col1 as ProjectName,
           p3.col2 as ProjectId,
           p4.col1,
           p4.col2,
           p5.col1 as Department,
           p5.col2 as DepartmentId,
           p4.col3,
           p4.col4,
           p6.col1 as ProgramName,
           p5.col3,
           p5.col4
       FROM `project1.dataset1.table1` as p1
       INNER JOIN `project2.dataset2.table2` as p6
           ON p1.col3 = p6.col1
       INNER JOIN `project3.dataset3.table3` as p5
           ON p6.col2 = p5.col2
       LEFT JOIN `project4.dataset4.table4` as p7
           ON p7.col1 = p1.col4
       LEFT JOIN `project5.dataset5.table5` as p2
           ON p1.col1 = p2.col2
       LEFT JOIN `project6.dataset6.table6` as p3
           ON p3.col2 = cast(p2.col3 as int64)
       LEFT JOIN `project7.dataset7.table7` as p4
           ON p4.col5 = safe_cast(p1.col5 as int64)
   """,
        "schema": {
            "project1.dataset1.table1": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
                "col6": "STRING",
                "col7": "STRING",
            },
            "project2.dataset2.table2": {"col1": "STRING", "col2": "STRING"},
            "project3.dataset3.table3": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
            },
            "project4.dataset4.table4": {"col1": "STRING"},
            "project5.dataset5.table5": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
            },
            "project6.dataset6.table6": {"col1": "STRING", "col2": "STRING"},
            "project7.dataset7.table7": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
            },
        },
        "expected": [
            Column(name="col1", table="project1.dataset1.table1"),
            Column(name="col1", table="project2.dataset2.table2"),
            Column(name="col1", table="project4.dataset4.table4"),
            Column(name="col2", table="project2.dataset2.table2"),
            Column(name="col2", table="project3.dataset3.table3"),
            Column(name="col2", table="project5.dataset5.table5"),
            Column(name="col2", table="project6.dataset6.table6"),
            Column(name="col3", table="project1.dataset1.table1"),
            Column(name="col3", table="project5.dataset5.table5"),
            Column(name="col4", table="project1.dataset1.table1"),
            Column(name="col5", table="project1.dataset1.table1"),
            Column(name="col5", table="project7.dataset7.table7"),
        ],
    },
]
test_cases = [
    {
        "name": "nested subqueries",
        "dialect": "bigquery",
        "query": """
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
         """,
        "schema": {
            "table1": {"a": "str", "b": "int64"},
            "table2": {"a": "str", "c": "int64"},
        },
        "expected": [
            {
                "name": "a",
                "type": "TEXT",
                "upstream": [
                    {"column": "a", "table": "table1"},
                    {"column": "a", "table": "table2"},
                ],
            },
            {
                "name": "b",
                "type": "BIGINT",
                "upstream": [{"column": "b", "table": "table1"}],
            },
            {
                "name": "c",
                "type": "BIGINT",
                "upstream": [{"column": "c", "table": "table2"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "a",
                "upstream": [
                    {"column": "a", "table": "table1"},
                    {"column": "a", "table": "table2"},
                ],
            }
        ],
    },
    {
        "name": "case-when",
        "dialect": "bigquery",
        "query": """
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
         """,
        "schema": {
            "items": {"item_id": "str", "price": "int64", "in_stock": "bool"},
            "orders": {"item_id": "str", "somecol": "int64"},
        },
        "expected": [
            {
                "name": "item_id",
                "type": "TEXT",
                "upstream": [
                    {"column": "item_id", "table": "items"},
                ],
            },
            {
                "name": "price_category",
                "type": "VARCHAR",
                "upstream": [
                    {"column": "price", "table": "items"},
                    {"column": "somecol", "table": "orders"},
                ],
            },
        ],
        "expected_non_selected": [
            {
                "name": "in_stock",
                "upstream": [{"column": "in_stock", "table": "items"}],
            },
            {
                "name": "item_id",
                "upstream": [
                    {"column": "item_id", "table": "items"},
                    {"column": "item_id", "table": "orders"},
                ],
            },
        ],
    },
    {
        "name": "simple join",
        "dialect": "bigquery",
        "query": """
             SELECT t1.col1, t2.col2
             FROM table1 t1
             JOIN table2 t2 ON t1.id = t2.id
         """,
        "schema": {
            "table1": {"id": "str", "col1": "int64"},
            "table2": {"id": "str", "col2": "int64"},
        },
        "expected": [
            {
                "name": "col1",
                "type": "BIGINT",
                "upstream": [{"column": "col1", "table": "table1"}],
            },
            {
                "name": "col2",
                "type": "BIGINT",
                "upstream": [{"column": "col2", "table": "table2"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "id",
                "upstream": [
                    {"column": "id", "table": "table1"},
                    {"column": "id", "table": "table2"},
                ],
            }
        ],
    },
    {
        "name": "aggregate function",
        "dialect": "bigquery",
        "query": """
             SELECT customer_id as cid, COUNT(order_id) as order_count
             FROM orders
             GROUP BY customer_id
         """,
        "schema": {
            "orders": {"customer_id": "str", "order_id": "int64"},
        },
        "expected": [
            {
                "name": "cid",
                "type": "TEXT",
                "upstream": [{"column": "customer_id", "table": "orders"}],
            },
            {
                "name": "order_count",
                "type": "BIGINT",
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "customer_id",
                "upstream": [{"column": "customer_id", "table": "orders"}],
            }
        ],
    },
    {
        "name": "subquery in select",
        "dialect": "bigquery",
        "query": """
             SELECT
                 emp_id,
                 (SELECT AVG(salary) FROM salaries WHERE salaries.emp_id = employees.emp_id) as avg_salary
             FROM employees
         """,
        "schema": {
            "employees": {"emp_id": "str"},
            "salaries": {"emp_id": "str", "salary": "int64"},
        },
        "expected": [
            {
                "name": "avg_salary",
                "type": "DOUBLE",
                "upstream": [{"column": "salary", "table": "salaries"}],
            },
            {
                "name": "emp_id",
                "type": "TEXT",
                "upstream": [{"column": "emp_id", "table": "employees"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "emp_id",
                "upstream": [
                    {"column": "emp_id", "table": "employees"},
                    {"column": "emp_id", "table": "salaries"},
                ],
            },
        ],
    },
    {
        "name": "union all",
        "dialect": "bigquery",
        "query": """
             SELECT id, name FROM customers
             UNION ALL
             SELECT id, name FROM employees
         """,
        "schema": {
            "customers": {"id": "str", "name": "str"},
            "employees": {"id": "str", "name": "str"},
        },
        "expected": [
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [
                    {"column": "id", "table": "customers"},
                    {"column": "id", "table": "employees"},
                ],
            },
            {
                "name": "name",
                "type": "TEXT",
                "upstream": [
                    {"column": "name", "table": "customers"},
                    {"column": "name", "table": "employees"},
                ],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "self join",
        "dialect": "bigquery",
        "query": """
             SELECT e1.id, e2.manager_id
             FROM employees e1
             JOIN employees e2 ON e1.manager_id = e2.id
         """,
        "schema": {
            "employees": {"id": "str", "manager_id": "str"},
        },
        "expected": [
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "employees"}],
            },
            {
                "name": "manager_id",
                "type": "TEXT",
                "upstream": [
                    {"column": "manager_id", "table": "employees"},
                ],
            },
        ],
        "expected_non_selected": [
            {"name": "id", "upstream": [{"column": "id", "table": "employees"}]},
            {
                "name": "manager_id",
                "upstream": [{"column": "manager_id", "table": "employees"}],
            },
        ],
    },
    {
        "name": "complex case-when",
        "dialect": "bigquery",
        "query": """
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
                 'fixed' as fixed,
                 now() as updated_at
             FROM sales
             JOIN regions ON sales.region_id = regions.id
         """,
        "schema": {
            "sales": {"id": "str", "amount": "int64", "region_id": "str"},
            "regions": {"id": "str", "name": "str"},
        },
        "expected": [
            {"name": "fixed", "type": "VARCHAR", "upstream": []},
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "sales"}],
            },
            {
                "name": "region_abbr",
                "type": "VARCHAR",
                "upstream": [{"column": "name", "table": "regions"}],
            },
            {
                "name": "sale_size",
                "type": "VARCHAR",
                "upstream": [{"column": "amount", "table": "sales"}],
            },
            {"name": "updated_at", "upstream": [], "type": "UNKNOWN"},
        ],
        "expected_non_selected": [
            {"name": "id", "upstream": [{"column": "id", "table": "regions"}]},
            {
                "name": "region_id",
                "upstream": [{"column": "region_id", "table": "sales"}],
            },
        ],
    },
    {
        "name": "aggregate functions with multiple columns",
        "dialect": "bigquery",
        "query": """
             SELECT
                 customer_id,
                 SUM(order_amount) as total_amount,
                 AVG(order_amount) as average_amount,
                 COUNT(order_id) as order_count
             FROM orders
             GROUP BY customer_id
         """,
        "schema": {
            "orders": {
                "customer_id": "str",
                "order_id": "str",
                "order_amount": "int64",
            },
        },
        "expected": [
            {
                "name": "average_amount",
                "type": "DOUBLE",
                "upstream": [{"column": "order_amount", "table": "orders"}],
            },
            {
                "name": "customer_id",
                "type": "TEXT",
                "upstream": [{"column": "customer_id", "table": "orders"}],
            },
            {
                "name": "order_count",
                "type": "BIGINT",
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
            {
                "name": "total_amount",
                "type": "BIGINT",
                "upstream": [{"column": "order_amount", "table": "orders"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "customer_id",
                "upstream": [{"column": "customer_id", "table": "orders"}],
            }
        ],
    },
    {
        "name": "upper function",
        "dialect": "bigquery",
        "query": """
             SELECT UPPER(name) as upper_name
             FROM users
         """,
        "schema": {
            "users": {"name": "str"},
        },
        "expected": [
            {
                "name": "upper_name",
                "type": "TEXT",
                "upstream": [{"column": "name", "table": "users"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "lower function",
        "dialect": "bigquery",
        "query": """
             SELECT LOWER(email) as lower_email
             FROM users
         """,
        "schema": {
            "users": {"email": "str"},
        },
        "expected": [
            {
                "name": "lower_email",
                "type": "TEXT",
                "upstream": [{"column": "email", "table": "users"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "length function",
        "dialect": "bigquery",
        "query": """
             SELECT LENGTH(description) as description_length
             FROM products
         """,
        "schema": {
            "products": {"description": "str"},
        },
        "expected": [
            {
                "name": "description_length",
                "type": "BIGINT",
                "upstream": [{"column": "description", "table": "products"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "trim function",
        "dialect": "bigquery",
        "query": """
             SELECT TRIM(whitespace_column) as trimmed_column
             FROM data
         """,
        "schema": {
            "data": {"whitespace_column": "str"},
        },
        "expected": [
            {
                "name": "trimmed_column",
                "type": "TEXT",
                "upstream": [{"column": "whitespace_column", "table": "data"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "round function",
        "dialect": "bigquery",
        "query": """
             SELECT ROUND(price, 2) as rounded_price
             FROM products
         """,
        "schema": {
            "products": {"price": "float"},
        },
        "expected": [
            {
                "name": "rounded_price",
                "type": "FLOAT",
                "upstream": [{"column": "price", "table": "products"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "coalesce function",
        "dialect": "bigquery",
        "query": """
             SELECT COALESCE(middle_name, 'N/A') as middle_name
             FROM users
         """,
        "schema": {
            "users": {"middle_name": "str"},
        },
        "expected": [
            {
                "name": "middle_name",
                "type": "TEXT",
                "upstream": [{"column": "middle_name", "table": "users"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "cast function",
        "dialect": "bigquery",
        "query": """
             SELECT CAST(order_id AS INT) as order_id_int
             FROM orders
         """,
        "schema": {
            "orders": {"order_id": "str"},
        },
        "expected": [
            {
                "name": "order_id_int",
                "type": "INT",
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "date function",
        "dialect": "bigquery",
        "query": """
             SELECT DATE(order_date) as order_date_only
             FROM orders
         """,
        "schema": {
            "orders": {"order_date": "datetime"},
        },
        "expected": [
            {
                "name": "order_date_only",
                "type": "DATE",
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "extract function",
        "dialect": "bigquery",
        "query": """
             SELECT EXTRACT(YEAR FROM order_date) as order_year
             FROM orders
         """,
        "schema": {
            "orders": {"order_date": "datetime"},
        },
        "expected": [
            {
                "name": "order_year",
                "type": "INT",
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "substring function",
        "dialect": "bigquery",
        "query": """
             SELECT SUBSTRING(name FROM 1 FOR 3) as name_prefix
             FROM users
         """,
        "schema": {
            "users": {"name": "str"},
        },
        "expected": [
            {
                "name": "name_prefix",
                "type": "TEXT",
                "upstream": [{"column": "name", "table": "users"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "floor function",
        "dialect": "bigquery",
        "query": """
             SELECT FLOOR(price) as floored_price
             FROM products
         """,
        "schema": {
            "products": {"price": "float"},
        },
        "expected": [
            {
                "name": "floored_price",
                "type": "FLOAT",
                "upstream": [{"column": "price", "table": "products"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "ceil function",
        "dialect": "bigquery",
        "query": """
             SELECT CEIL(price) as ceiled_price
             FROM products
         """,
        "schema": {
            "products": {"price": "float"},
        },
        "expected": [
            {
                "name": "ceiled_price",
                "type": "FLOAT",
                "upstream": [{"column": "price", "table": "products"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "mysql date_format function",
        "dialect": "mysql",
        "query": """
             SELECT DATE_FORMAT(order_date, '%Y-%m-%d') as formatted_date
             FROM orders
         """,
        "schema": {
            "orders": {"order_date": "datetime"},
        },
        "expected": [
            {
                "name": "formatted_date",
                "type": "VARCHAR",
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "snowflake to_timestamp function",
        "dialect": "snowflake",
        "query": """
             SELECT TO_TIMESTAMP(order_date) as timestamp_date
             FROM orders
         """,
        "schema": {
            "orders": {"order_date": "str"},
        },
        "expected": [
            {
                "name": "TIMESTAMP_DATE",
                "type": "UNKNOWN",
                "upstream": [{"column": "ORDER_DATE", "table": "ORDERS"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "duckdb current_timestamp function",
        "dialect": "duckdb",
        "query": """
             SELECT order_id,CURRENT_TIMESTAMP as current_time
             FROM orders
         """,
        "schema": {
            "orders": {"order_id": "str"},
        },
        "expected": [
            {
                "name": "current_time",
                "type": "TIMESTAMP",
                "upstream": [],
            },
            {
                "name": "order_id",
                "type": "TEXT",
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "redshift date_trunc function",
        "dialect": "redshift",
        "query": """
             SELECT DATE_TRUNC('month', order_date) as month_start
             FROM orders
         """,
        "schema": {
            "orders": {"order_date": "datetime"},
        },
        "expected": [
            {
                "name": "month_start",
                "type": "UNKNOWN",
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "dashboard.report",
        "dialect": "bigquery",
        "query": """
         SELECT
             t1.col1,
             t1.col2,
             t1.col3,
             t1.col4,
             t1.col5,
             t1.col6,
             t1.col7 is not null as is_refunded,
             1 as ai_credits,
             if(t2.col1 is not null, 1, 0) as ai_credits_used,
             t3.col1 as TeamName,
             t3.col2 as TeamId,
             t4.col1,
             t4.col2,
             t5.col1 as Organization,
             t5.col2 as OrganizationId,
             t4.col3,
             t4.col4,
             t6.col1 as ProgramName,
             t5.col3,
             t5.col4
         FROM `dataset1.table1` as t1
         INNER JOIN `dataset2.table2` as t6
             ON t1.col3 = t6.col1
         INNER JOIN `dataset3.table3` as t5
             ON t6.col2 = t5.col2
         LEFT JOIN `dataset4.table4` as t7
             ON t7.col1 = t1.col4
         LEFT JOIN `dataset5.table5` as t2
             ON t1.col1 = t2.col2
         LEFT JOIN `dataset6.table6` as t3
             ON t3.col2 = cast(t2.col3 as int64)
         LEFT JOIN `dataset7.table7` as t4
             ON t4.col5 = safe_cast(t1.col5 as int64)
     """,
        "schema": {
            "dataset1.table1": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
                "col6": "STRING",
                "col7": "STRING",
            },
            "dataset2.table2": {"col1": "STRING", "col2": "STRING"},
            "dataset3.table3": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
            },
            "dataset4.table4": {"col1": "STRING"},
            "dataset5.table5": {"col1": "STRING", "col2": "STRING", "col3": "STRING"},
            "dataset6.table6": {"col1": "STRING", "col2": "STRING"},
            "dataset7.table7": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
            },
        },
        "expected": [
            {"name": "ai_credits", "type": "INT", "upstream": []},
            {
                "name": "ai_credits_used",
                "type": "INT",
                "upstream": [{"column": "col1", "table": "dataset5.table5"}],
            },
            {
                "name": "col1",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "dataset1.table1"}],
            },
            {
                "name": "col1",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "dataset1.table1"}],
            },
            {
                "name": "col2",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "dataset1.table1"}],
            },
            {
                "name": "col2",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "dataset1.table1"}],
            },
            {
                "name": "col3",
                "type": "TEXT",
                "upstream": [{"column": "col3", "table": "dataset1.table1"}],
            },
            {
                "name": "col3",
                "type": "TEXT",
                "upstream": [{"column": "col3", "table": "dataset1.table1"}],
            },
            {
                "name": "col3",
                "type": "TEXT",
                "upstream": [{"column": "col3", "table": "dataset1.table1"}],
            },
            {
                "name": "col4",
                "type": "TEXT",
                "upstream": [{"column": "col4", "table": "dataset1.table1"}],
            },
            {
                "name": "col4",
                "type": "TEXT",
                "upstream": [{"column": "col4", "table": "dataset1.table1"}],
            },
            {
                "name": "col4",
                "type": "TEXT",
                "upstream": [{"column": "col4", "table": "dataset1.table1"}],
            },
            {
                "name": "col5",
                "type": "TEXT",
                "upstream": [{"column": "col5", "table": "dataset1.table1"}],
            },
            {
                "name": "col6",
                "type": "TEXT",
                "upstream": [{"column": "col6", "table": "dataset1.table1"}],
            },
            {
                "name": "is_refunded",
                "type": "BOOLEAN",
                "upstream": [{"column": "col7", "table": "dataset1.table1"}],
            },
            {
                "name": "organization",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "dataset3.table3"}],
            },
            {
                "name": "organizationid",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "dataset3.table3"}],
            },
            {
                "name": "programname",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "dataset2.table2"}],
            },
            {
                "name": "teamid",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "dataset6.table6"}],
            },
            {
                "name": "teamname",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "dataset6.table6"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "col1",
                "upstream": [
                    {"column": "col1", "table": "dataset1.table1"},
                    {"column": "col1", "table": "dataset2.table2"},
                    {"column": "col1", "table": "dataset4.table4"},
                ],
            },
            {
                "name": "col2",
                "upstream": [
                    {"column": "col2", "table": "dataset2.table2"},
                    {"column": "col2", "table": "dataset3.table3"},
                    {"column": "col2", "table": "dataset5.table5"},
                    {"column": "col2", "table": "dataset6.table6"},
                ],
            },
            {
                "name": "col3",
                "upstream": [
                    {"column": "col3", "table": "dataset1.table1"},
                    {"column": "col3", "table": "dataset5.table5"},
                ],
            },
            {
                "name": "col4",
                "upstream": [{"column": "col4", "table": "dataset1.table1"}],
            },
            {
                "name": "col5",
                "upstream": [
                    {"column": "col5", "table": "dataset1.table1"},
                    {"column": "col5", "table": "dataset7.table7"},
                ],
            },
        ],
    },
    {
        "name": "project_report",
        "dialect": "bigquery",
        "query": """
        SELECT
            p1.col1,
            p1.col2,
            p1.col3,
            p1.col4,
            p1.col5,
            p1.col6,
            p1.col7 is not null as is_active,
            1 as project_credits,
            if(p2.col1 is not null, 1, 0) as credits_used,
            p3.col1 as ProjectName,
            p3.col2 as ProjectId,
            p4.col1,
            p4.col2,
            p5.col1 as Department,
            p5.col2 as DepartmentId,
            p4.col3,
            p4.col4,
            p6.col1 as ProgramName,
            p5.col3,
            p5.col4
        FROM `project1.dataset1.table1` as p1
        INNER JOIN `project2.dataset2.table2` as p6
            ON p1.col3 = p6.col1
        INNER JOIN `project3.dataset3.table3` as p5
            ON p6.col2 = p5.col2
        LEFT JOIN `project4.dataset4.table4` as p7
            ON p7.col1 = p1.col4
        LEFT JOIN `project5.dataset5.table5` as p2
            ON p1.col1 = p2.col2
        LEFT JOIN `project6.dataset6.table6` as p3
            ON p3.col2 = cast(p2.col3 as int64)
        LEFT JOIN `project7.dataset7.table7` as p4
            ON p4.col5 = safe_cast(p1.col5 as int64)
    """,
        "schema": {
            "project1.dataset1.table1": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
                "col6": "STRING",
                "col7": "STRING",
            },
            "project2.dataset2.table2": {"col1": "STRING", "col2": "STRING"},
            "project3.dataset3.table3": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
            },
            "project4.dataset4.table4": {"col1": "STRING"},
            "project5.dataset5.table5": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
            },
            "project6.dataset6.table6": {"col1": "STRING", "col2": "STRING"},
            "project7.dataset7.table7": {
                "col1": "STRING",
                "col2": "STRING",
                "col3": "STRING",
                "col4": "STRING",
                "col5": "STRING",
            },
        },
        "expected": [
            {
                "name": "col1",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col1",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col2",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col2",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col3",
                "type": "TEXT",
                "upstream": [{"column": "col3", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col3",
                "type": "TEXT",
                "upstream": [{"column": "col3", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col3",
                "type": "TEXT",
                "upstream": [{"column": "col3", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col4",
                "type": "TEXT",
                "upstream": [{"column": "col4", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col4",
                "type": "TEXT",
                "upstream": [{"column": "col4", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col4",
                "type": "TEXT",
                "upstream": [{"column": "col4", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col5",
                "type": "TEXT",
                "upstream": [{"column": "col5", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col6",
                "type": "TEXT",
                "upstream": [{"column": "col6", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "credits_used",
                "type": "INT",
                "upstream": [{"column": "col1", "table": "project5.dataset5.table5"}],
            },
            {
                "name": "department",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "project3.dataset3.table3"}],
            },
            {
                "name": "departmentid",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "project3.dataset3.table3"}],
            },
            {
                "name": "is_active",
                "type": "BOOLEAN",
                "upstream": [{"column": "col7", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "programname",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "project2.dataset2.table2"}],
            },
            {"name": "project_credits", "type": "INT", "upstream": []},
            {
                "name": "projectid",
                "type": "TEXT",
                "upstream": [{"column": "col2", "table": "project6.dataset6.table6"}],
            },
            {
                "name": "projectname",
                "type": "TEXT",
                "upstream": [{"column": "col1", "table": "project6.dataset6.table6"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "col1",
                "upstream": [
                    {"column": "col1", "table": "project1.dataset1.table1"},
                    {"column": "col1", "table": "project2.dataset2.table2"},
                    {"column": "col1", "table": "project4.dataset4.table4"},
                ],
            },
            {
                "name": "col2",
                "upstream": [
                    {"column": "col2", "table": "project2.dataset2.table2"},
                    {"column": "col2", "table": "project3.dataset3.table3"},
                    {"column": "col2", "table": "project5.dataset5.table5"},
                    {"column": "col2", "table": "project6.dataset6.table6"},
                ],
            },
            {
                "name": "col3",
                "upstream": [
                    {"column": "col3", "table": "project1.dataset1.table1"},
                    {"column": "col3", "table": "project5.dataset5.table5"},
                ],
            },
            {
                "name": "col4",
                "upstream": [{"column": "col4", "table": "project1.dataset1.table1"}],
            },
            {
                "name": "col5",
                "upstream": [
                    {"column": "col5", "table": "project1.dataset1.table1"},
                    {"column": "col5", "table": "project7.dataset7.table7"},
                ],
            },
        ],
    },
    {
        "name": "simple nested table references",
        "dialect": "bigquery",
        "query": """SELECT mycol FROM raw.table1""",
        "schema": {
            "raw.table1": {
                "mycol": "STRING",
            }
        },
        "expected": [
            {
                "name": "mycol",
                "type": "TEXT",
                "upstream": [{"column": "mycol", "table": "raw.table1"}],
            },
        ],
        "expected_non_selected": [],
    },
    {
        "name": "simple nested table references",
        "dialect": "bigquery",
        "query": """
    select
        u.OrganizationId,
        o.Name as Organization,
    from raw.Users as u
    inner join `raw.Organizations` o
        on u.OrganizationId = o.Id
    """,
        "schema": {
            "raw.Organizations": {
                "Id": "INTEGER",
                "Name": "STRING",
            },
            "raw.Users": {
                "OrganizationId": "INTEGER",
            },
            "raw.TeamMemberships": {
                "UserId": "INTEGER",
                "TeamId": "INTEGER",
            },
            "raw.Teams": {
                "Id": "INTEGER",
                "Name": "STRING",
            },
        },
        "expected": [
            {
                "name": "organization",
                "type": "TEXT",
                "upstream": [{"column": "name", "table": "raw.Organizations"}],
            },
            {
                "name": "organizationid",
                "type": "INT",
                "upstream": [{"column": "organizationid", "table": "raw.Users"}],
            },
        ],
        "expected_non_selected": [
            {
                "name": "id",
                "upstream": [{"column": "id", "table": "raw.Organizations"}],
            },
            {
                "name": "organizationid",
                "upstream": [{"column": "organizationid", "table": "raw.Users"}],
            },
        ],
    },
    {
        "name": "failing scenario for CTEs and case-sensitive fields",
        "dialect": "bigquery",
        "query": """
    SELECT Teams.Name, count(*) as c
    FROM raw.TeamMemberships as TeamMemberships
    join raw.Teams 
        on Teams.Id = TeamMemberships.TeamId
    GROUP BY 1
""",
        "schema": {
            "raw.TeamMemberships": {
                "Id": "INTEGER",
                "UserId": "INTEGER",
                "TeamId": "INTEGER",
                "CreatedDate": "TIMESTAMP",
                "CreatedById": "INTEGER",
                "UpdatedDate": "TIMESTAMP",
                "UpdatedById": "INTEGER",
            },
            "raw.teams": {
                "Id": "INTEGER",
                "Name": "STRING",
                "Description": "STRING",
                "ResourceId": "INTEGER",
                "OrganizationId": "INTEGER",
                "TimeZoneId": "INTEGER",
                "TeamMemberCount": "INTEGER",
                "TeamLeaderId": "INTEGER",
                "Active": "BOOLEAN",
                "CreatedDate": "TIMESTAMP",
                "CreatedById": "INTEGER",
                "UpdatedDate": "TIMESTAMP",
                "UpdatedById": "INTEGER",
                "IsGuest": "BOOLEAN",
            },
        },
        "expected": [
            {
                "name": "c",
                "type": "BIGINT",
                "upstream": [],
            },
            {
                "name": "name",
                "type": "TEXT",
                "upstream": [{"column": "name", "table": "raw.teams"}],
            },
        ],
        "expected_non_selected": [
            {"name": "id", "upstream": [{"column": "id", "table": "raw.teams"}]},
            {"name": "name", "upstream": [{"column": "name", "table": "raw.teams"}]},
            {
                "name": "teamid",
                "upstream": [{"column": "teamid", "table": "raw.teammemberships"}],
            },
        ],
    },
    {
        "name": "snowflake function call",
        "dialect": "snowflake",
        "query": """
WITH ufd AS (
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
GROUP BY 1
        """,
        "schema": {
            "fact.some_daily_metrics": {
                "user_id": "integer",
                "date_utc": "date",
                "first_device_type": "string",
                "first_activity_timestamp": "timestamp",
                "estimated_session_duration": "float",
                "event_start": "integer",
            }
        },
        "expected": [
            {
                "name": "ACTIVE_DAYS",
                "type": "BIGINT",
                "upstream": [
                    {
                        "column": "DATE_UTC",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
            {
                "name": "AVG_SESSION_DURATION",
                "type": "DOUBLE",
                "upstream": [
                    {
                        "column": "ESTIMATED_SESSION_DURATION",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
            {
                "name": "FIRST_DEVICE_TYPE",
                "type": "UNKNOWN",
                "upstream": [
                    {
                        "column": "FIRST_ACTIVITY_TIMESTAMP",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                    {
                        "column": "FIRST_DEVICE_TYPE",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
            {
                "name": "RECENCY",
                "type": "INT",
                "upstream": [
                    {
                        "column": "DATE_UTC",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
            {
                "name": "SOME_DAY1_METRIC",
                "type": "INT",
                "upstream": [
                    {
                        "column": "DATE_UTC",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
            {
                "name": "TOTAL_EVENT_START",
                "type": "BIGINT",
                "upstream": [
                    {
                        "column": "EVENT_START",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
            {
                "name": "USER_ID",
                "type": "INT",
                "upstream": [
                    {
                        "column": "USER_ID",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
            {
                "name": "USER_TYPE",
                "type": "VARCHAR",
                "upstream": [
                    {
                        "column": "EVENT_START",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            },
        ],
        "expected_non_selected": [
            {
                "name": "USER_ID",
                "upstream": [
                    {
                        "column": "USER_ID",
                        "table": "FACT.SOME_DAILY_METRICS",
                    },
                ],
            }
        ],
    },
    {
        "name": "snowflake test",
        "dialect": "snowflake",
        "query": """
       SELECT
    t.event_date,
    t.location_code as location,
    t.session_id as session,
    COUNT(DISTINCT t.customer_id) as visitor_count,
    SUM(t.activity_count) as total_activities,
    SUM(t.interaction_count) as total_interactions,
    CURRENT_TIMESTAMP() as created_at
FROM analytics.daily_customer_metrics t
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
        """,
        "schema": {
            "analytics.daily_customer_metrics": {
                "event_date": "date",
                "location_code": "string",
                "session_id": "string",
                "customer_id": "integer",
                "activity_count": "integer",
                "interaction_count": "integer",
            }
        },
        "expected": [
            {"name": "CREATED_AT", "type": "TIMESTAMP", "upstream": []},
            {
                "name": "EVENT_DATE",
                "type": "DATE",
                "upstream": [
                    {
                        "column": "EVENT_DATE",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
            {
                "name": "LOCATION",
                "type": "TEXT",
                "upstream": [
                    {
                        "column": "LOCATION_CODE",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
            {
                "name": "SESSION",
                "type": "TEXT",
                "upstream": [
                    {
                        "column": "SESSION_ID",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
            {
                "name": "TOTAL_ACTIVITIES",
                "type": "BIGINT",
                "upstream": [
                    {
                        "column": "ACTIVITY_COUNT",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
            {
                "name": "TOTAL_INTERACTIONS",
                "type": "BIGINT",
                "upstream": [
                    {
                        "column": "INTERACTION_COUNT",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
            {
                "name": "VISITOR_COUNT",
                "type": "BIGINT",
                "upstream": [
                    {
                        "column": "CUSTOMER_ID",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
        ],
        "expected_non_selected": [
            {
                "name": "EVENT_DATE",
                "upstream": [
                    {
                        "column": "EVENT_DATE",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
            {
                "name": "LOCATION_CODE",
                "upstream": [
                    {
                        "column": "LOCATION_CODE",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
            {
                "name": "SESSION_ID",
                "upstream": [
                    {
                        "column": "SESSION_ID",
                        "table": "ANALYTICS.DAILY_CUSTOMER_METRICS",
                    }
                ],
            },
        ],
    },
    {
        "name": "snowflake example with condition",
        "dialect": "snowflake",
        "query": """
        SELECT 
            case
                when bookings.CancelledAt is not null
                then coalesce(bookings.CancellationReason, 'Empty Reason')
            end as CancellationReason,
            case
                when
                    bookings.Id is not null and
                    bookingCreditRefundedAt is null and
                    bookings.Accepted
                then 1
                else 0
            end as credits_spent
        FROM bookings
        """,
        "schema": {
            "bookings": {
                "Id": "STRING",
                "CancelledAt": "TIMESTAMP",
                "CancellationReason": "STRING",
                "bookingCreditRefundedAt": "TIMESTAMP",
                "Accepted": "BOOLEAN",
            }
        },
        "expected": [
            {
                "name": "CANCELLATIONREASON",
                "type": "TEXT",
                "upstream": [
                    {"column": "CANCELLATIONREASON", "table": "BOOKINGS"},
                    {"column": "CANCELLEDAT", "table": "BOOKINGS"},
                ],
            },
            {
                "name": "CREDITS_SPENT",
                "type": "INT",
                "upstream": [
                    {"column": "ACCEPTED", "table": "BOOKINGS"},
                    {"column": "BOOKINGCREDITREFUNDEDAT", "table": "BOOKINGS"},
                    {"column": "ID", "table": "BOOKINGS"},
                ],
            },
        ],
        "expected_non_selected": [],
    },
]

#


@pytest.mark.parametrize(
    "query,schema,expected,expected_non_selected,dialect",
    [
        (
            tc["query"],
            tc["schema"],
            tc["expected"],
            tc["expected_non_selected"],
            tc["dialect"],
        )
        for tc in test_cases
    ],
    ids=[tc["name"] for tc in test_cases],
)
def test_get_column_lineage(query, schema, expected, expected_non_selected, dialect):
    result = get_column_lineage(query, schema, dialect)
    assert result["columns"] == expected
    assert result["non_selected_columns"] == expected_non_selected


@pytest.mark.parametrize(
    "query,schema,expected,dialect",
    [
        (tc["query"], tc["schema"], tc["expected"], tc["dialect"])
        for tc in test_cases_non_selected_columns
    ],
    ids=[tc["name"] for tc in test_cases_non_selected_columns],
)
def test_extract_non_select_column(query, schema, expected, dialect):
    parsed = parse_one(query, read=dialect)
    optimized = optimize(parsed, schema, dialect=dialect)
    result = extract_non_selected_columns(optimized)
    assert result == expected


def test_get_tables():
    dialect = "bigquery"

    query = "SELECT * FROM table1"
    expected = {"tables": ["table1"]}
    assert get_tables(query, dialect) == expected

    query = """CREATE TABLE public.example AS SELECT 1 AS id, 'Spain' AS country, 'Juan' AS name UNION ALL SELECT 2 AS id, 'Germany' AS country, 'Markus' AS name UNION ALL SELECT 3 AS id, 'France' AS country, 'Antoine' AS name UNION ALL SELECT 4 AS id, 'Poland' AS country, 'Franciszek' AS name"""
    expected = {"tables": ["public.example"]}
    assert get_tables(query, dialect) == expected

    query = """
    with my_cte as (
    select
        COUNTRY_CODE,
        COUNTRY_NAME
    from `raw.my_cte`
)
SELECT * from my_cte
    """
    expected = {"tables": ["raw.my_cte"]}
    assert get_tables(query, dialect) == expected


def test_get_tables_trailing_semicolon():
    dialect = "bigquery"

    query = "SELECT * FROM table1;"
    expected = {"tables": ["table1"]}
    assert get_tables(query, dialect) == expected

    query = "SELECT * FROM table1;;"
    expected = {"tables": ["table1"]}
    assert get_tables(query, dialect) == expected


def test_add_limit():
    query = """
    SELECT DISTINCT product_id, product_name, price, stock, created_at
    FROM test.products
    ORDER BY created_at DESC
    LIMIT 1000;
    """
    expected_query = """
    SELECT DISTINCT product_id, product_name, price, stock, created_at
    FROM test.products
    ORDER BY created_at DESC
    LIMIT 10;
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_no_existing_limit():
    query = """
    SELECT product_id, product_name
    FROM test.products
    """
    expected_query = """
    SELECT product_id, product_name
    FROM test.products
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_replace_existing_limit():
    query = """
    SELECT product_id, product_name
    FROM test.products
    LIMIT 50
    """
    expected_query = """
    SELECT product_id, product_name
    FROM test.products
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_with_order_by():
    query = """
    SELECT product_id, product_name
    FROM test.products
    ORDER BY product_name
    """
    expected_query = """
    SELECT product_id, product_name
    FROM test.products
    ORDER BY product_name
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_with_group_by():
    query = """
    SELECT product_name, COUNT(*)
    FROM test.products
    GROUP BY product_name
    """
    expected_query = """
    SELECT product_name, COUNT(*)
    FROM test.products
    GROUP BY product_name
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_with_semicolon():
    query = """
    SELECT product_id, product_name
    FROM test.products
    LIMIT 50;
    """
    expected_query = """
    SELECT product_id, product_name
    FROM test.products
    LIMIT 10;
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_without_semicolon():
    query = """
    SELECT product_id, product_name
    FROM test.products
    LIMIT 50
    """
    expected_query = """
    SELECT product_id, product_name
    FROM test.products
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_nested_query():
    query = """
    SELECT * FROM (
        SELECT product_id, product_name
        FROM test.products
        LIMIT 50
    ) AS subquery
    """
    expected_query = """
    SELECT * FROM (
        SELECT product_id, product_name
        FROM test.products
        LIMIT 50
    ) AS subquery
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_nested_query_no_existing_limit():
    query = """
    SELECT * FROM (
        SELECT product_id, product_name
        FROM test.products
    ) AS subquery
    """
    expected_query = """
    SELECT * FROM (
        SELECT product_id, product_name
        FROM test.products
    ) AS subquery
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="bigquery")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()


def test_add_limit_with_convert_timezone():
    query = """
    SELECT CONVERT_TIMEZONE('CET', '2025-05-20T00:00:00Z')
    LIMIT 100
    """
    expected_query = """
    SELECT CONVERT_TIMEZONE('CET', '2025-05-20T00:00:00Z')
    LIMIT 10
    """

    result = add_limit(query, 10, dialect="snowflake")
    assert "query" in result
    assert parse_one(result["query"]).sql() == parse_one(expected_query).sql()
