import pytest
from sqlglot import parse_one

from .rename import replace_table_references

test_cases_rename = [
    {
        "name": "simple single table select",
        "query": "SELECT * FROM items",
        "table_references": {"items": "t1"},
        "expected": "SELECT * FROM t1 AS items",
    },
    {
        "name": "multi table with schemas",
        "query": "SELECT * FROM raw.items join raw.orders on items.item_id = orders.item_id",
        "table_references": {"raw.items": "t1", "orders": "raw_dev.t2"},
        "expected": "SELECT * FROM t1 AS items JOIN raw_dev.t2 AS orders ON items.item_id = orders.item_id",
    },
    {
        "name": "table name in select",
        "query": """
             SELECT
                 items.item_id as item_id,
                 CASE
                     WHEN price > 1000 AND t2.somecol < 250 THEN 'high'
                     WHEN price > 100 THEN 'medium'
                     ELSE 'low'
                 END as price_category
             FROM raw.items
             JOIN raw.orders as t2 on items.item_id = t2.item_id
             WHERE in_stock = true
         """,
        "table_references": {"raw.items": "t1", "raw.orders": "raw_dev.orders"},
        "expected": """SELECT items.item_id AS item_id, CASE WHEN price > 1000 AND t2.somecol < 250 THEN 'high' WHEN price > 100 THEN 'medium' ELSE 'low' END AS price_category FROM t1 AS items JOIN raw_dev.orders AS t2 ON items.item_id = t2.item_id WHERE in_stock = TRUE""",
    },
    {
        "name": "subquery",
        "query": """
             SELECT
                 emp_id,
                 (SELECT AVG(salary) FROM raw.salaries WHERE salaries.emp_id = employees.emp_id) as avg_salary
             FROM raw.employees
         """,
        "table_references": {
            "raw.salaries": "raw_dev.salaries",
            "raw.employees": "raw_dev.employees",
        },
        "expected": """SELECT emp_id, (SELECT AVG(salary) FROM raw_dev.salaries WHERE salaries.emp_id = employees.emp_id) AS avg_salary FROM raw_dev.employees""",
    },
    {
        "name": "subquery",
        "query": """
WITH ufd AS (
    SELECT
        user_id,
        MIN(date_utc) as my_date_col
    FROM fact.some_other_table
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
GROUP BY 1""",
        "table_references": {
            "fact.some_daily_metrics": "fact_dev.some_daily_metrics",
            "fact.some_other_table": "fact_dev.some_other_table",
        },
        "expected": """WITH ufd AS (
    SELECT
        user_id,
        MIN(date_utc) as my_date_col
    FROM fact_dev.some_other_table
    GROUP BY 1
),
user_retention AS (
    SELECT
        d.user_id,
        MAX(CASE WHEN DATEDIFF(day, f.my_date_col, d.date_utc) = 1 THEN 1 ELSE 0 END) as some_day1_metric,
    FROM fact_dev.some_daily_metrics d
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
FROM fact_dev.some_daily_metrics d
LEFT JOIN user_retention r ON d.user_id = r.user_id
GROUP BY 1""",
    },
    {
        "name": "cte with similar names",
        "query": """
with
t1 as
(
	select t1.col1, col2
	from raw.table1 as t1
),
t2 as
(
	select t2.col1, col3
	from raw.table1 t2
),
t3 as
(
	select table1.col1, col3
	from raw.table1
)
select *
from t1
join t2
	using(col1)
        """,
        "table_references": {"raw.table1": "raw_dev.table1"},
        "expected": """
with
t1 as
(
	select t1.col1, col2
	from raw_dev.table1 as t1
),
t2 as
(
	select t2.col1, col3
	from raw_dev.table1 t2
),
t3 as
(
	select table1.col1, col3
	from raw_dev.table1
)
select *
from t1
join t2
	using(col1)
        """,
    },
]


@pytest.mark.parametrize(
    "query,table_references,expected",
    [(tc["query"], tc["table_references"], tc["expected"]) for tc in test_cases_rename],
    ids=[tc["name"] for tc in test_cases_rename],
)
def test_replace_table_references(query, table_references, expected):
    result = replace_table_references(query, "bigquery", table_references)
    assert result["query"] == parse_one(expected).sql()
    assert result["error"] is None
