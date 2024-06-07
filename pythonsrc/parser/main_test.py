import unittest
import pytest
from main import get_column_lineage

test_cases = [
    {
        "name": "nested subqueries",
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
                "upstream": [
                    {"column": "a", "table": "table1"},
                    {"column": "a", "table": "table2"},
                ],
            },
            {"name": "b", "upstream": [{"column": "b", "table": "table1"}]},
            {"name": "c", "upstream": [{"column": "c", "table": "table2"}]},
        ],
    },
    {
        "name": "case-when",
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
                "upstream": [
                    {"column": "item_id", "table": "items"},
                ],
            },
            {
                "name": "price_category",
                "upstream": [
                    {"column": "price", "table": "items"},
                    {"column": "somecol", "table": "orders"},
                ],
            },
        ],
    },
    {
        "name": "simple join",
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
            {"name": "col1", "upstream": [{"column": "col1", "table": "table1"}]},
            {"name": "col2", "upstream": [{"column": "col2", "table": "table2"}]},
        ],
    },
    {
        "name": "aggregate function",
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
                "upstream": [{"column": "customer_id", "table": "orders"}],
            },
            {
                "name": "order_count",
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
        ],
    },
    {
        "name": "subquery in select",
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
                "upstream": [{"column": "salary", "table": "salaries"}],
            },
            {
                "name": "emp_id",
                "upstream": [{"column": "emp_id", "table": "employees"}],
            },
        ],
    },
    {
        "name": "union all",
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
                "upstream": [
                    {"column": "id", "table": "customers"},
                    {"column": "id", "table": "employees"},
                ],
            },
            {
                "name": "name",
                "upstream": [
                    {"column": "name", "table": "customers"},
                    {"column": "name", "table": "employees"},
                ],
            },
        ],
    },
    {
        "name": "self join",
        "query": """
            SELECT e1.id, e2.manager_id
            FROM employees e1
            JOIN employees e2 ON e1.manager_id = e2.id
        """,
        "schema": {
            "employees": {"id": "str", "manager_id": "str"},
        },
        "expected": [
            {"name": "id", "upstream": [{"column": "id", "table": "employees"}]},
            {
                "name": "manager_id",
                "upstream": [
                    {"column": "manager_id", "table": "employees"},
                ],
            },
        ],
    },
    {
        "name": "complex case-when",
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
                END as region_abbr
            FROM sales
            JOIN regions ON sales.region_id = regions.id
        """,
        "schema": {
            "sales": {"id": "str", "amount": "int64", "region_id": "str"},
            "regions": {"id": "str", "name": "str"},
        },
        "expected": [
            {"name": "id", "upstream": [{"column": "id", "table": "sales"}]},
            {
                "name": "region_abbr",
                "upstream": [{"column": "name", "table": "regions"}],
            },
            {"name": "sale_size", "upstream": [{"column": "amount", "table": "sales"}]},
        ],
    },
]


@pytest.mark.parametrize(
    "query,schema,expected",
    [(tc["query"], tc["schema"], tc["expected"]) for tc in test_cases],
    ids=[tc["name"] for tc in test_cases],
)
def test_get_column_lineage(query, schema, expected):
    result = get_column_lineage(query, schema, "bigquery")
    assert result['columns'] == expected
