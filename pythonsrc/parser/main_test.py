import unittest
import pytest
from main import get_column_lineage

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
        "expectedAllColumns": [
            {
                "name": "a",
                "type": "TEXT",
                "upstream": [
                    {"column": "a", "table": "table1"},
                ],
            },
            {
                "name": "a",
                "type": "TEXT",
                "upstream": [
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
                'type': 'TEXT',
                "upstream": [
                    {"column": "item_id", "table": "items"},
                ],
            },
            {
                "name": "price_category",
                'type': 'VARCHAR',
                "upstream": [
                    {"column": "price", "table": "items"},
                    {"column": "somecol", "table": "orders"},
                ],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "in_stock",
                "type": "BOOLEAN",
                "upstream": [{"column": "in_stock", "table": "items"}],
            },
            {
                "name": "item_id",
                "type": "TEXT",
                "upstream": [{"column": "item_id", "table": "items"}],
            },
            {
                "name": "item_id",
                "type": "TEXT",
                "upstream": [{"column": "item_id", "table": "t2"}],
            },
            {
                "name": "price",
                "type": "BIGINT",
                "upstream": [{"column": "price", "table": "items"}],
            },
            {
                "name": "somecol",
                "type": "BIGINT",
                "upstream": [{"column": "somecol", "table": "t2"}],
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
                'type': 'BIGINT',
                "upstream": [{"column": "col1", "table": "table1"}],
            },
            {
                "name": "col2",
                'type': 'BIGINT',
                "upstream": [{"column": "col2", "table": "table2"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "col1",
                "type": "BIGINT",
                "upstream": [{"column": "col1", "table": "t1"}],
            },
            {
                "name": "col2",
                "type": "BIGINT",
                "upstream": [{"column": "col2", "table": "t2"}],
            },
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "t1"}],
            },
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "t2"}],
            },
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
                'type': 'TEXT',
                "upstream": [{"column": "customer_id", "table": "orders"}],
            },
            {
                "name": "order_count",
                'type': 'BIGINT',
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "customer_id",
                "type": "TEXT",
                "upstream": [{"column": "customer_id", "table": "orders"}],
            },
            {
                "name": "order_id",
                "type": "BIGINT",
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
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
                'type': 'DOUBLE',
                "upstream": [{"column": "salary", "table": "salaries"}],
            },
            {
                "name": "emp_id",
                'type': 'TEXT',
                "upstream": [{"column": "emp_id", "table": "employees"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "_col_0",
                "type": "DOUBLE",
                "upstream": [{"column": "_col_0", "table": "_u_0"}],
            },
            {
                "name": "_u_1",
                "type": "TEXT",
                "upstream": [{"column": "_u_1", "table": "_u_0"}],
            },
            {
                "name": "emp_id",
                "type": "TEXT",
                "upstream": [{"column": "emp_id", "table": "employees"}],
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
                'type': 'TEXT',
                "upstream": [
                    {"column": "id", "table": "customers"},
                    {"column": "id", "table": "employees"},
                ],
            },
            {
                "name": "name",
                'type': 'TEXT',
                "upstream": [
                    {"column": "name", "table": "customers"},
                    {"column": "name", "table": "employees"},
                ],
            },
        ],
        "expectedAllColumns": [

        ],
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
                'type': 'TEXT',
                "upstream": [{"column": "id", "table": "employees"}],
            },
            {
                "name": "manager_id",
                'type': 'TEXT',
                "upstream": [
                    {"column": "manager_id", "table": "employees"},
                ],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "e1"}],
            },
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "e2"}],
            },
            {
                "name": "manager_id",
                "type": "TEXT",
                "upstream": [{"column": "manager_id", "table": "e2"}],
            },
            {
                "name": "manager_id",
                "type": "TEXT",
                "upstream": [{"column": "manager_id", "table": "e1"}],
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
            {
                "name": "fixed",
                'type': 'VARCHAR',
                "upstream": [],
            },
            {
                "name": "id",
                'type': 'TEXT',
                "upstream": [{"column": "id", "table": "sales"}],
            },
            {
                "name": "region_abbr",
                'type': 'VARCHAR',
                "upstream": [{"column": "name", "table": "regions"}],
            },
            {
                "name": "sale_size",
                'type': 'VARCHAR',
                "upstream": [{"column": "amount", "table": "sales"}],
            },
            {
                "name": "updated_at",
                "type": "UNKNOWN",
                "upstream": [],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "amount",
                "type": "BIGINT",
                "upstream": [{"column": "amount", "table": "sales"}],
            },
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "sales"}],
            },
            {
                "name": "id",
                "type": "TEXT",
                "upstream": [{"column": "id", "table": "regions"}],
            },
            {
                "name": "name",
                "type": "TEXT",
                "upstream": [{"column": "name", "table": "regions"}],
            },
            {
                "name": "region_id",
                "type": "TEXT",
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
            "orders": {"customer_id": "str", "order_id": "str", "order_amount": "int64"},
        },
        "expected": [
            {
                "name": "average_amount",
                'type': 'DOUBLE',
                "upstream": [{"column": "order_amount", "table": "orders"}],
            },
            {
                "name": "customer_id",
                'type': 'TEXT',
                "upstream": [{"column": "customer_id", "table": "orders"}],
            },
            {
                "name": "order_count",
                'type': 'BIGINT',
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
            {
                "name": "total_amount",
                'type': 'BIGINT',
                "upstream": [{"column": "order_amount", "table": "orders"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "customer_id",
                "type": "TEXT",
                "upstream": [{"column": "customer_id", "table": "orders"}],
            },
            {
                "name": "order_amount",
                "type": "BIGINT",
                "upstream": [{"column": "order_amount", "table": "orders"}],
            },
            {
                "name": "order_id",
                "type": "TEXT",
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
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
                'type': 'TEXT',
                "upstream": [{"column": "name", "table": "users"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "name",
                "type": "TEXT",
                "upstream": [{"column": "name", "table": "users"}],
            },
        ],
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
                'type': 'TEXT',
                "upstream": [{"column": "email", "table": "users"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "email",
                "type": "TEXT",
                "upstream": [{"column": "email", "table": "users"}],
            },
        ],
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
                'type': 'BIGINT',
                "upstream": [{"column": "description", "table": "products"}],
            },
        ],
        "expectedAllColumns": [{'name': 'description',
  'type': 'TEXT',
  'upstream': [{'column': 'description', 'table': 'products'}]}],
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
                'type': 'TEXT',
                "upstream": [{"column": "whitespace_column", "table": "data"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "whitespace_column",
                "type": "TEXT",
                "upstream": [{"column": "whitespace_column", "table": "data"}],
            },
        ],
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
                'type': 'FLOAT',
                "upstream": [{"column": "price", "table": "products"}],
            },
        ],
        "expectedAllColumns": [{'name': 'price',
  'type': 'FLOAT',
  'upstream': [{'column': 'price', 'table': 'products'}]}],
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
                'type': 'TEXT',
                "upstream": [{"column": "middle_name", "table": "users"}],
            },
        ],
        "expectedAllColumns": [{'name': 'middle_name',
  'type': 'TEXT',
  'upstream': [{'column': 'middle_name', 'table': 'users'}]}],
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
                'type': 'INT',
                "upstream": [{"column": "order_id", "table": "orders"}],
            },
        ],
        "expectedAllColumns": [{'name': 'order_id',
  'type': 'TEXT',
  'upstream': [{'column': 'order_id', 'table': 'orders'}]}],
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
                'type': 'DATE',
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expectedAllColumns": [{'name': 'order_date',
  'type': 'TIMESTAMP',
  'upstream': [{'column': 'order_date', 'table': 'orders'}]}],
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
                'type': 'INT',
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expectedAllColumns": [{'name': 'order_date',
  'type': 'TIMESTAMP',
  'upstream': [{'column': 'order_date', 'table': 'orders'}]}],
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
                'type': 'TEXT',
                "upstream": [{"column": "name", "table": "users"}],
            },
        ],
        "expectedAllColumns": [{'name': 'name',
  'type': 'TEXT',
  'upstream': [{'column': 'name', 'table': 'users'}]}],
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
                'type': 'FLOAT',
                "upstream": [{"column": "price", "table": "products"}],
            },
        ],
        "expectedAllColumns": [{'name': 'price',
  'type': 'FLOAT',
  'upstream': [{'column': 'price', 'table': 'products'}]}],
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
                'type': 'FLOAT',
                "upstream": [{"column": "price", "table": "products"}],
            },
        ],
        "expectedAllColumns": [{'name': 'price',
  'type': 'FLOAT',
  'upstream': [{'column': 'price', 'table': 'products'}]}],
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
                'type': 'VARCHAR',
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expectedAllColumns": [{'name': 'order_date',
  'type': 'DATETIME',
  'upstream': [{'column': 'order_date', 'table': 'orders'}]}],
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
                'type': 'UNKNOWN',
                "upstream": [{"column": "ORDER_DATE", "table": "ORDERS"}],
            },
        ],
        "expectedAllColumns": [{'name': 'ORDER_DATE',
  'type': 'TEXT',
  'upstream': [{'column': 'ORDER_DATE', 'table': 'ORDERS'}]}],
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
                'type': 'TIMESTAMP',
                "upstream": [{'column': 'current_time', 'table': ''}],
            },
            {
                "name": "order_id",
                'type': 'TEXT',
                "upstream": [{'column': 'order_id', 'table': 'orders'}],
            },
        ],
        "expectedAllColumns": [{'name': 'order_id',
  'type': 'TEXT',
  'upstream': [{'column': 'order_id', 'table': 'orders'}]}],
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
                'type': 'UNKNOWN',
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
        "expectedAllColumns": [
            {
                "name": "order_date",
                'type': 'DATETIME',
                "upstream": [{"column": "order_date", "table": "orders"}],
            },
        ],
    },
]


@pytest.mark.parametrize(
    "query,schema,expected,expectedAllColumns, dialect",
    [(tc["query"], tc["schema"], tc["expected"], tc["expectedAllColumns"],tc["dialect"]) for tc in test_cases],
    ids=[tc["name"] for tc in test_cases],
)
def test_get_column_lineage(query, schema, expected,expectedAllColumns, dialect):
    result = get_column_lineage(query, schema, dialect)
    assert result['columns'] == expected
    assert result['lineage'] == expectedAllColumns
