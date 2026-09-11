import pytest

from .main import is_read_only_query


@pytest.mark.parametrize(
    "query, expected_read_only",
    [
        ("SELECT * FROM orders", True),
        ("SELECT * FROM (SELECT * FROM orders) AS nested_query", True),
        (
            """
            WITH t1 AS (SELECT * FROM t2)
            SELECT * FROM t1
            """,
            True,
        ),
        ("SELECT my_udf(1)", True),
        ("SELECT SYSTEM$CANCEL_QUERY('id')", True),
        ("SELECT * FROM TABLE(GETNEXTVAL(seq))", True),
        ("SELECT EXECUTE_AI_EVALUATION('id')", True),
        ("SELECT * FROM TABLE(TO_QUERY('SELECT 1'))", True),
        ("SELECT 1; SELECT 2", True),
        ("INSERT INTO orders VALUES (1)", False),
        ("UPDATE orders SET amount = 0", False),
        ("DELETE FROM orders", False),
        ("SELECT 1; DELETE FROM orders", False),
        (
            """
            SELECT * FROM (
                WITH inserted AS (INSERT INTO orders VALUES (1))
                SELECT * FROM inserted
            ) AS hidden_write
            """,
            False,
        ),
        (
            """
            SELECT * FROM (
                WITH updated AS (UPDATE orders SET amount = 0)
                SELECT * FROM updated
            ) AS hidden_write
            """,
            False,
        ),
        (
            """
            SELECT * FROM (
                WITH deleted AS (DELETE FROM orders)
                SELECT * FROM deleted
            ) AS hidden_write
            """,
            False,
        ),
        ("SELECT * FROM (INSERT INTO orders VALUES (1)) AS hidden_write", False),
        ("SELECT * FROM (UPDATE orders SET amount = 0) AS hidden_write", False),
        ("SELECT * FROM (DELETE FROM orders) AS hidden_write", False),
        ("SELECT (SELECT seq.NEXTVAL)", False),
        ("SELECT (SELECT SYSTEM$CANCEL_QUERY('id'))", True),
        ("SELECT FROM", False),
    ],
)
def test_snowflake_read_only(query, expected_read_only):
    result = is_read_only_query(query, "snowflake")

    assert result["is_read_only"] is expected_read_only
    if expected_read_only:
        assert result["error"] == ""
