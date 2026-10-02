import pytest

from .main import hoist_declares, hoist_declares_list


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT 1; SELECT 2;", "SELECT 1; SELECT 2;"),
        ("DECLARE x INT64;\nSELECT 1;", "DECLARE x INT64;\nSELECT 1;"),
        (
            "SET x = 1;\nDECLARE y INT64;\nSELECT 1;",
            "DECLARE y INT64;\nSET x = 1;\nSELECT 1;",
        ),
        ("SELECT 'declare bankruptcy' AS msg;", "SELECT 'declare bankruptcy' AS msg;"),
        (
            "SET separator = ';';\nDECLARE y INT64;",
            "DECLARE y INT64;\nSET separator = ';';",
        ),
        (
            "SET x = 1;\nBEGIN\n  DECLARE y INT64;\n  SELECT y;\nEND;",
            "SET x = 1;\nBEGIN\n  DECLARE y INT64;\n  SELECT y;\nEND;",
        ),
        (
            "SET x = 1;\nBEGIN\n  SELECT CASE WHEN x>0 THEN 'a' ELSE 'b' END;\n  DECLARE y INT64;\nEND;",
            "SET x = 1;\nBEGIN\n  SELECT CASE WHEN x>0 THEN 'a' ELSE 'b' END;\n  DECLARE y INT64;\nEND;",
        ),
        (
            "SET x = 1;\n-- setup\nDECLARE y INT64;",
            "-- setup\nDECLARE y INT64;\nSET x = 1;",
        ),
        (
            "SET x = 1;\nDECLARE distinct_keys array<STRING>;",
            "DECLARE distinct_keys array<STRING>;\nSET x = 1;",
        ),
        (
            "BEGIN TRANSACTION;\nSET x = 1;\nDECLARE y INT64;\nCOMMIT TRANSACTION;",
            "DECLARE y INT64;\nBEGIN TRANSACTION;\nSET x = 1;\nCOMMIT TRANSACTION;",
        ),
        (
            "SELECT (1 + 2); /* ; DECLARE nope */ DECLARE café STRING DEFAULT '雪;';",
            "/* ; DECLARE nope */ DECLARE café STRING DEFAULT '雪;';\nSELECT (1 + 2);",
        ),
    ],
)
def test_hoist_declares_bigquery_fixtures(query, expected):
    assert hoist_declares(query, "bigquery") == {"query": expected, "error": ""}


def test_hoist_declares_empty_and_tokenization_error_preserve_source():
    assert hoist_declares(" \n", "bigquery") == {"query": " \n", "error": ""}
    result = hoist_declares("SELECT 'unterminated", "bigquery")
    assert result["query"] == "SELECT 'unterminated"
    assert result["error"]


def test_hoist_declares_uses_dialect_normalization():
    result = hoist_declares("SELECT 1; DECLARE x INT;", "fabric")
    assert result == {"query": "DECLARE x INT;\nSELECT 1;", "error": ""}


def test_hoist_declares_list_preserves_whole_entries_and_exact_formatting():
    queries = [
        "  SET x = 1; DECLARE nested INT64  ",
        "\n-- variable\nDECLARE y INT64\n",
        "SELECT 1",
        "DECLARE z STRING DEFAULT '雪;'",
    ]
    assert hoist_declares_list(queries, "bigquery") == {
        "queries": [queries[1], queries[3], queries[0], queries[2]],
        "error": "",
    }


def test_hoist_declares_list_no_reorder_returns_same_object():
    queries = ["DECLARE x INT64", "SELECT 1"]
    result = hoist_declares_list(queries, "bigquery")
    assert result["queries"] is queries


@pytest.mark.parametrize(
    "dialect",
    ["bigquery", "tsql", "snowflake", "postgres", "mysql", "duckdb", "fabric"],
)
@pytest.mark.parametrize(
    "block",
    [
        "BEGIN BEGIN SELECT 1; END; DECLARE inside INT; END",
        "BEGIN SELECT CASE WHEN 1=1 THEN 2 ELSE CASE WHEN 3=3 THEN 4 END END; DECLARE inside INT; END",
    ],
)
def test_hoist_declares_nested_blocks_and_dialect_fallback(dialect, block):
    query = f"SELECT 17; {block}; DECLARE outside INT; SELECT 29;"
    assert hoist_declares(query, dialect) == {
        "query": f"DECLARE outside INT;\nSELECT 17;\n{block};\nSELECT 29;",
        "error": "",
    }
    queries = [block, "DECLARE outside INT", "SELECT 29"]
    assert hoist_declares_list(queries, dialect) == {
        "queries": ["DECLARE outside INT", block, "SELECT 29"],
        "error": "",
    }
