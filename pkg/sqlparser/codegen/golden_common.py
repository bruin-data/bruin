"""Shared helpers for recording pkg/sqlparser/testdata/golden from the Python reference implementation.

The reference is ./pythonsrc: Bruin's former embedded parser (the JSON-over-stdin command loop in
main.py and the command handlers in parser/), run on the pinned sqlglot version.
"""

import gzip
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "pythonsrc"))

# The sqlglot conformance corpus recorded for pkg/sqlengine (see pkg/sqlengine/codegen/README.md).
CORPUS = os.path.join(HERE, "..", "..", "sqlengine", "testdata", "parse.json.gz")
GOLDEN_DIR = os.path.join(HERE, "..", "testdata", "golden")

BRUIN_DIALECTS = {
    "", "athena", "bigquery", "clickhouse", "databricks", "doris", "duckdb", "fabric", "mysql",
    "oracle", "postgres", "redshift", "snowflake", "spark", "starrocks", "trino", "tsql",
}


def load_corpus():
    """The corpus statements in Bruin's dialects, in corpus order."""
    with gzip.open(CORPUS, "rt") as f:
        entries = json.load(f)
    return [{"dialect": e["dialect"], "sql": e["sql"]} for e in entries if e["dialect"] in BRUIN_DIALECTS]


def init_worker():
    # Deterministic BigQuery coercion side effect: dialects that snapshot TypeAnnotator.COERCES_TO
    # are imported first, then BigQuery (the Go port applies it once BigQuery is used, and the Go
    # golden test loads BigQuery before replaying).
    import sqlglot.dialects.hive  # noqa: F401
    import sqlglot.dialects.databricks  # noqa: F401
    import sqlglot.dialects.bigquery  # noqa: F401
    import logging

    logging.disable(logging.CRITICAL)


def run(cmd):
    """What pythonsrc/main.py answered for a command: its result, or {"error": str(e)}."""
    from parser import main as m
    from parser.rename import replace_table_references

    c = cmd["contents"]
    try:
        k = cmd["command"]
        if k == "lineage":
            r = m.get_column_lineage(c["query"], c["schema"], c["dialect"])
        elif k == "get-tables":
            r = m.get_tables(c["query"], c["dialect"])
        elif k == "replace-table-references":
            r = replace_table_references(c["query"], c["dialect"], c["table_mapping"])
        elif k == "add-limit":
            r = m.add_limit(c["query"], c["limit"], c["dialect"])
        elif k == "is-read-only":
            r = m.is_read_only_query(c["query"], c["dialect"])
        elif k == "is-single-select":
            r = m.is_single_select_query(c["query"], c["dialect"])
        elif k == "add-ctes":
            r = m.add_ctes(c["query"], c.get("dialect"), c.get("ctes"))
        elif k == "extract-select":
            r = m.extract_select(c["query"], c.get("dialect"))
        elif k == "select-cte":
            r = m.select_cte(c["query"], c.get("dialect"), c.get("cte_name"))
        elif k == "freeze-time":
            r = m.freeze_time(c["query"], c.get("dialect"), c.get("execution_time"))
        elif k == "hoist-declares":
            r = m.hoist_declares(c["query"], c.get("dialect"))
        else:
            raise Exception("invalid cmd")
        # Normalize through JSON exactly like the command loop.
        return json.loads(json.dumps(r))
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


def record(cmds, runner=run):
    import copy

    # Handlers may mutate their input (schema_dict_to_schema_object does); record it unmodified.
    return [{"cmd": copy.deepcopy(c), "want": runner(c)} for c in cmds]


def write(path, results):
    with gzip.open(path, "wt") as f:
        json.dump(results, f)
    print(f"wrote {len(results)} commands to {path}")
