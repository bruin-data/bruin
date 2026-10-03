"""Records testdata/golden/fixtures.json.gz: lineage (with and without schema), tables and rename for
sqlglot's optimizer fixtures (TPC-H, TPC-DS, optimizer.sql, ...) with their schemas, across all Bruin
dialects, answered by the Python reference implementation.

usage: gen_golden_fixtures.py <sqlglot-src> [out.json.gz]
"""

import multiprocessing as mp
import os
import sys

import golden_common as g

SQLGLOT_SRC = sys.argv[1]
sys.path.append(SQLGLOT_SRC)
from tests.helpers import TPCDS_SCHEMA, TPCH_SCHEMA, load_sql_fixture_pairs  # noqa: E402

OPT_SCHEMA = {
    "x": {"a": "INT", "b": "INT"},
    "y": {"b": "INT", "c": "INT"},
    "z": {"b": "INT", "c": "INT"},
    "w": {"d": "TEXT", "e": "TEXT"},
    "temporal": {"d": "DATE", "t": "DATETIME"},
    "structs": {
        "one": "STRUCT<a_1 INT, b_1 VARCHAR>",
        "nested_0": "STRUCT<a_1 INT, nested_1 STRUCT<a_2 INT, nested_2 STRUCT<a_3 INT>>>",
        "quoted": 'STRUCT<"foo bar" INT>',
    },
    "t_bool": {"a": "BOOLEAN"},
}

FIXTURES = [
    ("optimizer/tpc-h/tpc-h.sql", TPCH_SCHEMA),
    ("optimizer/tpc-ds/tpc-ds.sql", TPCDS_SCHEMA),
    ("optimizer/optimizer.sql", OPT_SCHEMA),
    ("optimizer/qualify_columns.sql", OPT_SCHEMA),
    ("optimizer/merge_subqueries.sql", OPT_SCHEMA),
    ("optimizer/unnest_subqueries.sql", OPT_SCHEMA),
    ("optimizer/pushdown_projections.sql", OPT_SCHEMA),
    ("optimizer/annotate_types.sql", OPT_SCHEMA),
]


def sorted_schema(schema):
    # Go marshals map keys sorted; Python received them in that order.
    return {
        t: {c: str(schema[t][c]) for c in sorted(schema[t])} for t in sorted(schema)
    }


def process(item):
    sql, schema, dialect = item
    q = {"query": sql, "dialect": dialect}
    return g.record(
        [
            {"command": "lineage", "contents": {**q, "schema": sorted_schema(schema)}},
            {"command": "lineage", "contents": {**q, "schema": {}}},
            {"command": "get-tables", "contents": dict(q)},
            {
                "command": "replace-table-references",
                "contents": {
                    **q,
                    "table_mapping": {t: "dev_" + t for t in sorted(schema)},
                },
            },
        ]
    )


if __name__ == "__main__":
    out = (
        sys.argv[2]
        if len(sys.argv) > 2
        else os.path.join(g.GOLDEN_DIR, "fixtures.json.gz")
    )
    items = []
    for path, schema in FIXTURES:
        for _meta, sql, _ in load_sql_fixture_pairs(path):
            for d in sorted(g.BRUIN_DIALECTS):
                items.append((sql, schema, d))
    with mp.get_context("fork").Pool(mp.cpu_count(), initializer=g.init_worker) as pool:
        results = []
        for r in pool.imap(process, items, chunksize=4):
            results.extend(r)
    g.write(out, results)
