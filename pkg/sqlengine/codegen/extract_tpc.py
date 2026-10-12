"""Extracts the TPC-H and TPC-DS queries and schemas from sqlglot's test-suite (used by benchmarks).

usage: extract_tpc.py <sqlglot repo checkout at the pinned tag> <out tpc.json.gz>
"""

import gzip
import json
import sys

SQLGLOT_REPO, OUT = sys.argv[1], sys.argv[2]
sys.path.insert(0, SQLGLOT_REPO)
from tests.helpers import TPCDS_SCHEMA, TPCH_SCHEMA, load_sql_fixture_pairs  # noqa: E402

SETS = {
    "tpch": ("optimizer/tpc-h/tpc-h.sql", TPCH_SCHEMA),
    "tpcds": ("optimizer/tpc-ds/tpc-ds.sql", TPCDS_SCHEMA),
}

out = {}
for name, (path, schema) in SETS.items():
    out[name] = {
        "schema": {
            t: {c: str(v) for c, v in cols.items()} for t, cols in schema.items()
        },
        "queries": [sql for _meta, sql, _ in load_sql_fixture_pairs(path)],
    }
with gzip.open(OUT, "wt") as f:
    json.dump(out, f)
print({k: len(v["queries"]) for k, v in out.items()})
