"""Records the column types sqlglot infers for the annotate corpus (annotate_corpus.json).

Each statement is annotated with and without a small schema; the output column types are recorded.

usage: gen_annotate_expected.py <annotate_corpus.json> <out annotate.json.gz>
"""

import copy
import gzip
import json
import sys

import sqlglot
from sqlglot.dialects.dialect import Dialect
from sqlglot.optimizer.annotate_types import TypeAnnotator, annotate_types

# Importing BigQuery mutates the base dialect's type coercion table; record without that leak, like
# pkg/sqlparser/codegen/golden_common.py does.
_pristine = copy.deepcopy(TypeAnnotator.COERCES_TO)
import sqlglot.dialects.hive  # noqa: E402,F401
import sqlglot.dialects.databricks  # noqa: E402,F401
import sqlglot.dialects.bigquery  # noqa: E402,F401

TypeAnnotator.COERCES_TO.update(_pristine)

# Must match annotateSchema in pkg/sqlengine/annotate_conformance_test.go (key order matters).
SCHEMA = {
    "t": {
        "a": "INT",
        "b": "VARCHAR",
        "c": "DOUBLE",
        "d": "DATE",
        "e": "TIMESTAMP",
        "f": "DECIMAL(10, 2)",
        "g": "BIGINT",
        "h": "BOOLEAN",
        "s": "STRUCT<x INT, y VARCHAR>",
        "arr": "ARRAY<INT>",
        "j": "JSON",
    },
    "u": {"a": "BIGINT", "b": "TEXT", "k": "FLOAT", "dt": "DATETIME"},
}


def record(dialect, sql, with_schema):
    rec = {"dialect": dialect, "sql": sql, "schema": with_schema}
    try:
        d = Dialect.get_or_raise(dialect)
        e = sqlglot.parse_one(sql, dialect=d)
        annotate_types(e, schema=SCHEMA if with_schema else None, dialect=d)
        rec["types"] = [s.type.sql() if s.type else "None" for s in e.selects]
    except Exception as ex:  # noqa: BLE001
        rec["error"] = f"{type(ex).__name__}: {ex}"
    return rec


with open(sys.argv[1]) as f:
    corpus = json.load(f)
out = [record(c["dialect"], c["sql"], s) for c in corpus for s in (True, False)]
with gzip.open(sys.argv[2], "wt") as f:
    json.dump(out, f)
print(len(out), sum(1 for r in out if "error" in r))
