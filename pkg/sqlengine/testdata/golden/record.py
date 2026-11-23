"""Records the golden expectations of pkg/sqlengine with sqlglot.

pkg/sqlengine started as a Go port of sqlglot, so sqlglot is the reference this suite is
recorded with. The suite pins behavior, not internals: whether a statement parses and the SQL it
regenerates, the types of a query's output columns, and each output column's type and source
columns after the optimizer rules column lineage runs. Inputs are the files next to this script;
outputs are the *.json.gz files that golden_test.go replays.

    pip install "sqlglot==30.33.0"
    python pkg/sqlengine/testdata/golden/record.py            # every family
    python pkg/sqlengine/testdata/golden/record.py parse      # parse, annotate or lineage

Families:
    parse     statements.jsonl -> parse.json.gz     parses or not; SQL regenerated in the dialect
    annotate  annotate.jsonl   -> annotate.json.gz  inferred types of the output columns
    lineage   lineage.jsonl    -> lineage.json.gz   output columns with type and source columns
              (+ schemas.json)
"""

import gzip
import io
import json
import logging
import os
import re
import sys

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.lineage import lineage
from sqlglot.optimizer import optimize
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.merge_subqueries import merge_subqueries
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope
from sqlglot.optimizer.unnest_subqueries import unnest_subqueries
from sqlglot.parser import Parser

SQLGLOT_VERSION = "30.33.0"
HERE = os.path.dirname(os.path.abspath(__file__))

# sqlglot logs a warning for every unsupported construct; the recordings capture the outcome.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# Error texts are kept for reading the files; the test only compares whether there was an error.
CLASS_REPR = re.compile(r"<class 'sqlglot\.expressions\.(?:\w+\.)*(\w+)'>")


def error_text(e):
    return CLASS_REPR.sub(r"\1", f"{type(e).__name__}: {e}").split("\n")[0]


def pin_iteration_order():
    """Makes sqlglot's output independent of PYTHONHASHSEED where it iterates a Python set.

    Missing required arguments are reported in definition order (it shows in the error texts the
    files keep), and set-operation modifiers are moved in sorted order.
    """
    stack = [exp.Expr]
    while stack:
        cls = stack.pop()
        stack.extend(cls.__subclasses__())
        cls.required_args = tuple(k for k, v in cls.arg_types.items() if v)
    stack = [Parser]
    while stack:
        cls = stack.pop()
        stack.extend(cls.__subclasses__())
        if isinstance(vars(cls).get("SET_OP_MODIFIERS"), (set, frozenset)):
            cls.SET_OP_MODIFIERS = sorted(cls.SET_OP_MODIFIERS)


_dialects = {}


def dialect(name):
    if name not in _dialects:
        _dialects[name] = Dialect.get_or_raise(name)
        pin_iteration_order()  # loading a dialect can define expression and parser classes
    return _dialects[name]


def read_jsonl(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_gz(name, value):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0, compresslevel=9) as f:
        f.write(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    with open(os.path.join(HERE, name), "wb") as f:
        f.write(buf.getvalue())


def record_parse():
    out = []
    for r in read_jsonl("statements.jsonl"):
        d = dialect(r["dialect"])
        rec = {"dialect": r["dialect"], "sql": r["sql"]}
        try:
            trees = d.parse(r["sql"])
        except Exception as e:
            rec["error"] = error_text(e)
            out.append(rec)
            continue
        try:
            rec["out"] = [d.generate(t) if t is not None else None for t in trees]
        except Exception as e:
            rec["gen_error"] = error_text(e)
        out.append(rec)
    write_gz("parse.json.gz", out)
    return len(out)


# Must match annotateSchema in golden_test.go (key order matters).
ANNOTATE_SCHEMA = {
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
    "tbl": {
        "bin_col": "BINARY",
        "str_col": "STRING",
        "bignum_col": "BIGNUMERIC",
        "date_col": "DATE",
        "decfloat_col": "DECFLOAT",
        "float_col": "FLOAT",
        "timestamp_col": "TIMESTAMP",
        "double_col": "DOUBLE",
        "bigint_col": "BIGINT",
        "smallint_col": "SMALLINT",
        "bit_col": "BIT",
        "obj_col": "OBJECT",
        "int_col": "INT",
        "bool_col": "BOOLEAN",
        "bytes_col": "BYTES",
        "interval_col": "INTERVAL",
        "array_col": "ARRAY<STRING>",
    },
}


def type_sql(e):
    return e.type.sql() if e.type else "None"


def output_types(e):
    """The types of a query's output columns, or the type of a bare expression."""
    if isinstance(e, exp.Query):
        return [type_sql(s) for s in e.selects]
    return [type_sql(e)]


def annotate_case(c):
    d = dialect(c["dialect"])
    rec = dict(c)
    try:
        e = sqlglot.parse_one(c["sql"], read=d)
        annotate_types(e, schema=ANNOTATE_SCHEMA if c["schema"] else None, dialect=d)
        rec["types"] = output_types(e)
    except Exception as e:
        rec["error"] = error_text(e)
    return rec


def record_annotate():
    # Loading BigQuery adds its coercion rules to the base table that every dialect shares.
    # pkg/sqlengine keeps them to BigQuery, so everything else is recorded before BigQuery loads.
    if "sqlglot.dialects.bigquery" in sys.modules:
        sys.exit("record.py must run before anything loads the BigQuery dialect")
    inputs = read_jsonl("annotate.jsonl")
    cases = [annotate_case(c) if c["dialect"] != "bigquery" else None for c in inputs]
    assert "sqlglot.dialects.bigquery" not in sys.modules, "a dialect loaded BigQuery"
    cases = [annotate_case(c) if r is None else r for c, r in zip(inputs, cases)]
    write_gz("annotate.json.gz", cases)
    return len(cases)


# The rules column lineage runs; when they fail it falls back to every rule (the default list).
LINEAGE_RULES = (qualify, unnest_subqueries, merge_subqueries, annotate_types)


def source_columns(node):
    """The (table, column) pairs a lineage graph reads, sorted: its leaves that are tables."""
    found, stack = set(), [node]
    while stack:
        n = stack.pop()
        stack.extend(n.downstream)
        if not n.downstream and isinstance(n.expression, exp.Table):
            table = ".".join(p.name for p in n.expression.parts)
            found.add((table, n.name.split(".")[-1]))
    return [list(p) for p in sorted(found)]


def lineage_case(c, schemas):
    d = dialect(c["dialect"])
    schema = schemas.get(c["schema"]) or {}
    rec = dict(c)
    try:
        parsed = sqlglot.parse_one(c["sql"], read=d)
        if not isinstance(parsed, exp.Query):
            raise ValueError("not a query")
        try:
            opt = optimize(parsed.copy(), schema=schema, dialect=d, rules=LINEAGE_RULES)
        except Exception:
            opt = optimize(parsed, schema=schema, dialect=d)
    except Exception as e:
        rec["error"] = error_text(e)
        return rec
    try:
        graph = lineage(
            None,
            opt,
            schema=schema,
            dialect=d,
            scope=build_scope(opt),
            trim_selects=False,
            copy=False,
        )
    except Exception:
        graph = {}
    rec["columns"] = [
        [
            s.alias_or_name,
            s.type.sql(d) if s.type else "",
            source_columns(graph[s.alias_or_name]),
        ]
        if s.alias_or_name in graph
        else [s.alias_or_name, s.type.sql(d) if s.type else "", None]
        for s in opt.selects
    ]
    return rec


def record_lineage():
    with open(os.path.join(HERE, "schemas.json"), encoding="utf-8") as f:
        schemas = json.load(f)
    cases = [lineage_case(c, schemas) for c in read_jsonl("lineage.jsonl")]
    write_gz("lineage.json.gz", cases)
    return len(cases)


FAMILIES = {
    "parse": record_parse,
    "annotate": record_annotate,
    "lineage": record_lineage,
}


def main():
    if sqlglot.__version__ != SQLGLOT_VERSION:
        sys.exit(f"needs sqlglot=={SQLGLOT_VERSION}, found {sqlglot.__version__}")
    names = sys.argv[1:] or list(FAMILIES)
    unknown = [n for n in names if n not in FAMILIES]
    if unknown:
        sys.exit(f"unknown families {unknown}; choose from {list(FAMILIES)}")
    # annotate checks that BigQuery is not loaded yet, so it runs first.
    for name in sorted(names, key=lambda n: n != "annotate"):
        print(f"{name}: {FAMILIES[name]()} records")


if __name__ == "__main__":
    main()
