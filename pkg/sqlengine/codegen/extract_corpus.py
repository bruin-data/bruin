"""Extract (dialect, sql) pairs from sqlglot's test-suite by recording Validator calls.

usage: extract_corpus.py <sqlglot repo checkout at the pinned tag> <out corpus.json>
"""

from collections import Counter
import json
import sys
import os
import unittest
import importlib
import glob

SQLGLOT_REPO, OUT = sys.argv[1], sys.argv[2]
sys.path.insert(0, SQLGLOT_REPO)
from tests.dialects import test_dialect  # noqa: E402 (needs the sqlglot checkout on sys.path)

RECORDS = []


def rec(d, sql, src):
    if isinstance(sql, str) and sql.strip():
        d = (
            d
            if isinstance(d, str) or d is None
            else getattr(d, "__name__", str(d)).lower()
        )
        RECORDS.append({"dialect": (d or ""), "sql": sql, "src": src})


V = test_dialect.Validator


def vi(
    self, sql, write_sql=None, pretty=False, check_command_warning=False, identify=False
):
    rec(self.dialect, sql, "identity")
    if write_sql and not pretty:
        rec(self.dialect, write_sql, "identity_out")
    try:
        return self.parse_one(sql)
    except Exception:
        return None


def va(self, sql, read=None, write=None, pretty=False, identify=False):
    rec(self.dialect, sql, "all")
    for d, s in (read or {}).items():
        rec(d, s, "all_read")
    for d, s in (write or {}).items():
        if isinstance(s, str) and not pretty:
            rec(d, s, "all_write")
    try:
        return self.parse_one(sql)
    except Exception:
        return None


def vt(self, sql, write_sql, write_dialect=None):
    rec(self.dialect, sql, "transpile")
    if write_dialect:
        rec(write_dialect, write_sql, "transpile_out")
    try:
        return self.parse_one(sql)
    except Exception:
        return None


V.validate_identity = vi
V.validate_all = va
V.validate_transpile = vt
# make assertions no-ops so tests run through


class _Swallow:
    output = [""] * 100
    records = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return True


def _ctx(self, *a, **k):
    if len(a) >= 2 and callable(a[1]):
        try:
            a[1](*a[2:], **k)
        except Exception:
            pass
        return None
    return _Swallow()


for name in dir(unittest.TestCase):
    if name.startswith("assert"):
        if name in (
            "assertLogs",
            "assertRaises",
            "assertRaisesRegex",
            "assertWarns",
            "assertNoLogs",
            "assertWarnsRegex",
        ):
            setattr(V, name, _ctx)
        else:
            setattr(V, name, lambda self, *a, **k: None)

loader = unittest.TestLoader()
mods = sorted(glob.glob(os.path.join(SQLGLOT_REPO, "tests", "dialects", "test_*.py")))
for m in mods:
    name = "tests.dialects." + os.path.basename(m)[:-3]
    try:
        mod = importlib.import_module(name)
    except Exception as e:
        print("import fail", name, e, file=sys.stderr)
        continue
    suite = loader.loadTestsFromModule(mod)
    res = unittest.TestResult()
    suite.run(res)
    if res.errors or res.failures:
        print(
            name,
            "errors",
            len(res.errors),
            len(res.failures),
            [e[1].splitlines()[-1][:150] for e in (res.errors + res.failures)][:3],
            file=sys.stderr,
        )
# identity fixtures
fx = os.path.join(SQLGLOT_REPO, "tests", "fixtures", "identity.sql")
for line in open(fx):
    line = line.strip()
    if line and not line.startswith("--"):
        rec("", line, "fixture_identity")

want = set(
    [
        "",
        "athena",
        "bigquery",
        "clickhouse",
        "databricks",
        "doris",
        "duckdb",
        "fabric",
        "hive",
        "mysql",
        "oracle",
        "postgres",
        "presto",
        "redshift",
        "snowflake",
        "spark",
        "spark2",
        "sqlite",
        "starrocks",
        "trino",
        "tsql",
    ]
)
seen = set()
out = []
for r in RECORDS:
    if r["dialect"] not in want:
        continue
    k = (r["dialect"], r["sql"])
    if k in seen:
        continue
    seen.add(k)
    out.append(r)
json.dump(out, open(OUT, "w"))

print(len(RECORDS), len(out), Counter(r["dialect"] for r in out))
