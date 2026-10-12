"""Records, for the conformance corpus, whether sqlglot parses each statement and the SQL it
generates back in the same dialect.

usage: gen_parse_expected.py <corpus.json> <out parse.json.gz>
"""

import gzip
import json
import sys

from sqlglot.dialects.dialect import Dialect

corpus = json.load(open(sys.argv[1]))
out = []
for r in corpus:
    d = Dialect.get_or_raise(r["dialect"])
    rec = {"dialect": r["dialect"], "sql": r["sql"]}
    try:
        trees = d.parse(r["sql"])
        try:
            rec["out"] = [d.generate(t) if t is not None else None for t in trees]
        except Exception as e:
            rec["gen_error"] = f"{type(e).__name__}: {e}"
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"
    out.append(rec)
with gzip.open(sys.argv[2], "wt") as f:
    json.dump(out, f)
print(
    len(out),
    sum(1 for r in out if "error" in r),
    sum(1 for r in out if "gen_error" in r),
)
