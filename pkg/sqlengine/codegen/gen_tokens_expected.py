"""Records sqlglot tokenizer output for the conformance corpus."""

import gzip
import json
import sys
from sqlglot.dialects.dialect import Dialect

corpus = json.load(open(sys.argv[1]))
out = []
for r in corpus:
    d = Dialect.get_or_raise(r["dialect"])
    try:
        toks = d.tokenize(r["sql"])
        res = {
            "tokens": [
                [t.token_type.name, t.text, t.line, t.col, t.start, t.end, t.comments]
                for t in toks
            ]
        }
    except Exception as e:
        res = {"error": f"{type(e).__name__}: {e}"}
    out.append({"dialect": r["dialect"], "sql": r["sql"], **res})
with gzip.open(sys.argv[2], "wt") as f:
    json.dump(out, f)
print(len(out))
