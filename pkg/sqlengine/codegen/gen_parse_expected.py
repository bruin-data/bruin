"""Records sqlglot parse trees and same-dialect roundtrip SQL for the conformance corpus."""
import gzip, json, sys, enum
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect


def ser(v):
    if isinstance(v, exp.Expr):
        out = {"k": v.__class__.__name__, "a": [[k, ser(x)] for k, x in v.args.items()]}
        if v.comments:
            out["c"] = list(v.comments)
        return out
    if isinstance(v, list):
        return [ser(x) for x in v]
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, int):
        return {"int": v}
    if isinstance(v, exp.DType):
        return {"dt": v.name}
    if isinstance(v, enum.Enum):
        return {"enum": str(v.value)}
    return {"repr": repr(v)}


corpus = json.load(open(sys.argv[1]))
out = []
for r in corpus:
    d = Dialect.get_or_raise(r["dialect"])
    rec = {"dialect": r["dialect"], "sql": r["sql"]}
    try:
        trees = d.parse(r["sql"])
        rec["trees"] = [ser(t) for t in trees]
        try:
            rec["out"] = [d.generate(t) if t is not None else None for t in trees]
        except Exception as e:
            rec["gen_error"] = f"{type(e).__name__}: {e}"
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"
    out.append(rec)
with gzip.open(sys.argv[2], "wt") as f:
    json.dump(out, f)
print(len(out), sum(1 for r in out if "error" in r), sum(1 for r in out if "gen_error" in r))
