"""Records testdata/golden/fuzz.json.gz: malformed variants of the corpus statements (truncated,
dropped, duplicated and swapped tokens) through seven commands, answered by the Python reference
implementation. Commands that run longer than 5 s are recorded as {"__timeout__": true} (Python
hangs on a few inputs; the Go test does not compare them).

usage: gen_golden_fuzz.py [out.json.gz] [seed]
"""

import multiprocessing as mp
import os
import random
import re
import sys

import golden_common as g

TOKEN_RE = re.compile(r"\s+|\w+|'[^']*'|\"[^\"]*\"|`[^`]*`|.", re.S)


def mutants(sql, rnd):
    toks = TOKEN_RE.findall(sql)
    out = []
    if len(sql) > 4:
        out.append(sql[: rnd.randrange(1, len(sql))])  # truncate
    if len(toks) > 2:
        i = rnd.randrange(len(toks))
        out.append("".join(toks[:i] + toks[i + 1 :]))  # drop a token
        j = rnd.randrange(len(toks))
        out.append("".join(toks[: j + 1] + toks[j:]))  # duplicate a token
        a, b = rnd.randrange(len(toks)), rnd.randrange(len(toks))
        t2 = list(toks)
        t2[a], t2[b] = t2[b], t2[a]
        out.append("".join(t2))  # swap two tokens
    return out


class Timeout(BaseException):
    pass


def _alarm(signum, frame):
    raise Timeout()


def timed_run(cmd):
    import signal

    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(5)
    try:
        return g.run(cmd)
    except Timeout:
        return {"__timeout__": True}
    finally:
        signal.alarm(0)


def process(item):
    d, sql = item
    q = {"query": sql, "dialect": d}
    return g.record(
        [
            {"command": "get-tables", "contents": dict(q)},
            {"command": "lineage", "contents": {**q, "schema": {}}},
            {"command": "is-single-select", "contents": dict(q)},
            {"command": "is-read-only", "contents": dict(q)},
            {"command": "add-limit", "contents": {**q, "limit": 5}},
            {"command": "hoist-declares", "contents": dict(q)},
            {"command": "extract-select", "contents": dict(q)},
        ],
        runner=timed_run,
    )


if __name__ == "__main__":
    out = (
        sys.argv[1] if len(sys.argv) > 1 else os.path.join(g.GOLDEN_DIR, "fuzz.json.gz")
    )
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    rnd = random.Random(seed)
    items = []
    for e in g.load_corpus():
        for m in mutants(e["sql"], rnd):
            if m.strip():
                items.append((e["dialect"], m))
    with mp.get_context("fork").Pool(mp.cpu_count(), initializer=g.init_worker) as pool:
        results = []
        for r in pool.imap(process, items, chunksize=16):
            results.extend(r)
    g.write(out, results)
