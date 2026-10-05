"""Records testdata/golden/commands.json.gz: every corpus statement in a Bruin dialect through every
command pkg/sqlparser sends, answered by the Python reference implementation.

usage: gen_golden_commands.py [out.json.gz]
"""

import multiprocessing as mp
import os
import sys

import golden_common as g


def commands_for(entry):
    import sqlglot
    from sqlglot import exp

    d, sql = entry["dialect"], entry["sql"]
    q = {"query": sql, "dialect": d}
    cmds = [
        {"command": "get-tables", "contents": dict(q)},
        {"command": "is-read-only", "contents": dict(q)},
        {"command": "is-single-select", "contents": dict(q)},
        {"command": "add-limit", "contents": {**q, "limit": 10}},
        {"command": "extract-select", "contents": dict(q)},
        {
            "command": "freeze-time",
            "contents": {**q, "execution_time": "2024-05-06T07:08:09"},
        },
        {"command": "hoist-declares", "contents": dict(q)},
        {
            "command": "add-ctes",
            "contents": {
                **q,
                "ctes": [{"name": "bruin_cte", "query": "SELECT 1 AS x"}],
            },
        },
        {"command": "lineage", "contents": {**q, "schema": {}}},
    ]
    tables = g.run({"command": "get-tables", "contents": dict(q)}).get("tables") or []
    if tables:
        mapping = {t: "renamed_" + t.replace(".", "_") for t in sorted(tables)}
        cmds.append(
            {
                "command": "replace-table-references",
                "contents": {**q, "table_mapping": mapping},
            }
        )
        cols = []
        try:
            parsed = sqlglot.parse_one(sql, dialect=d)
            if parsed is not None:
                cols = sorted({c.name for c in parsed.find_all(exp.Column) if c.name})
        except Exception:  # noqa: BLE001
            pass
        types = ["int", "varchar", "timestamp", "decimal(10, 2)", "boolean", "date"]
        schema = {}
        for i, t in enumerate(sorted(tables)):
            names = cols[i :: len(tables)] or ["id"]
            schema[t] = {n: types[(i + j) % len(types)] for j, n in enumerate(names)}
        cmds.append({"command": "lineage", "contents": {**q, "schema": schema}})
    ctename = "missing_cte"
    try:
        parsed = sqlglot.parse_one(sql, dialect=d)
        if parsed is not None:
            ctes = list(parsed.find_all(exp.CTE))
            if ctes:
                ctename = ctes[0].alias
    except Exception:  # noqa: BLE001
        pass
    cmds.append({"command": "select-cte", "contents": {**q, "cte_name": ctename}})
    return cmds


def process(entry):
    return g.record(commands_for(entry))


if __name__ == "__main__":
    out = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.path.join(g.GOLDEN_DIR, "commands.json.gz")
    )
    corpus = g.load_corpus()
    with mp.get_context("fork").Pool(mp.cpu_count(), initializer=g.init_worker) as pool:
        results = []
        for i, r in enumerate(pool.imap(process, corpus, chunksize=8)):
            results.extend(r)
            if i % 1000 == 0:
                print(i, len(corpus), file=sys.stderr, flush=True)
    g.write(out, results)
