# pkg/sqlengine golden tests

`golden_test.go` runs the corpus below through the engine and compares its observable behavior
with expectations recorded with [sqlglot](https://github.com/tobymao/sqlglot), the library the
engine was forked from. Only behavior is compared, never internals: parse trees, token streams and
error wording are the engine's own business.

| family | input | expectations | compares |
|---|---|---|---|
| parse | `statements.jsonl` | `parse.json.gz` | whether each statement parses, and the SQL it regenerates in its dialect |
| annotate | `annotate.jsonl` | `annotate.json.gz` | the inferred types of each query's output columns |
| lineage | `lineage.jsonl`, `schemas.json` | `lineage.json.gz` | each query's output columns after the optimizer rules column lineage runs, with their types and the (table, column) pairs they read |

- `statements.jsonl`: one `{"dialect", "sql"}` per line, 19,830 statements over 21 dialects,
  mostly from sqlglot's test-suite (v30.13.0 and v30.33.0) plus malformed statements.
- `annotate.jsonl`: `{"dialect", "sql", "schema"}`, 3,562 queries and expressions, including
  sqlglot's type-inference fixtures; `schema: true` uses the tables in `record.py`.
- `lineage.jsonl`: `{"dialect", "sql", "schema"}`, 2,516 queries from sqlglot's optimizer fixtures
  and TPC-H / TPC-DS in five dialects; `schema` names a schema in `schemas.json`.

## Recording with sqlglot

```bash
python -m venv venv && venv/bin/pip install "sqlglot==30.33.0"
venv/bin/python pkg/sqlengine/testdata/golden/record.py            # all families, ~40 s
venv/bin/python pkg/sqlengine/testdata/golden/record.py parse      # or: annotate, lineage
```

`record.py` reads only the files in this directory and writes byte-identical output on every run.
Two normalizations keep sqlglot's output usable as the reference (see the script): set-operation
modifiers are moved in a fixed order, and BigQuery's coercion rules do not leak into the other
dialects.

## Adding cases

Append lines to an input file and run `record.py`. The test fails if an input has no recorded
expectation.

## Deliberate differences

Where the engine deliberately behaves differently from sqlglot, the input is listed in
`<family>.deviations.jsonl` (`parse`, `annotate`, `lineage`): its dialect, SQL, a `reason`, and
the record the engine is expected to produce instead. The test compares such inputs against that
record, and fails when a deviation has no reason, no longer differs from the recording, or names an
input that is gone. The `*.json.gz` recordings always stay exactly as sqlglot produced them.

After an intentional behavior change, rewrite the deviation lists from the Go output, then fill in
the reason of each new entry and review the diff:

```bash
go test ./pkg/sqlengine -run TestGolden -update
```

To see how a newer sqlglot release changes the recordings, decode and compare the `.json.gz`
files, e.g. with [jq](https://jqlang.org):

```bash
git show HEAD:pkg/sqlengine/testdata/golden/parse.json.gz | gzip -dc | jq -S . > /tmp/old.json
gzip -dc pkg/sqlengine/testdata/golden/parse.json.gz | jq -S . > /tmp/new.json
diff /tmp/old.json /tmp/new.json
```

`go test ./pkg/sqlengine -run TestGolden -golden.dump=/tmp/mismatches.jsonl` writes every
mismatch as a JSON line (family, input, recorded and computed records).
