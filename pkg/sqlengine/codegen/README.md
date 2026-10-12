# pkg/sqlengine codegen

Scripts that record the golden files in `../testdata` from [sqlglot](https://github.com/tobymao/sqlglot)
(MIT, see `../LICENSE.sqlglot`), the reference implementation `pkg/sqlengine` was forked from.
The goldens pin down behavior only: whether a statement parses, the SQL generated back and the
inferred column types. The Go code does not mirror sqlglot's source and is not generated from it.

The scripts run against a pinned sqlglot version (currently 30.13.0) installed in a Python
virtualenv, plus a checkout of the sqlglot repository at the same tag (for its test-suite):

```bash
python -m venv venv && venv/bin/pip install "sqlglot==30.13.0" pytz python-dateutil
git clone --branch v30.13.0 https://github.com/tobymao/sqlglot sqlglot-src
```

## Golden files

| file | recorded by | replayed by |
|---|---|---|
| `parse.json.gz` | `extract_corpus.py`, then `gen_parse_expected.py` | `conformance_parse_test.go` |
| `annotate.json.gz` | `gen_annotate_expected.py` from `annotate_corpus.json` | `annotate_conformance_test.go` |
| `tpc.json.gz` | `extract_tpc.py` | benchmarks (`bench_test.go`, `../../sqlparser/bench_test.go`) |

1. `extract_corpus.py <sqlglot-src> corpus.json` records every `(dialect, sql)` pair used by
   sqlglot's `tests/dialects/*` and `tests/fixtures/identity.sql` for the dialects in its `want`
   set (statement order depends on `PYTHONHASHSEED`; content does not).
2. `gen_parse_expected.py corpus.json pkg/sqlengine/testdata/parse.json.gz` records whether each
   statement parses and the SQL generated back in the same dialect.
3. `gen_annotate_expected.py pkg/sqlengine/codegen/annotate_corpus.json pkg/sqlengine/testdata/annotate.json.gz`
   records the output column types of each statement, with and without a small schema. The
   statements are ours: add cases to `annotate_corpus.json`.
4. `extract_tpc.py <sqlglot-src> pkg/sqlengine/testdata/tpc.json.gz` extracts the TPC-H and TPC-DS
   queries and schemas.

`parse.json.gz` is also the corpus `../../sqlparser/codegen` records Bruin's command goldens from.

The conformance tests fail on any mismatch. Useful env vars: `CONFORMANCE_DIALECT=<d>` (only one
dialect), `CONFORMANCE_DUMP=/tmp/conf` (write the parse/generate mismatches to files).

## Upgrading sqlglot

Bump the version above, regenerate the goldens and run the tests. Each mismatch is a behavior change
in sqlglot: implement the ones Bruin wants in Go, however fits the engine.
