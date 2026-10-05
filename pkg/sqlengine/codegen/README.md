# pkg/sqlengine codegen

Tooling behind `pkg/sqlengine`, the Go port of [sqlglot](https://github.com/tobymao/sqlglot)
(MIT, see `../LICENSE.sqlglot`).
Everything here runs against the pinned sqlglot version (`SQLGLOT_VERSION` in `gen.py`, currently
30.13.0) installed in a Python virtualenv, plus a checkout of the sqlglot repository at the same tag
(for its test-suite).

```bash
python -m venv venv && venv/bin/pip install "sqlglot==30.13.0" pytz python-dateutil
git clone --branch v30.13.0 https://github.com/tobymao/sqlglot sqlglot-src
```

## Generated tables (`zz_*.go`)

Everything in sqlglot that is plain data — token types, expression classes (argument specs, MRO,
flags), data types, every dialect's tokenizer/parser/generator/dialect settings, keyword tables,
type mappings, Unicode case tables — is generated from the live Python objects:

```bash
venv/bin/python pkg/sqlengine/codegen/gen.py pkg/sqlengine   # from the Bruin repository root
```

The output is formatted with gofumpt (as `make format` does) and reproducible; CI-style check: regenerate into a temp dir and `cmp`.
To add a dialect, add it to `DIALECTS` in `gen.py`, register it in `pkg/sqlengine/dialects.go`, and
port its parser/generator customizations in `pkg/sqlengine/d_<dialect>*.go`.

## Conformance corpus (`pkg/sqlengine/testdata`)

1. `extract_corpus.py <sqlglot-src> corpus.json` records every `(dialect, sql)` pair used by
   sqlglot's `tests/dialects/*` and `tests/fixtures/identity.sql` for the dialects in its `want`
   set (statement order depends on `PYTHONHASHSEED`; content does not).
2. `gen_tokens_expected.py corpus.json pkg/sqlengine/testdata/tokens.json.gz` records Python's tokens.
3. `gen_parse_expected.py corpus.json pkg/sqlengine/testdata/parse.json.gz` records Python's parse
   trees (class, ordered args, comments) and same-dialect regenerated SQL.

`conformance_tokens_test.go` / `conformance_parse_test.go` replay them. Useful env vars:
`SQLGLOT_CONF_DIALECT=<d>` (filter), `SQLGLOT_CONF_DUMP=/tmp/conf` (write mismatches),
`SQLGLOT_CONF_STRICT=1` (fail on any mismatch).
