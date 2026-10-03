# Replacing the sqlglot (Python) parser with Go — decisions and tradeoffs

This document records the decisions made while replacing Bruin's embedded-Python SQLGlot parser
with a pure-Go implementation. It is written for review; nothing has been pushed anywhere.

## 1. Architecture: a faithful Go port of sqlglot inside the gosqlx fork

**Decision:** the Go implementation lives in the gosqlx fork as a new, self-contained package
`pkg/sqlglot` (module path `github.com/ajitpratap0/GoSQLX/pkg/sqlglot`). It is a line-by-line port of
sqlglot v30.13.0 (the exact version Bruin embeds today): tokenizer, expression model, parser,
generator, the 16 Bruin dialects (+ their parent dialects hive/spark2/presto), `transforms`, the
`dialects/dialect.py` helpers, JSON paths, the optimizer rules Bruin uses (qualify, unnest/merge
subqueries, annotate_types and the full default rule list used by Bruin's fallback path), schema,
scope and lineage.

**Why not extend gosqlx's existing parser/AST?** Bruin's contract tests pin SQLGlot's observable
behavior very precisely: exact regenerated SQL per dialect (e.g. `CROSS JOIN UNNEST(...) AS x`,
`ORDER BY (SELECT NULL) OFFSET ...` for T-SQL, `` `CURRENT_DATE` `` for Doris), the exact set of
accepted/rejected statements and their error messages, SQLGlot's type names
(`DECIMAL(38, 0)`, `STRUCT<name TEXT, age INT>`), and lineage quirks. gosqlx's AST is a fixed set of
Go structs that discards information SQLGlot keeps (identifier quoting, data types as structured
nodes, comments, function-name spelling, argument order) and its parser accepts/rejects a different
language per dialect. Reaching byte-for-byte parity on top of it would have meant re-implementing
SQLGlot's semantics anyway, while fighting a different design and risking regressions in gosqlx's
own consumers (LSP, linter, formatter). Mirroring SQLGlot's structure is the only practical way to
get — and keep — exact parity, and makes future upgrades a mechanical diff against SQLGlot.

The existing gosqlx packages are untouched; `pkg/sqlglot` has no dependency on them and no external
dependencies at all.

**Generated vs hand-ported code.** Everything that is pure data in SQLGlot (token types, the 1,038
expression classes and their argument specs/MRO, data types, every dialect's tokenizer/parser/generator
settings, keyword tables, token sets, type mappings, unicode case tables) is generated from the live
Python objects by `tools/sqlglotgen/gen.py`, so it cannot drift. Procedural code is ported by hand,
keeping Python's structure and names (`_parse_table_parts` -> `parseTableParts`,
`table_sql` -> `tableSQL`) so the two can be diffed side by side.

## 2. Conformance testing

Three layers verify parity:

1. **Bruin's contract suite** (`pkg/sqlparser/*_contract_test.go`, branch
   `test/sqlparser-dialect-contracts`) plus all pre-existing sqlglot-backed Go tests.
2. **SQLGlot's own test-suite corpus**: every SQL string used in sqlglot's `tests/dialects/*` for the
   16 Bruin dialects (13,614 (dialect, SQL) pairs) was recorded through the real Python
   implementation: tokens, full parse trees (including argument order) and same-dialect regenerated
   SQL. `pkg/sqlglot/conformance_*_test.go` replays them against the Go port.
3. Targeted differential checks against a pinned Python sqlglot 30.13.0 for the optimizer/lineage.

## 3. Deliberate deviations / known edges

- **`False` vs `None` argument values.** SQLGlot sometimes stores `False` and sometimes `None` for an
  absent flag. Both are falsy for every consumer (hashing, equality, generation), so the Go port does
  not always reproduce which of the two is stored. The conformance comparison treats them as equal.
- **Python-specific error texts.** A few error messages Bruin surfaced came from Python itself rather
  than from SQLGlot (`'NoneType' object has no attribute 'find_all'`, `'Block' object has no attribute
  'limit'`, ...). The contract tests pin them, so the Go code reproduces these strings verbatim at the
  same call sites. They are poor UX and could be replaced with clearer messages in a follow-up; doing so
  only requires updating those assertions.
- **Logging.** SQLGlot logs warnings (unsupported syntax fallback to `Command`, unsupported generator
  features). The Go port does not log; behavior is otherwise identical.
- **Concurrency.** SQLGlot's CONNECT BY parsing mutates a class-level table temporarily; the Go port
  keeps that state per parser instance so concurrent parses are safe. Type annotation, however, shares
  (and re-parents) cached type nodes exactly like SQLGlot, so `pkg/sqlparser` serializes commands with
  a process-wide mutex. Before, each `SQLParser` was its own Python process handling one command at a
  time, so per-instance throughput is unchanged (and each command is far cheaper without the IPC);
  only cross-instance parallelism is lost. Making annotation re-entrant is a possible follow-up.
- **BigQuery's global coercion side effect.** Importing SQLGlot's BigQuery dialect mutates the shared
  `TypeAnnotator.COERCES_TO` table (DECIMAL/BIGINT gain BIGDECIMAL, VARCHAR gains date/time types), so
  in Python, type inference for *every* dialect changes once BigQuery has been used in the process
  (e.g. by the DECLARE-hoisting BigQuery probe). The port mirrors this faithfully: the extra coercions
  apply once the BigQuery dialect has been instantiated in the process.

- **Python list identity inside builders.** A few SQLGlot helpers put a raw SQL string inside an
  expression list (`groupconcat_sql`, `array_append_sql`); Go lists hold expressions only, so the string
  is wrapped in a `Var`, which renders verbatim — the generated SQL is identical.
- **Function builders that mutate their argument list.** Several SQLGlot builders `pop`/`insert`/`del`
  on the `args` list they receive (e.g. T-SQL `HASHBYTES`, `build_json_extract_path`), and the parser
  then validates arity against the *mutated* list. A Go builder cannot change its caller's slice
  length, so such builders return `WithValidateArgs(expr, newArgs)` (a transient metadata entry that
  `callFuncBuilder` strips immediately) and the parser / `exp.func` ports validate against it.

## 4. Removing Python from Bruin

- `pythonsrc/` (the JSON-over-stdin command server), `internal/data` (≈141 MB of per-platform embedded
  CPython + sqlglot wheels), `internal/generate` (the pip packaging step), the `go-embed-python`
  dependency and the `make lint-python` target (it only linted `pythonsrc`) are deleted.
- `pkg/sqlparser`'s public API is unchanged. `sendCommand` still JSON-roundtrips each request (so
  inputs are normalized exactly like before, e.g. nil maps vs empty maps) and calls an in-process Go
  port of `pythonsrc/main.py` / `parser/main.py` / `rename.py` (`glot*.go`). `Start`/`Close` are kept
  as cheap lifecycle no-ops for API compatibility; there is no subprocess anymore, so the parser can no
  longer hang or desynchronize, and the first call no longer pays the CPython extraction/start cost.

(More entries are added below as the work progresses.)
