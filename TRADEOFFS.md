# Replacing the sqlglot (Python) parser with Go — decisions and tradeoffs

This document records the decisions made while replacing Bruin's embedded-Python SQLGlot parser
with a pure-Go implementation. It is written for review; nothing has been pushed anywhere (the
gosqlx work is on the local branch `sqlglot-port` of `.context/gosqlx`, the Bruin work on this branch).

## TL;DR

- Bruin no longer embeds Python. `pkg/sqlparser` keeps its exact public API and now runs an
  in-process Go port of the old Python command server on top of `pkg/sqlglot`, a faithful Go port
  of sqlglot 30.13.0 that lives in the gosqlx fork.
- Parity evidence (all at 100%):
  - all pre-existing sqlparser tests and the full `test/sqlparser-dialect-contracts` suite
    (latest: `dbe969430`), `make test`, `make integration-test-light`;
  - sqlglot's own test-suite corpus for every Bruin dialect plus hive/spark2/presto/sqlite —
    15,124 statements: identical tokens, identical parse trees (node classes, argument order,
    comments) and identical regenerated SQL;
  - a Bruin-level differential: every corpus statement pushed through every Bruin command
    (lineage with and without schemas, tables, rename, limit, CTE/select/freeze-time rewrites,
    DECLARE hoisting, read-only/single-select checks) — 145,326 commands — plus TPC-H, TPC-DS and
    sqlglot's optimizer fixtures with their schemas across all 17 Bruin dialects — 47,124 commands —
    produce byte-identical JSON responses (including error messages) to the Python implementation;
  - mutation fuzzing (756k commands on malformed variants of the corpus) — identical except where
    Python itself is nondeterministic or hangs (§5).
- Speed: the 145k-command differential takes ~4 s single-threaded in Go vs ~55 CPU-seconds in
  Python (plus IPC); the sqlparser test package went from ~42 s to <1 s;
  `bruin internal parse-pipeline -c` on the lineage integration pipeline: 0.03 s vs 0.2 s warm
  (6.2 s vs 3.8 s on a cold first run), byte-identical output.
- Size: the stripped darwin/arm64 binary shrinks from 196 MB to 153 MB, and the repository loses
  ~141 MB of vendored wheels.

## 1. Architecture: a faithful Go port of sqlglot inside the gosqlx fork

**Decision:** the Go implementation lives in the gosqlx fork as a new, self-contained package
`pkg/sqlglot`, its own Go module (`github.com/ajitpratap0/GoSQLX/pkg/sqlglot`, go 1.24, stdlib-only).
It is a line-by-line port of sqlglot v30.13.0 (the exact version Bruin embedded): tokenizer,
expression model, parser, generator, 21 dialects (the 16 Bruin dialects plus hive, spark2, presto
as parents and sqlite, which Bruin's tests use), `transforms`, the `dialects/dialect.py` helpers,
JSON paths, the whole optimizer (qualify, annotate_types incl. per-dialect typing, simplify,
unnest/merge subqueries, pushdown, normalize, canonicalize, eliminate_*, optimize_joins — i.e. the
full default rule list, which Bruin's lineage fallback path uses), schema, scope and lineage.

**Why a nested module?** The gosqlx root module requires Go 1.26.1 and pulls in LSP/MCP
dependencies; Bruin is on Go 1.25. `pkg/sqlglot` has no dependency on the rest of gosqlx, so a
nested module keeps Bruin's dependency graph to a single stdlib-only module.

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
get — and keep — exact parity, and makes future upgrades a mechanical diff against SQLGlot. The
existing gosqlx packages are untouched.

**Generated vs hand-ported code.** Everything that is pure data in SQLGlot (token types, the 1,038
expression classes and their argument specs/MRO, data types, every dialect's tokenizer/parser/
generator settings, keyword tables, token sets, type mappings, unicode case tables) is generated from
the live Python objects by `tools/sqlglotgen/gen.py` (reproducible, gofmt'ed), so it cannot drift.
Procedural code is ported by hand, keeping Python's structure and names (`_parse_table_parts` ->
`parseTableParts`, `table_sql` -> `tableSQL`, dialect overrides in `d_<dialect>*.go`) so the two can
be diffed side by side. Python's virtual dispatch (dialect subclasses overriding parser/generator
methods) is modelled with per-dialect hook tables; class-level callable tables (FUNCTIONS,
TRANSFORMS, ...) are per-dialect maps built once.

**Upgrading sqlglot later:** bump the pin in `gen.py`, regenerate tables and the conformance corpus
(`tools/sqlglotgen/README.md`), then port the procedural diff between the two sqlglot tags — the
conformance tests point at exactly what changed.

## 2. Conformance testing

1. **Bruin's own tests**: all pre-existing sqlglot-backed tests plus the contract suite from
   `test/sqlparser-dialect-contracts`, merged into this branch (re-fetched regularly; latest
   `dbe969430`). The contract suite was first validated against the Python implementation.
2. **SQLGlot's own test-suite corpus** (`pkg/sqlglot/conformance_*_test.go`): every SQL string used
   in sqlglot's `tests/dialects/*` and `tests/fixtures/identity.sql` for the 21 ported dialects
   (15,124 statements), recorded through the real Python implementation: tokens, full parse trees
   and same-dialect regenerated SQL. 100% identical.
3. **Bruin-level differential** (harness in `.context/harness`, not committed: it needs the deleted
   Python sources): the same corpus run through every Bruin command, and sqlglot's optimizer/TPC
   fixtures through lineage/rename/tables with their schemas, comparing the full JSON responses with
   what the Python command loop returned — 192,450 commands, 100% identical.
4. **Mutation fuzzing** against Python (see §5).
5. Agents also ran per-module differential checks while porting (e.g. all 467 simplify fixtures,
   1,014 annotate_types fixtures, the optimizer pipeline on TPC-H/TPC-DS, 7,099 ISO-date strings
   against CPython's C `fromisoformat`, 4,000 Decimal operations).
6. Exact parse-tree comparison distinguishes `False` from `None` (an extra 394 trees were fixed for
   this; `falsediff_test.go` is the diagnostic).

## 3. Deliberate deviations / known edges

- **`False` vs `None` argument values** are reproduced exactly (they differ for required-argument
  validation, see §5); the conformance comparison is exact.
- **Python-specific error texts.** Some error messages Bruin surfaced come from Python itself rather
  than from SQLGlot (`'NoneType' object has no attribute 'find_all'`, `'Block' object has no attribute
  'limit'`, `object of type 'Add' has no len()`, ...). The contract tests pin several of them, so the
  Go code reproduces these strings verbatim at the same call sites (as `ValueError`s). They are poor
  UX and could be replaced with clearer messages in a follow-up; doing so only requires updating the
  assertions.
- **Logging.** SQLGlot logs warnings (unsupported syntax falling back to `Command`, unsupported
  generator features); Bruin sent those to `~/.bruin/pylogs`. The port does not log by default
  (`sqlglot.Logger` is an overridable no-op hook).
- **Concurrency.** SQLGlot's CONNECT BY parsing mutates a class-level table temporarily; the Go port
  keeps that state per parser instance so concurrent parses are safe. `pkg/sqlparser` keeps the old
  semantics of one command at a time per `SQLParser` instance (previously one Python process each)
  via a per-instance mutex; different instances run in parallel. Validated by replaying the full
  145k-command differential from 8 goroutines under `go test -race`: no data races, identical outputs.
- **BigQuery's global coercion side effect.** Importing SQLGlot's BigQuery dialect mutates the shared
  `TypeAnnotator.COERCES_TO` table (DECIMAL/BIGINT gain BIGDECIMAL, VARCHAR gains date/time types), so
  in Python, type inference for *every* dialect changes once BigQuery has been used in the process
  (e.g. by the DECLARE-hoisting BigQuery probe). The port mirrors this: the extra coercions apply once
  the BigQuery dialect has been instantiated in the process. (In Python this state was per parser
  process; in Go it is per OS process.)
- **Import-order-dependent quirks.** Two more SQLGlot behaviors depend on which dialect module
  Python imported first (Hive/Databricks snapshot the coercion table; Athena's Trino generator keeps
  JSON-path transforms only if Athena loads before Trino). The port picks the order Bruin's Python
  process effectively had.
- **Raw strings inside expression lists.** A few SQLGlot helpers put a raw SQL string inside an
  expression list (`groupconcat_sql`, `array_append_sql`); Go lists hold expressions only, so the
  string is wrapped in a `Var`, which renders verbatim — the generated SQL is identical.
- **Function builders that mutate their argument list.** Several SQLGlot builders `pop`/`insert`/`del`
  on the `args` list they receive (e.g. T-SQL `HASHBYTES`, `build_json_extract_path`), and the parser
  then validates arity against the *mutated* list. A Go builder cannot change its caller's slice
  length, so such builders return `WithValidateArgs(expr, newArgs)` (a transient metadata entry that
  `callFuncBuilder` strips immediately) and the parser / `exp.func` ports validate against it.
- **Python object reprs.** One ClickHouse JSON-path parse stores the Dialect object inside the tree;
  its Python repr contains a memory address, so the parse-tree comparison normalizes it.

## 4. Changes in Bruin

- **Deleted:** `pythonsrc/` (the JSON-over-stdin command server and its Python tests),
  `internal/data` (≈141 MB of per-platform sqlglot wheels, plus the CPython runtime pulled in via the
  `go-embed-python` module), `internal/generate` (the pip packaging step), the `go-embed-python`
  dependency, and the `make lint-python` target (it only linted `pythonsrc`; `make format` no longer
  depends on it). Doc references (AGENTS.md), `.gitattributes` and `.golangci.yml` exclusions updated.
- **`pkg/sqlparser`:** public API unchanged. `sendCommand` still JSON-roundtrips each request (so
  inputs are normalized exactly like before: map key order, nil vs empty maps, numbers) and calls
  `dispatch`, an in-process Go port of `pythonsrc/main.py`, `parser/main.py` and `rename.py`
  (`glot.go`, `glot_lineage.go`, `glot_ops.go`). `Start`/`Close` are kept as cheap lifecycle no-ops;
  there is no subprocess anymore, so the parser can no longer hang or desynchronize, and the first
  call no longer pays the CPython extraction/start-up cost.
- **One test assertion changed:** `TestSQLParser_HoistingStartsLazilyAndPreservesErrors` asserted
  that starting the parser *extracts embedded files* into its temp dir. That is an implementation
  detail of the Python embedding; it now asserts the directory stays empty. Everything else in the
  test (lazy start, hoisting results, error preservation) is unchanged. No other expectation changed.
- **Dependency wiring (local only for now):** `go.mod` requires
  `github.com/ajitpratap0/GoSQLX/pkg/sqlglot` with a `replace` to `./.context/gosqlx/pkg/sqlglot`
  (same in `integration-tests/cloud-integration-tests/clickhouse`, the only nested module that
  imports `pkg/sqlparser`). **Before shipping:** push the gosqlx branch, tag the nested module
  (`pkg/sqlglot/vX.Y.Z`), drop the `replace` lines and `go get` the tag — or, if you prefer to keep
  it in-tree, move `pkg/sqlglot` under Bruin (it has no dependencies, so this is a copy).

## 5. Robustness: mutation fuzzing

Bruin feeds arbitrary user SQL to the parser, so malformed input matters as much as valid input.
`.context/harness/gen_bruin_fuzz.py` derives ~54k mutants from the corpus (truncation, dropped,
duplicated and swapped tokens) and records Python's answer to 7 Bruin commands for each
(378,042 commands, 5 s timeout per command); the Go side replays them with a 10 s timeout.

Findings and decisions:

- **Python itself is not deterministic on some error messages.** `Expr.error_messages` iterates
  `required_args`, a `set` of strings, so when several required arguments are missing, which one
  "Required keyword: 'x' missing" names depends on `PYTHONHASHSEED`. The port reports them in
  argument-definition order (deterministic); the fuzz comparison ignores the keyword name. The same
  holds for messages that print a Python set (e.g. merge_subqueries' "Joins {'t', 's'} missing
  source table"): element order differs between Python runs.
- **Stack overflows must never happen in Go.** Python turns runaway recursion into a catchable
  `RecursionError` ("maximum recursion depth exceeded"); a Go stack overflow kills the process. The
  port bounds recursion at the parser's recursive entry points (`maxParseDepth`) and in scope
  recursion (self-referential union scopes, which SQLGlot builds for malformed set operations because
  of a variable-shadowing bug in `_traverse_union` that the port reproduces), raising the same
  message. The limits are far above Python's (~100 nested parentheses), so Go accepts deeper SQL.
- **Infinite loops.** 7 mutants make SQLGlot loop forever (e.g. `CALLED ON NULL INPUTAS ...`: the
  property parser retreats inside a `while True` loop; T-SQL `SYSTEM_VERSIONING=ON(...)` with an
  unknown option). Python — and therefore old Bruin — hangs on them. The port has a generic
  no-progress guard in the parser's match primitives: 10M match attempts without reaching a new token
  position raise "Parser made no progress (infinite loop on malformed input)" (~0.1–1 s). This is a
  deliberate deviation (an error instead of a hang).
- **Python exceptions on malformed input** (`AttributeError`, `KeyError`, `IndexError`,
  `decimal.InvalidOperation`, `TypeError`) are reproduced at the same call sites with the same
  `str(e)` text, since Bruin surfaces those strings; unexpected Go runtime index errors map to
  "list index out of range".
- **`False` vs `None`.** Every SQLGlot `_match*` helper returns `False`, so `x = self._match(...) and
  self._parse_y()` stores `False`, which *passes* required-argument validation where `None` fails.
  The port stores `false` at those sites too (parse-tree comparison is exact for False/None).

Result, two seeds (756,063 commands): every command Python completes produces the same JSON as
Go, except a single response whose message embeds a Python `set` (hash-order dependent, see
above). The 93 commands on which Python hangs (13 distinct inputs) return the no-progress error
in Go.

## 6. Follow-ups worth considering

- Replace the verbatim Python exception texts with clearer messages (contract assertions pin some).
- Teradata and other sqlglot dialects Bruin does not map to are not ported (Presto's `TO_CHAR` embeds
  Teradata's time-format table instead). Adding a dialect is mechanical: codegen + `d_<dialect>.go`.
- Several agents wrote small private copies of the same helpers (`is_type`, `_binop`, `exp.func`,
  `list.remove` with structural equality); they are correct but could be unified.
