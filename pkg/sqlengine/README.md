# pkg/sqlengine

Bruin's SQL engine: tokenizer, expression tree, per-dialect parser and generator, scope analysis,
optimizer rules and column lineage. `pkg/sqlparser` builds Bruin's parser commands (lineage, used
tables, renames, limits, DECLARE hoisting, ...) on top of it.

It started as a Go port of [sqlglot](https://github.com/tobymao/sqlglot) v30.13.0 and is now
maintained here as a fork. Behavior changes from newer sqlglot releases are brought in
selectively (parsing coverage, parsing fixes, generated SQL, type inference and lineage; currently
from v30.33.0), not line by line: the golden tests compare behavior with that release. sqlglot's MIT license is kept in [`LICENSE.sqlglot`](LICENSE.sqlglot).

## Layout

| files | contents |
|---|---|
| `tokenizer.go`, `token_types.go`, `tokenizer_settings.go` | tokenizer, token types, per-dialect tokenizer tables |
| `expr*.go`, `kind*.go`, `dtype_table.go`, `datatypes.go` | expression tree, expression classes (`Kind`), data types |
| `parser*.go`, `parser_settings.go` | parser and per-dialect parser tables |
| `gen*.go`, `generator*.go` | SQL generator and per-dialect generator tables |
| `dialect*.go`, `d_<dialect>*.go` | dialect registry and dialect-specific parsing/generation |
| `opt_*.go`, `typing*.go`, `scope.go`, `schema.go`, `lineage.go` | optimizer rules, type inference, scopes, schema, lineage |
| `transforms.go`, `jsonpath.go` | AST transforms shared by dialects, JSON path parsing |

## Tests

- **Unit tests** (`*_test.go`) state their expectations inline.
- **Golden tests** (`golden_test.go`) compare the engine's behavior with sqlglot over a corpus of
  ~20k statements across 21 dialects, 3.5k type-inference cases and 2.5k lineage queries: whether
  each statement parses and the SQL it regenerates, the types of a query's output columns, and
  each output column's type and source columns. Internals (parse trees, tokens, error wording)
  are not compared. See [`testdata/golden`](testdata/golden/README.md).
- `pkg/sqlparser` has its own golden tests for the parser commands.

## Behavior notes

Where the engine deliberately differs from sqlglot:

- **Lineage keeps working where newer sqlglot gives up or guesses wrong** (the golden inputs
  affected by this and the next item are listed, with reasons, in
  `testdata/golden/*.deviations.jsonl`):
  - qualification still infers columns when the schema is empty or incomplete;
  - a parenthesized top-level query (`(SELECT a FROM x) LIMIT 1`) keeps the lineage of the inner
    query;
  - a projection alias referenced in the same query resolves even when a source has no schema,
    including over table functions with unknown columns (`read_parquet(...)`, `numbers(10)`);
  - a table function or UNNEST alias used as a value (`... CROSS JOIN UNNEST(x.a) AS elem`)
    resolves to that source;
  - pseudocolumns such as Oracle's `LEVEL` are never attributed to a table.
- **SQL is not rewritten where that changes its meaning:** `SORT_ARRAY(x)` and Hive's
  `SORT_ARRAY(x, FALSE)` are kept, since the newer rewrites move NULLs or drop the descending order;
  MySQL `ALTER TABLE` options are kept as written; DuckDB `~` / `!~` stay operators, since whether
  they mean a full or a partial match depends on the server version; a byte escape (`\xHH` or
  `\OOO` in ClickHouse, Postgres `E'...'` and Redshift strings) that is not part of a UTF-8
  character stays that byte, written back in an escape the target reads as a byte (`'\xFF'` stays
  `'\xff'`, not `'ÿ'`, whose UTF-8 bytes differ; Redshift gets `'\377'`).

- **Error messages name expression classes plainly** (`Required keyword: 'expression' missing
  for Mul`), not by their Python module path.
- **Deterministic order** where sqlglot iterates a Python set: missing required arguments are
  reported in definition order, and ORDER BY / LIMIT / OFFSET move onto a set operation in sorted
  order.
- **No infinite loops or stack overflows on malformed input.** A parser that makes no progress
  for 10M match attempts fails with "Parser made no progress (infinite loop on malformed input)",
  and recursion is bounded (`maxParseDepth`), raising "maximum recursion depth exceeded".
- **BigQuery's coercion rules apply to BigQuery only.** sqlglot adds them to a table shared by
  every dialect once BigQuery is loaded, so its type inference depended on what ran earlier.
- **Expression lists hold expressions only.** Where sqlglot puts a raw string into a list, the
  engine wraps it in a `Var` (same SQL), and a bare `false` in a `Properties` list (malformed DDL)
  is dropped.
- **A modifier a property does not take is a parse error** (`DEFAULT ENGINE=x` gives "Cannot
  parse property 'ENGINE'"); sqlglot fails with a Python `TypeError` on some of these paths.
- **No logging**: parse errors collected under `ErrorLevelWarn` go to `Logger`, a no-op hook by
  default; the generator's unsupported-feature warnings are dropped.

Other behavior to know about:

- `false` and absent (nil) argument values are distinct, as in sqlglot: required-argument
  validation accepts `false`.
- The SQL comment hints `/* sqlglot.meta ... */` (expression metadata) and
  `/* sqlglot.anonymous */` (parse a known function as an anonymous call) keep their names, so SQL
  written for sqlglot behaves the same.
- Some error texts still read like Python exceptions (`'NoneType' object has no attribute ...`,
  `list index out of range`) because Bruin's contract tests pin them. Replacing them with clearer
  messages only needs those assertions updated.
