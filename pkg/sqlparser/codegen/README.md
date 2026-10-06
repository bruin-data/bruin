# pkg/sqlparser codegen

Tooling behind the golden tests in `../golden_test.go`, which replay Bruin's parser commands against
responses recorded from the original Python implementation.

- `pythonsrc/`: Bruin's former embedded parser, copied unchanged from commit `6d1114eaf`. `main.py`
  is the JSON-over-stdin command loop and `parser/` holds the command handlers. `../engine*.go`
  ports it to Go. It runs on sqlglot 30.13.0.
- `golden_common.py`: shared helpers. Runs a command the way `main.py` did, loads the corpus and
  writes golden files.
- `gen_golden_commands.py`, `gen_golden_fixtures.py`, `gen_golden_fuzz.py`, `gen_golden_hoist.py`:
  record `../testdata/golden/{commands,fixtures,fuzz,hoist}.json.gz`.
- `golden_diff.py`: summarizes how two versions of a golden file differ.

## Golden files

| file | contents | commands |
|---|---|---|
| `commands.json.gz` | every statement of the sqlglot conformance corpus (`../../sqlengine/testdata/parse.json.gz`) in a Bruin dialect, through every command `pkg/sqlparser` sends | 145,326 |
| `fixtures.json.gz` | sqlglot's optimizer fixtures (TPC-H, TPC-DS, optimizer.sql, ...) through lineage (with and without their schemas), tables and rename, in every Bruin dialect | 47,124 |
| `fuzz.json.gz` | up to 4 malformed variants per corpus statement (truncated, dropped, duplicated and swapped tokens; seed 1) through 7 commands | 378,042 |
| `hoist.json.gz` | scripts and hook lists composed from DECLAREs, statements, procedural blocks (BEGIN, IF, LOOP, WHILE, TRY, `$$` bodies, ...), comments and separators, plus one malformed variant per script (seed 1), through `hoist-declares` and `hoist-declares-list` in every Bruin dialect | 32,506 |

Each record is `{"cmd": {"command": ..., "contents": ...}, "want": <response>}`. The test replays
`cmd` through `dispatch` and compares the response.

## Recording from Python

Use the same environment as `pkg/sqlengine/codegen`:

```bash
python -m venv venv && venv/bin/pip install "sqlglot==30.13.0" pytz python-dateutil
git clone --branch v30.13.0 https://github.com/tobymao/sqlglot sqlglot-src   # only for the fixtures
```

From the repository root:

```bash
venv/bin/python pkg/sqlparser/codegen/gen_golden_commands.py               # ~10 s on 12 cores
venv/bin/python pkg/sqlparser/codegen/gen_golden_fixtures.py sqlglot-src   # ~15 s
venv/bin/python pkg/sqlparser/codegen/gen_golden_fuzz.py                   # ~2 min
venv/bin/python pkg/sqlparser/codegen/gen_golden_hoist.py                  # ~10 s
```

Each script writes to `pkg/sqlparser/testdata/golden/` by default; pass an output path to write
elsewhere.

- **commands, fixtures and hoist** regenerate exactly.
- **fuzz** has about 1,500 responses whose text depends on `PYTHONHASHSEED`. When several
  required arguments are missing, sqlglot names the first one in a set's iteration order. The Go
  test ignores that keyword when comparing (Go reports them in definition order).
- **Hangs:** commands on which Python ran longer than 5 s are recorded as
  `{"__timeout__": true}` (49 in seed 1) and are not compared. Go returns a "no progress" error for
  them instead of hanging (see TRADEOFFS.md §5).

Record from Python again when the reference itself changes, for example after upgrading sqlglot
(regenerate `pkg/sqlengine/testdata` the same way, see `pkg/sqlengine/codegen/README.md`).

## Intentional behavior changes

Once Bruin deliberately departs from the Python behavior (say, clearer error messages), the
reference can no longer produce the expected output. Rewrite the golden files from the Go output
instead, then review the diff:

```bash
go test ./pkg/sqlparser -run TestGolden -update
git show HEAD:pkg/sqlparser/testdata/golden/commands.json.gz > /tmp/old.json.gz
python3 pkg/sqlparser/codegen/golden_diff.py /tmp/old.json.gz pkg/sqlparser/testdata/golden/commands.json.gz
```

`-update` leaves the `__timeout__` records alone. The first `-update` of `fuzz.json.gz` also
replaces the hash-seed-dependent keyword names with Go's.
