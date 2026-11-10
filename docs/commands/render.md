# `render` Command

The `render` command processes a Bruin SQL asset and generates a SQL query or materialized output for execution.
It supports multiple databases and output formats, making it a flexible tool for rendering assets in Bruin pipelines.

## Usage

```bash
bruin render [path to asset definition] [flags]
bruin render --stdin --connection <name> [flags] < query.sql
```

<img alt="Bruin - clean" src="/render.gif" style="margin: 10px;" />
### Arguments

**path-to-asset-definition** (required unless `--stdin` is given):

- The file path to the Bruin SQL asset you want to render.

### Flags

| Flag                  | Alias | Description                                                           |
|-----------------------|-------|-----------------------------------------------------------------------|
| `--raw-query`         |       | Output only the raw SELECT query. |
| `--full-refresh`      | `-r`  | Truncate the table before running the query. Also sets the `full_refresh` jinja variable to `True`. |
| `--start-date`        |       | Specify the start date in `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` format.|
| `--end-date`          |       | Specify the end date in `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` format. |
| `--apply-interval-modifiers` |       | Apply [interval modifiers](/assets/interval-modifiers). Off unless the flag is given — useful for previewing the window a Bruin Cloud run would use, since Cloud always applies them. |
| `--var`               |       | Override pipeline variables with custom values. |
| `--output [format]`   | `-o`  | Specify the output format (e.g., `json`). Defaults to console output.  |
| `--environment`       | `-e`, `--env` | The environment from `.bruin.yml` to render for. Applies its `schema_prefix`; see [Schema prefixes](#schema-prefixes). |
| `--config-file`       |       | The path to the `.bruin.yml` file. |
| `--stdin`             |       | Render the SQL on standard input instead of an asset file. Requires `--connection`; see [Rendering SQL from standard input](#rendering-sql-from-standard-input). |
| `--connection`        | `-c`  | The connection to render `--stdin` SQL for. Only valid with `--stdin`. |
| `--this`              |       | With `--stdin`, the name of the asset the SQL belongs to, as `bruin run` names it in the environment (e.g. `dev_mart.events`). Defaults to `BRUIN_THIS`, which Python assets get. |

### Examples

**Render an Asset with Default Settings :**

```bash
bruin render path/to/sample.asset.yml
```

**Render an Asset with a Date Range:**

```bash
bruin render path/to/sample.asset.yml --start-date 2024-01-01 --end-date 2024-01-31
```

**Render an Asset in JSON Format:**

```bash
bruin render path/to/sample.asset.yml --output json
```

**Render an Asset with Full Refresh:**

```bash
bruin render path/to/sample.asset.yml --full-refresh
```

**Render an Asset for a Developer Environment:**

```bash
bruin render path/to/sample.asset.yml --environment dev
```

**Render SQL from Standard Input for a Connection:**

```bash
echo "SELECT * FROM raw.events" | bruin render --stdin --connection my_connection --environment dev --output json
```

## Rendering SQL from standard input

`bruin render --stdin --connection <name>` renders SQL without an asset file. It shows the SQL [`bruin query --connection <name> --query`](/commands/query) would run on that connection and environment, without running it:

- The SQL is rendered as one Jinja template with the same variables as `bruin query`, including `{{ start_date }}`, `{{ end_date }}` and `{{ schema_prefix }}`. `{{ full_refresh }}` follows `--full-refresh`.
- When the environment has a `schema_prefix`, the same schema-prefix rewrite as [`bruin query`](/commands/query#schema-prefixes) is applied. Render connects to the database to see which prefixed tables exist.
- There is no asset, so no materialization or hooks are applied, and `--full-refresh` only sets `{{ full_refresh }}`.

The output is the rendered SQL; `--raw-query` is implied. With `--output json` it is a single JSON object:

```bash
bruin render --stdin --connection my_connection --environment dev --output json < query.sql
```

```json
{"query":"SELECT * FROM dev_raw.events"}
```

### Variables

`--var key=value` values are decoded as JSON, as they are when rendering an asset or running a pipeline, so `--var count=10` is a number and `--var 'name="alice"'` is the string `alice`. A value that is not valid JSON, such as `--var day=2024-01-01`, is used as a string. Integers too large for a 64-bit integer keep their exact digits. Keys accept dot notation, e.g. `--var filters.country='"US"'`.

Variables are available both by name, e.g. `{{ count }}` as in `bruin query`, and under `var`, e.g. `{{ var.count }}` as in asset SQL.

`bruin query` keeps scalar `--var` values as written, so `--var 'name="alice"'` renders as `"alice"` there, with the quotes. Pass plain values (`--var name=alice`) when the output has to match a `bruin query` run.

### Self references

Python assets can query their own table. During `bruin run` in an environment with a `schema_prefix`, the asset is named with the prefix, e.g. `dev_mart.events`, and Python assets get that name as `BRUIN_THIS`. With `--this`, or `BRUIN_THIS` set, references written exactly as the unprefixed name (`mart.events`) point at the prefixed table even before it exists, the same rule `render` uses for an asset's own table. `{{ this }}` renders to that name.

### Connections that run in Bruin Cloud

For a connection marked `use_cloud: true`, there is no local connection to check, so tables are left as written. Jinja is still rendered. `bruin query` sends SQL for these connections to Bruin Cloud without rendering it.

## Schema prefixes

When the environment has a [`schema_prefix`](/getting-started/devenv#schema-based-environments), `render` shows the asset as `bruin run` would build it in that environment. It uses the default environment unless you pass `--environment`.

- The asset is created under the prefixed schema, e.g. `jane_mart.table1` instead of `mart.table1`.
- `{{ schema_prefix }}` renders to the prefix and `{{ this }}` to the prefixed asset name.
- References to the asset's own table, e.g. in an incremental query, point at the prefixed table.

Rendering an asset file does not connect to the database, so other tables the query reads are not rewritten. `bruin run` points those at the prefixed schema when the prefixed table exists. To see that rewrite, run the query with [`bruin query --asset`](/commands/query#schema-prefixes).
