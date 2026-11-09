# `render` Command

The `render` command processes a Bruin SQL asset and generates a SQL query or materialized output for execution.
It supports multiple databases and output formats, making it a flexible tool for rendering assets in Bruin pipelines.

## Usage

```bash
bruin render [path to asset definition] [flags]
bruin render --stdin --connection <name> [flags]
```

<img alt="Bruin - clean" src="/render.gif" style="margin: 10px;" />
### Arguments

**path-to-asset-definition** (required unless `--stdin` is set):

- The file path to the Bruin SQL asset you want to render.

### Flags

| Flag                  | Alias | Description                                                           |
|-----------------------|-------|-----------------------------------------------------------------------|
| `--raw-query`         |       | Output only the raw SELECT query. |
| `--stdin`             |       | Read raw SQL from standard input instead of an asset file. Requires `--connection`; see [Rendering raw SQL](#rendering-raw-sql). |
| `--connection`        | `-c`  | The connection used to render SQL from `--stdin`. Its dialect and tables drive schema-prefix rewriting. |
| `--full-refresh`      | `-r`  | Truncate the table before running the query. Also sets the `full_refresh` jinja variable to `True`. With `--stdin` it only sets the variable. |
| `--start-date`        |       | Specify the start date in `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` format.|
| `--end-date`          |       | Specify the end date in `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` format. |
| `--apply-interval-modifiers` |       | Apply [interval modifiers](/assets/interval-modifiers). Off unless the flag is given — useful for previewing the window a Bruin Cloud run would use, since Cloud always applies them. |
| `--var`               |       | Override pipeline variables with custom values. With `--stdin`, values are decoded as JSON when valid. |
| `--output [format]`   | `-o`  | Specify the output format (e.g., `json`). Defaults to console output.  |
| `--environment`       | `-e`, `--env` | The environment from `.bruin.yml` to render for. Applies its `schema_prefix`; see [Schema prefixes](#schema-prefixes). |
| `--config-file`       |       | The path to the `.bruin.yml` file. |

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

**Render Raw SQL for a Connection:**

```bash
echo "SELECT * FROM raw.events" | bruin render --stdin --connection my_connection --raw-query --output json
```

## Rendering raw SQL

`bruin render --stdin` renders arbitrary SQL for a connection and environment without an asset file. It uses the same Jinja and schema-prefix pipeline as [`bruin query`](/commands/query), so schema-prefix behavior matches:

- Jinja rendering runs first, including `{{ schema_prefix }}`, `{{ start_date }}`, `{{ end_date }}`, `{{ full_refresh }}` and variables passed with `--var`.
- When the selected environment has a `schema_prefix`, each `schema.table` reference is rewritten to `<prefix>schema.table` only if the prefixed table exists on the connection. Quoted or differently-cased identifiers are matched exactly and left alone when they do not name an existing prefixed table.
- No asset exists, so no materialization or asset hooks are applied.
- `--full-refresh` sets the `full_refresh` Jinja variable. It does not materialize anything.
- Connections marked `use_cloud: true` skip local schema-prefix rewriting, since there is no local connection to inspect.

The SQL is read from stdin. With `--output json`, the final SQL is written to stdout as a JSON object:

```bash
cat query.sql | bruin render --stdin --connection my_connection --raw-query --output json
```

```json
{"query":"SELECT * FROM jane_raw.events"}
```

`--var` values are decoded as JSON when valid, so SDKs and scripts can pass typed values:

```bash
bruin render --stdin --connection my_connection --raw-query --output json \
  --var 'filters={"start_date":"2024-01-01"}' --var count=10 \
  < query.sql
```

This mode is used by the Python SDK's `bruin.render()` and `bruin.query()` helpers. See the [Python SDK documentation](/assets/python-sdk) for the client side.

## Schema prefixes

When the environment has a [`schema_prefix`](/getting-started/devenv#schema-based-environments), `render` shows the asset as `bruin run` would build it in that environment. It uses the default environment unless you pass `--environment`.

- The asset is created under the prefixed schema, e.g. `jane_mart.table1` instead of `mart.table1`.
- `{{ schema_prefix }}` renders to the prefix and `{{ this }}` to the prefixed asset name.
- References to the asset's own table, e.g. in an incremental query, point at the prefixed table.

Rendering an asset file does not connect to the database, so other tables the query reads are not rewritten. `bruin run` points those at the prefixed schema when the prefixed table exists. To see that rewrite, run the query with [`bruin query --asset`](/commands/query#schema-prefixes) or render raw SQL with [`bruin render --stdin`](#rendering-raw-sql), which does connect to check the prefixed tables.
