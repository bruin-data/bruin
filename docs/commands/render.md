# `render` Command

The `render` command processes a Bruin SQL asset and generates a SQL query or materialized output for execution.
It supports multiple databases and output formats, making it a flexible tool for rendering assets in Bruin pipelines.

## Usage

```bash
bruin render [path to asset definition] [flags]
```

<img alt="Bruin - clean" src="/render.gif" style="margin: 10px;" />
### Arguments

**path-to-asset-definition** (required):

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

## Schema prefixes

When the environment has a [`schema_prefix`](/getting-started/devenv#schema-based-environments), `render` shows the asset as `bruin run` would build it in that environment. It uses the default environment unless you pass `--environment`.

- The asset is created under the prefixed schema, e.g. `jane_mart.table1` instead of `mart.table1`.
- `{{ schema_prefix }}` renders to the prefix and `{{ this }}` to the prefixed asset name.
- References to the asset's own table, e.g. in an incremental query, point at the prefixed table.

`render` does not connect to the database, so other tables the query reads are not rewritten. `bruin run` points those at the prefixed schema when the prefixed table exists. To see that rewrite, run the query with [`bruin query --asset`](/commands/query#schema-prefixes).
