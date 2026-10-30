# `render-ddl` Command

The `render-ddl` command prints the `CREATE TABLE` statement for a SQL asset, built from the asset's declared [`columns`](/assets/columns). It renders the asset as if it used the [`ddl` materialization strategy](/assets/materialization#ddl), whatever strategy the asset actually declares, and does not connect to the database.

The asset needs `materialization.type: table`. An asset with no materialization prints its query unchanged, and a view fails with an unsupported materialization error.

Use it to review or hand off a table's schema before the asset runs, or to create the table outside Bruin.

## Usage

```bash
bruin render-ddl [flags] [path to asset definition]
```

### Arguments

**path-to-asset-definition** (required):

- The file path to the Bruin SQL asset you want to render.

### Flags

| Flag | Alias | Description |
|------|-------|-------------|
| `--start-date` | | Start date in `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` format. Used for Jinja rendering. Defaults to the beginning of the previous UTC day. Can also be set with `BRUIN_START_DATE`. |
| `--end-date` | | End date in the same formats as `--start-date`. Defaults to the end of the previous UTC day. Can also be set with `BRUIN_END_DATE`. |
| `--apply-interval-modifiers` | | Apply [interval modifiers](/assets/interval-modifiers) to the start and end dates. |
| `--output [format]` | `-o` | Output format. Set to `json` to print `{"query": "..."}`. Defaults to plain SQL. |
| `--config-file` | | The path to the `.bruin.yml` file. Only read for Athena assets, to pick up the connection's `query_results_path`. Defaults to `.bruin.yml` at the Git repository root. Can also be set with `BRUIN_CONFIG_FILE`. |

## Example

Given this asset:

```bruin-sql
/* @bruin
name: analytics.orders
type: duckdb.sql
materialization:
  type: table
columns:
  - name: order_id
    type: integer
    primary_key: true
    description: Unique order ID
  - name: amount
    type: double
    description: Order total
@bruin */

SELECT 1 AS order_id, 10.5 AS amount
```

Run:

```bash
bruin render-ddl assets/orders.sql
```

Output:

```sql
CREATE TABLE IF NOT EXISTS analytics.orders (
  order_id integer,
  amount double,
  PRIMARY KEY (order_id)
);
COMMENT ON COLUMN analytics.orders.order_id IS 'Unique order ID';
COMMENT ON COLUMN analytics.orders.amount IS 'Order total';
```

The exact syntax depends on the asset's platform. Options such as `partition_by` and `cluster_by` are included where the platform supports them; see [partitioning and clustering](/assets/materialization#partitioning-and-clustering).

## Related

- [`render`](/commands/render): render the full query Bruin would run for an asset.
- [Materialization](/assets/materialization): all materialization strategies.
