# Query Command

The `query` command executes and retrieves the results of a query on a specified connection and
returns the results in table format, JSON, or CSV.

You can run it in three modes:

- **Direct query**: provide `--connection` and `--query`
- **Asset query**: provide `--asset` (optional `--environment`) to execute the SQL from an asset file
- **Auto-detect**: provide `--asset` + `--query` to run an ad-hoc query using the asset's connection and dialect
- **Semantic query**: provide `--semantic-model` with `--asset` or `--pipeline` to query a [semantic model](/core-concepts/semantic-layer)

**Flags:**

| Flag                 | Alias | Description                                                                 |
|----------------------|-------|-----------------------------------------------------------------------------|
| `--connection`       | `-c`  | The name of the connection to use (direct query mode).                     |
| `--query`            | `-q`  | The SQL query to execute.                                                  |
| `--asset`            |       | Path to a SQL asset file within a Bruin pipeline.                          |
| `--pipeline`         |       | Path to a Bruin pipeline. Used with `--semantic-model` when no asset is provided. |
| `--environment`      | `--env` | Target environment name as defined in `.bruin.yml`.                      |
| `--semantic-model`   |       | Semantic model name to compile and query.                                  |
| `--metric`           |       | Semantic metric to select. Can be passed multiple times.                   |
| `--dimension`        |       | Semantic dimension to select. Use `name:granularity` for time dimensions. Can be passed multiple times. |
| `--filter`           |       | Semantic filter as JSON, for example `{"dimension":"country","operator":"equals","value":"US"}`. Can be passed multiple times. |
| `--segment`          |       | Semantic segment to apply. Can be passed multiple times.                   |
| `--sort`             |       | Semantic sort field. Use `name:asc` or `name:desc`. Can be passed multiple times. |
| `--var`              |       | Set a Jinja template variable for query rendering. Supports flat, dot-notation nested, and JSON values. Can be passed multiple times. See [Template variables](#template-variables). |
| `--start-date`       |       | Start date for query variables in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`.    |
| `--end-date`         |       | End date for query variables in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`.      |
| `--limit`            | `-l`  | Limit the number of rows returned.                                         |
| `--timeout`          | `-t`  | Timeout for query execution in seconds (default: 1000).                    |
| `--output [format]`  | `-o`  | Output type: `plain`, `json`, `csv`.                                       |
| `--export`           |       | Export results to a CSV file.                                              |
| `--split-rows`       |       | Split export into multiple CSV files with at most this many rows per file (requires `--export`). |
| `--config-file`      |       | The path to the `.bruin.yml` file.                                         |
| `--dangerously-bypass-soft-limits` | | Bypass BigQuery soft query limits configured on the connection. |

## Example

```bash
bruin query --connection my_connection --query "SELECT * FROM table"
```

**Example output:**

```plaintext
+-------------+-------------+----------------+
|   Column1   |   Column2   |    Column3     |
+-------------+-------------+----------------+
| Value1      | Value2      | Value3         |
| Value4      | Value5      | Value6         |
| Value7      | Value8      | Value9         |
+-------------+-------------+----------------+
```

## Splitting Large Exports

When exporting large query results, you can use `--split-rows` to split the output into multiple CSV files. This is useful when:

- Your query returns millions of rows that are too large for a single file
- You need to process the data in chunks
- You're working with tools that have file size limitations

**Example:**

```bash
# Export a large table, splitting into files of 400,000 rows each
bruin query --connection my_connection --query "SELECT * FROM large_table" --export --split-rows 400000
```

If your query returns 1,000,000 rows with `--split-rows 400000`, you'll get 3 files:

- `query_result_<timestamp>_part1.csv` (400,000 rows)
- `query_result_<timestamp>_part2.csv` (400,000 rows)
- `query_result_<timestamp>_part3.csv` (200,000 rows)

Each file includes the header row with column names.

## Template variables

You can inject Jinja template variables into a query with `--var`. The flag can be passed
multiple times and supports flat, nested, and JSON values, so you can test dashboard-style
SQL — including nested `filters.*` variables — directly from the CLI.

**Flat variables:**

```bash
bruin query --connection my_connection \
  --query "SELECT * FROM events WHERE date >= '{{ start_date }}'" \
  --var start_date=2026-05-20
```

**Nested variables (dot-notation):**

Use a dotted key to build a nested object. This matches the nested `filters` object the
dashboard runtime injects, so `{{ filters.start_date }}` resolves as expected:

```bash
bruin query --connection my_connection \
  --query "SELECT * FROM events WHERE date >= '{{ filters.start_date }}'" \
  --var filters.start_date=2026-05-20 \
  --var filters.end_date=2026-05-27
```

**Nested variables (JSON):**

You can also pass a whole object (or array) as a JSON value:

```bash
bruin query --connection my_connection \
  --query "SELECT * FROM events WHERE date >= '{{ filters.start_date }}'" \
  --var filters='{"start_date":"2026-05-20","end_date":"2026-05-27"}'
```

Scalar values are kept as literal strings (matching how pipeline variables work in YAML);
only values that look like a JSON object or array are parsed as JSON.

## Semantic Queries

Semantic query mode compiles metrics, dimensions, segments, filters, joins, and windows from YAML models in the repository-level `semantic` directory. See the [semantic layer documentation](/core-concepts/semantic-layer) for model syntax.

Use an asset path when you want Bruin to infer the pipeline, connection, and SQL dialect from an existing SQL asset:

```bash
bruin query \
  --asset ./pipelines/daily-orders/assets/orders.sql \
  --semantic-model orders \
  --dimension order_date:month \
  --metric revenue \
  --metric avg_order_value \
  --filter '{"dimension":"country","operator":"equals","value":"US"}' \
  --segment completed \
  --sort order_date:asc \
  --output json
```

Use a pipeline path when there is no anchor asset. In this mode, pass the connection explicitly:

```bash
bruin query \
  --pipeline ./pipelines/daily-orders \
  --connection warehouse \
  --semantic-model orders \
  --dimension customers.country \
  --metric revenue \
  --sort revenue:desc \
  --output csv
```

Semantic query mode cannot be combined with `--query`.
