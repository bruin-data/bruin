# Query Command

The `query` command executes and retrieves the results of a query on a specified connection and
returns the results in table format, JSON, or CSV.

You can run it in three modes:

- **Direct query**: provide `--connection` and `--query`
- **Asset query**: provide `--asset` (optional `--environment`) to execute the SQL from an asset file
- **Auto-detect**: provide `--asset` + `--query` to run an ad-hoc query using the asset's connection and dialect

**Flags:**

| Flag                 | Alias | Description                                                                 |
|----------------------|-------|-----------------------------------------------------------------------------|
| `--connection`       | `-c`  | The name of the connection to use (direct query mode).                     |
| `--query`            | `-q`  | The SQL query to execute.                                                  |
| `--asset`            |       | Path to a SQL asset file within a Bruin pipeline.                          |
| `--environment`      | `--env` | Target environment name as defined in `.bruin.yml`.                      |
| `--start-date`       |       | Start date for query variables in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`.    |
| `--end-date`         |       | End date for query variables in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`.      |
| `--limit`            | `-l`  | Limit the number of rows returned.                                         |
| `--timeout`          | `-t`  | Timeout for query execution in seconds (default: 1000).                    |
| `--output [format]`  | `-o`  | Output type: `plain`, `json`, `csv`.                                       |
| `--export`           |       | Export results to a CSV file.                                              |
| `--split-rows`       |       | Split export into multiple CSV files with at most this many rows per file (requires `--export`). |
| `--config-file`      |       | The path to the `.bruin.yml` file.                                         |

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
