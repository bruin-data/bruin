# `data-diff` Command

The `data-diff` command compares data between two tables from the same or different data sources. It provides comprehensive schema comparison, statistical analysis, and data profiling to help identify differences between datasets across environments or sources.

This command is particularly useful for:
- Comparing tables between development, staging, and production environments
- Validating data migrations and ETL processes
- Monitoring data drift between different data sources
- Ensuring data consistency across different systems

## Usage

```bash
bruin data-diff [FLAGS] <table1> <table2>
```

By default, this command exits with a status code of `0` even when differences are found. Use `--fail-if-diff` to exit with a non-zero code when differences are detected.

**Arguments:**

- **table1:** The first table to compare. Can be specified as `connection:table` or just `table` if using a default connection.
- **table2:** The second table to compare. Can be specified as `connection:table` or just `table` if using a default connection.

<style>
table {
  width: 100%;
}
table th:first-child,
table td:first-child {
  white-space: nowrap;
}
</style>

## Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--connection`, `-c` | str | - | Name of the default connection to use when connection is not specified in table arguments |
| `--tolerance`, `-t` | float | `0.001` | Tolerance percentage for considering values equal. Values with percentage difference below this threshold are considered equal |
| `--config-file` | str | `.bruin.yml` | Optional path to the `.bruin.yml` configuration file . Other [secret backends](../secrets/overview.md) can be used.|
| `--schema-only` | bool | `false` | Compare only table schemas without analyzing row counts or column distributions |
| `--fail-if-diff` | bool | `false` | Return a non-zero exit code if differences are found |
| `--target-dialect` | str | auto-detect | Target SQL dialect for ALTER TABLE statements (postgresql, snowflake, bigquery, duckdb, generic). Auto-detected from connection types if not specified |
| `--reverse` | bool | `false` | Reverse the direction of ALTER statements (transform Table1 to match Table2 instead of Table2 to match Table1) |

## Table Identifier Format

Tables can be specified in two formats:

1. **With connection prefix:** `connection_name:table_name`
   - Example: `prod_db:users`, `staging_bq:events`

2. **Without connection prefix:** `table_name` 
   - Requires the `--connection` flag to specify the default connection
   - Example: `users` (when using `--connection prod_db`)

## What Gets Compared

The `data-diff` command performs a comprehensive comparison that includes:

### Schema Comparison
- **Column names and types:** Identifies missing, extra, and mismatched columns
- **Data type compatibility:** Checks if different types are comparable (e.g., `VARCHAR` vs `STRING`)
- **Nullability constraints:** Compares nullable/not-null settings
- **Uniqueness constraints:** Compares unique/non-unique settings

### Statistical Analysis
For each column that exists in both tables, the command provides detailed statistics based on the column's data type:

#### Numerical Columns
- Row count and null count
- Fill rate (percentage of non-null values)
- Min, Max, Average, Sum values
- Standard deviation

#### String Columns  
- Row count and null count
- Fill rate (percentage of non-null values)
- Distinct value count
- Empty string count
- Min, Max, and Average string lengths

#### Boolean Columns
- Row count and null count
- Fill rate (percentage of non-null values)  
- True and False counts

#### DateTime Columns
- Row count and null count
- Fill rate (percentage of non-null values)
- Distinct value count
- Earliest and Latest dates

#### JSON Columns
- Row count and null count
- Fill rate (percentage of non-null values)

### Difference Calculation
- **Absolute differences:** Raw numeric differences between values
- **Percentage differences:** Relative changes as percentages
- **Tolerance handling:** Values within the specified tolerance are considered equal
- **Color-coded output:** Green for matches/small differences, red for significant differences

## Output Format

The command generates several detailed tables:

1. **Summary Table:** High-level overview of row counts and schema differences
2. **Column Types Comparison:** Side-by-side comparison of column types
3. **Schema Comparison:** Detailed breakdown of matching vs different columns
4. **Column Differences:** Specific differences for columns that exist in both tables
5. **Missing Columns:** Columns that exist in one table but not the other
6. **Statistical Comparison:** Detailed statistics for each common column
7. **ALTER TABLE Statements:** SQL statements to synchronize the schemas (when differences are found)

### ALTER TABLE Statement Generation

When schema differences are detected, the command automatically generates ALTER TABLE SQL statements to synchronize the schemas. These statements are output to stdout (separate from the comparison tables which go to stderr), making it easy to capture and review them.

The generated statements can:
- Add missing columns
- Change column data types
- Modify column nullability
- Be combined into a single ALTER TABLE statement where the database dialect supports it

**Direction of Changes:**
- By default, statements transform Table2 to match Table1
- Use `--reverse` to transform Table1 to match Table2 instead

**Dialect Detection:**
- If both connections are the same type (e.g., both PostgreSQL), uses that dialect
- If connections are different types, uses the second table's dialect
- Can be overridden with `--target-dialect` flag

**Supported Dialects:**
- PostgreSQL (`postgresql`)
- Snowflake (`snowflake`)
- BigQuery (`bigquery`)
- DuckDB (`duckdb`)
- Generic SQL (`generic`)

## Examples

### Basic Usage

Compare two tables using explicit connection names:
```bash
bruin data-diff prod_db:users staging_db:users
```

Compare tables using a default connection:
```bash
bruin data-diff --connection my_db users_v1 users_v2
```

### Cross-Environment Comparison

Compare the same table across different environments:
```bash
bruin data-diff prod_bq:analytics.users dev_bq:analytics.users
```

### Custom Tolerance

Use a higher tolerance for considering values equal (useful for floating-point comparisons):
```bash
bruin data-diff --tolerance 0.1 prod_db:metrics staging_db:metrics
```

### Custom Config File

Specify a different configuration file:
```bash
bruin data-diff --config-file /path/to/custom/.bruin.yml prod:table1 staging:table2
```

### Generating ALTER TABLE Statements

Compare schemas and generate SQL statements to synchronize them:
```bash
bruin data-diff prod_db:users staging_db:users
```

Output (stdout):
```sql
-- ALTER TABLE statements to synchronize schemas:
ALTER TABLE "users"
  ADD COLUMN "email" VARCHAR(255) NOT NULL,
  ALTER COLUMN "age" TYPE INTEGER,
  ALTER COLUMN "bio" DROP NOT NULL,
  DROP COLUMN "legacy_code";
```

Columns that only exist in the table being modified are now automatically dropped, alongside column additions and property updates, so the schema truly matches the desired definition.

### Capturing ALTER Statements

Since ALTER statements are sent to stdout, you can easily capture them:
```bash
bruin data-diff prod:users staging:users > schema_sync.sql
```

The comparison tables will still be displayed on stderr, while the SQL goes to the file.

### Reversing ALTER Direction

By default, statements modify Table2 to match Table1. To reverse this:
```bash
bruin data-diff --reverse prod:users staging:users
```

This will generate statements to modify `prod:users` instead of `staging:users`.

### Specifying SQL Dialect

Override auto-detection and specify the target dialect explicitly:
```bash
bruin data-diff --target-dialect postgresql prod_duck:users staging_bq:users
```

This generates PostgreSQL-compatible ALTER statements even when comparing DuckDB and BigQuery tables.

### Schema-Only Comparison

Compare only table schemas without statistical analysis:
```bash
bruin data-diff --schema-only prod:large_table staging:large_table
```

This is faster for large tables when you only need schema differences.

## Supported Data Platforms

The `data-diff` command includes specialized type mapping support for the following data platforms:

- **DuckDB** - Full support for DuckDB data types including `HUGEINT`, `UHUGEINT`, and specialized time types
- **BigQuery** - Native support for BigQuery types including `INT64`, `FLOAT64`, `BIGNUMERIC`, and BigQuery-specific formatting
- **PostgreSQL & AWS Redshift** - Complete support for PostgreSQL types including `SERIAL` types, `MONEY`, network types (`CIDR`, `INET`), and `JSONB`
- **Snowflake** - Full support for Snowflake types including `NUMBER`, `VARIANT`, and timezone-aware timestamp types

### Type Mapping Features

Each supported platform includes intelligent type mapping that:

- **Normalizes data types** across platforms (e.g., PostgreSQL `INTEGER` and BigQuery `INT64` both map to `numeric`)
- **Handles parametrized types** automatically (e.g., `VARCHAR(255)` and `VARCHAR(100)` are both treated as `string`)
- **Supports case-insensitive matching** for type names
- **Maps platform-specific types** to common categories for cross-platform comparisons

### Cross-Platform Comparisons

When comparing tables between different platforms, the command intelligently maps data types to common categories:

- **Numeric types:** All integer, float, decimal, and monetary types
- **String types:** VARCHAR, CHAR, TEXT, and similar text types  
- **Boolean types:** BOOL, BOOLEAN, and logical types
- **DateTime types:** DATE, TIME, TIMESTAMP, and interval types
- **Binary types:** BLOB, BYTEA, BINARY, and similar binary types
- **JSON types:** JSON, JSONB, VARIANT, and structured data types

## Connection Requirements

To use the `data-diff` command, your connections must support table summarization. This means the connection type must implement the `TableSummarizer` interface, which includes the ability to:

- Retrieve table schema information
- Calculate statistical summaries for different column types
- Handle various data types (numerical, string, boolean, datetime, JSON)

For optimal results, use one of the fully supported data platforms listed above. Other database connections may work but may have limited type mapping capabilities.

## Use Cases

### Data Migration Validation
```bash
# Compare source and target after migration
bruin data-diff source_db:customer_data target_db:customer_data
```

### Environment Consistency Checks
If you are comparing different environments, you can use the `--schema-only` flag to only compare the schema of the tables and not the data.
```bash
# Ensure staging matches production structure
bruin data-diff --schema-only prod:important_table staging:important_table --tolerance 0.01
```

### ETL Process Monitoring  
```bash
# Compare before and after transformation
bruin data-diff raw_data:events processed_data:events_cleaned
```

### Data Quality Monitoring
```bash
# Check for unexpected changes in data distribution
bruin data-diff yesterday_snapshot:metrics today_snapshot:metrics
```

## Troubleshooting

### Error: "connection type does not support table summarization"
The specified connection type doesn't support the required table analysis features.

If you'd like to see another platform supported, feel free to open an issue on our [GitHub repository](https://github.com/bruin-data/bruin/issues).

### Error: "incorrect number of arguments"
The command requires exactly two table arguments.

Verify you've provided both table identifiers.

### Error: "connection not specified for table"
Table identifier doesn't include a connection prefix and no default connection was set.

Either use the format `connection:table` or add the `--connection` flag.

### Error: "failed to get connection"
The specified connection name doesn't exist in your configuration.

Check your `.bruin.yml` or other [secrets backend](../secrets/overview.md) file for available connections.
