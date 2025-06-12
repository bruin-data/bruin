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
| `--config-file` | str | `.bruin.yml` | The path to the .bruin.yml configuration file |

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

## Connection Requirements

To use the `data-diff` command, your connections must support table summarization. This means the connection type must implement the `TableSummarizer` interface, which includes the ability to:

- Retrieve table schema information
- Calculate statistical summaries for different column types
- Handle various data types (numerical, string, boolean, datetime, JSON)

Supported connection types include most major databases and data warehouses that Bruin supports.

## Use Cases

### Data Migration Validation
```bash
# Compare source and target after migration
bruin data-diff source_db:customer_data target_db:customer_data
```

### Environment Consistency Checks
```bash
# Ensure staging matches production structure
bruin data-diff prod:important_table staging:important_table --tolerance 0.01
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

**Error: "connection type does not support table summarization"**
- The specified connection type doesn't support the required table analysis features
- Ensure you're using a supported database connection

**Error: "incorrect number of arguments"**
- The command requires exactly two table arguments
- Verify you've provided both table identifiers

**Error: "connection not specified for table"**
- Table identifier doesn't include a connection prefix and no default connection was set
- Either use the format `connection:table` or add the `--connection` flag

**Error: "failed to get connection"**
- The specified connection name doesn't exist in your configuration
- Check your `.bruin.yml` file for available connections 