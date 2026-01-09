# Bruin - BigQuery Template

This pipeline is a simple example of a Bruin pipeline for BigQuery,
featuring `example.sql`—a SQL asset that creates a table with sample data and enforces schema constraints
like `not_null`, `unique`, and `primary_key`.

It also includes a `macros/` folder with reusable Jinja macros for common SQL patterns:
- **aggregations.sql**: Common aggregation patterns (count_by, sum_by, top_n)
- **filters.sql**: Common filtering patterns (date_range, recent_records, filter_null, in_list)
- **transformations.sql**: Data transformation helpers (pivot_sum, deduplicate, generate_surrogate_key, safe_divide)

The `macro_example.sql` asset demonstrates how to use these macros in your queries.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/google_cloud_platform.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp-default"
          service_account_file: "/path/to/your/service-account.json"
          project_id: "your-project-id"
```

## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell

bruin run ./bigquery/pipeline.yml
```

You can also run a single task:

```shell
bruin run assets/example.sql
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: example
[2023-03-16T18:26:00Z] [worker-0] Completed: example (1.2s)


Executed 1 tasks in 1.2s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

## Using Macros

The `macros/` folder contains reusable Jinja macros that are automatically available in all your SQL assets. Here are some examples:

### Aggregation Macros
```sql
-- Count records by a column
{{ count_by('example', 'country') }}

-- Sum a column grouped by another
{{ sum_by('sales', 'country', 'revenue') }}

-- Get top N records
{{ top_n('example', 'id', 5) }}
```

### Filter Macros
```sql
-- Filter by date range
{{ date_range('orders', 'created_at', '2024-01-01', '2024-12-31') }}

-- Get recent records (BigQuery syntax)
{{ recent_records('events', 'timestamp', 30) }}

-- Filter out nulls
{{ filter_null('users', ['email', 'name']) }}

-- Filter by list of values
{{ in_list('example', 'country', ['spain', 'germany']) }}
```

### Transformation Macros
```sql
-- Generate a surrogate key from multiple columns
SELECT {{ generate_surrogate_key(['id', 'country']) }}, *
FROM example

-- Safe division (uses BigQuery's SAFE_DIVIDE)
SELECT
    country,
    COUNT(*) as count,
    {{ safe_divide('revenue', 'count') }} as avg_revenue
FROM sales
GROUP BY country

-- Deduplicate records
{{ deduplicate('events', 'user_id', 'timestamp') }}

-- Pivot with sum (BigQuery style)
{{ pivot_sum('sales', 'country', 'product', 'revenue', ['product_a', 'product_b']) }}
```

Check out `assets/macro_example.sql` for more examples!

That's it, good luck!
