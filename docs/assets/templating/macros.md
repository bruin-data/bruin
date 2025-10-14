# Macros

Macros are reusable pieces of Jinja code that allow you to define SQL patterns once and use them throughout your pipeline. Think of them as functions for your SQL queries—they help you write cleaner, more maintainable code by eliminating repetition.

## Why Use Macros?

Macros are useful when you find yourself:
- Writing the same SQL patterns repeatedly across multiple assets
- Building complex queries that could be simplified with reusable components
- Maintaining consistency in how certain operations are performed
- Wanting to abstract away complexity from your main query logic

## Getting Started

### Directory Structure

To use macros in your pipeline, create a `macros/` folder at the root of your pipeline directory:

```
my-pipeline/
├── pipeline.yml
├── macros/
│   ├── aggregations.sql
│   ├── filters.sql
│   └── transformations.sql
└── assets/
    ├── customers.sql
    └── orders.sql
```

### File Naming

Macro files must end with `.sql`. You can organize your macros into multiple files based on their purpose (e.g., `aggregations.sql`, `filters.sql`, `transformations.sql`), or keep them all in a single file.

### Automatic Loading

All macros defined in the `macros/` folder are automatically loaded and available in all your assets. You don't need to import them explicitly—just define them once and use them anywhere.

## Defining Macros

### Basic Syntax

A macro is defined using the `{% macro %}` tag:

```jinja
{% macro macro_name(parameter1, parameter2) -%}
    SQL code here
{%- endmacro %}
```

The `{%-` and `-%}` syntax strips whitespace, keeping your rendered output clean.

### Simple Macro Without Parameters

Here's a basic macro that selects all columns from a table:

```jinja
{% macro select_all(table_name) -%}
SELECT * FROM {{ table_name }}
{%- endmacro %}
```

Usage in an asset:
```bruin-sql
/* @bruin
name: my_asset
type: duckdb.sql
@bruin */

{{ select_all('customers') }}
```

### Macro With Multiple Parameters

```jinja
{% macro date_range_filter(table, date_column, start_date, end_date) -%}
SELECT *
FROM {{ table }}
WHERE {{ date_column }} >= '{{ start_date }}'
  AND {{ date_column }} < '{{ end_date }}'
{%- endmacro %}
```

### Macro With Default Parameters

```jinja
{% macro top_n(table, column, n=10) -%}
SELECT *
FROM {{ table }}
ORDER BY {{ column }} DESC
LIMIT {{ n }}
{%- endmacro %}
```

Usage:
```bruin-sql
-- Uses default n=10
{{ top_n('sales', 'revenue') }}

-- Override with n=5
{{ top_n('sales', 'revenue', 5) }}
```

## Using Macros in Assets

Once defined in your `macros/` folder, macros can be used in any SQL asset by calling them with the `{{ }}` syntax.

### Simple Usage

::: tip Example
`macros/aggregations.sql`
```jinja
{% macro count_by(table, column) -%}
SELECT
    {{ column }},
    COUNT(*) as count
FROM {{ table }}
GROUP BY {{ column }}
ORDER BY count DESC
{%- endmacro %}
```

`assets/country_summary.sql`
```bruin-sql
/* @bruin
name: country_summary
type: duckdb.sql

materialization:
  type: table
@bruin */

{{ count_by('customers', 'country') }}
```
:::

This renders to:
```sql
SELECT
    country,
    COUNT(*) as count
FROM customers
GROUP BY country
ORDER BY count DESC
```

### Using Macros with Bruin Variables

Macros work seamlessly with Bruin's built-in variables like `start_date` and `end_date`:

```bruin-sql
/* @bruin
name: daily_orders
type: duckdb.sql
@bruin */

{{ date_range_filter('orders', 'order_date', start_date, end_date) }}
```

### Combining Macros with Jinja Features

You can use macros together with other Jinja features like loops and conditionals:

```bruin-sql
/* @bruin
name: multi_country_report
type: duckdb.sql
@bruin */

{% for country in ['USA', 'UK', 'Canada'] %}
{{ count_by('orders', 'product_id') }}
WHERE country = '{{ country }}'
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
```

## Practical Examples

### Aggregation Patterns

#### Count By Column
```jinja
{% macro count_by(table, column, order_by='count') -%}
SELECT
    {{ column }},
    COUNT(*) as count
FROM {{ table }}
GROUP BY {{ column }}
ORDER BY {{ order_by }} DESC
{%- endmacro %}
```

Usage:
```bruin-sql
{{ count_by('orders', 'customer_id') }}
```

#### Sum By Column
```jinja
{% macro sum_by(table, group_column, sum_column) -%}
SELECT
    {{ group_column }},
    SUM({{ sum_column }}) as total
FROM {{ table }}
GROUP BY {{ group_column }}
ORDER BY total DESC
{%- endmacro %}
```

Usage:
```bruin-sql
{{ sum_by('orders', 'customer_id', 'order_amount') }}
```

### Filtering Patterns

#### Recent Records
```jinja
{% macro recent_records(table, date_column, days=7) -%}
SELECT *
FROM {{ table }}
WHERE {{ date_column }} >= CURRENT_DATE - INTERVAL '{{ days }} days'
{%- endmacro %}
```

Usage:
```bruin-sql
-- Get orders from last 7 days (default)
{{ recent_records('orders', 'order_date') }}

-- Get orders from last 30 days
{{ recent_records('orders', 'order_date', 30) }}
```

#### Filter Null Values
```jinja
{% macro filter_null(table, columns) -%}
SELECT *
FROM {{ table }}
WHERE {% for col in columns %}
    {{- col }} IS NOT NULL
    {%- if not loop.last %} AND {% endif %}
{%- endfor %}
{%- endmacro %}
```

Usage:
```bruin-sql
{{ filter_null('customers', ['email', 'phone', 'address']) }}
```

Renders to:
```sql
SELECT *
FROM customers
WHERE email IS NOT NULL
  AND phone IS NOT NULL
  AND address IS NOT NULL
```

#### Filter by List of Values
```jinja
{% macro in_list(table, column, values) -%}
SELECT *
FROM {{ table }}
WHERE {{ column }} IN (
    {%- for val in values %}
        '{{ val }}'
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
)
{%- endmacro %}
```

Usage:
```bruin-sql
{{ in_list('orders', 'status', ['pending', 'processing', 'shipped']) }}
```

### Transformation Patterns

#### Deduplicate Records
```jinja
{% macro deduplicate(table, partition_column, order_column) -%}
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY {{ partition_column }}
               ORDER BY {{ order_column }} DESC
           ) as rn
    FROM {{ table }}
)
WHERE rn = 1
{%- endmacro %}
```

Usage:
```bruin-sql
-- Keep most recent record for each customer
{{ deduplicate('customer_events', 'customer_id', 'event_timestamp') }}
```

#### Generate Surrogate Key
```jinja
{% macro generate_surrogate_key(columns) -%}
MD5(CONCAT_WS('||',
    {%- for col in columns %}
        CAST({{ col }} AS VARCHAR)
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
)) as surrogate_key
{%- endmacro %}
```

Usage:
```bruin-sql
SELECT
    {{ generate_surrogate_key(['customer_id', 'order_id']) }},
    *
FROM orders
```

#### Safe Division
```jinja
{% macro safe_divide(numerator, denominator, default=0) -%}
CASE
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
    THEN {{ default }}
    ELSE {{ numerator }}::DOUBLE / {{ denominator }}::DOUBLE
END
{%- endmacro %}
```

Usage:
```bruin-sql
SELECT
    product_id,
    revenue,
    units_sold,
    {{ safe_divide('revenue', 'units_sold') }} as avg_price
FROM product_sales
```

## Advanced Usage

### Macro Composition

Macros can call other macros, allowing you to build more complex functionality:

::: tip Example
`macros/base.sql`
```jinja
{% macro date_range_filter(table, date_column, start_date, end_date) -%}
SELECT *
FROM {{ table }}
WHERE {{ date_column }} >= '{{ start_date }}'
  AND {{ date_column }} < '{{ end_date }}'
{%- endmacro %}
```

`macros/aggregations.sql`
```jinja
{% macro daily_summary(table, date_column, start_date, end_date) -%}
WITH filtered AS (
    {{ date_range_filter(table, date_column, start_date, end_date) }}
)
SELECT
    {{ date_column }}::DATE as day,
    COUNT(*) as total_count,
    COUNT(DISTINCT customer_id) as unique_customers
FROM filtered
GROUP BY {{ date_column }}::DATE
ORDER BY day
{%- endmacro %}
```

`assets/daily_orders.sql`
```bruin-sql
/* @bruin
name: daily_orders
type: duckdb.sql
@bruin */

{{ daily_summary('orders', 'created_at', start_date, end_date) }}
```
:::

### Using Macros with Pipeline Variables

Combine macros with pipeline variables for powerful parameterization:

::: tip Example
`pipeline.yml`
```yaml
name: sales_pipeline
variables:
  regions:
    type: array
    default: ['US', 'EU', 'APAC']
  min_revenue:
    type: integer
    default: 1000
```

`macros/filters.sql`
```jinja
{% macro revenue_filter(table, min_amount) -%}
SELECT *
FROM {{ table }}
WHERE revenue >= {{ min_amount }}
{%- endmacro %}
```

`assets/regional_sales.sql`
```bruin-sql
/* @bruin
name: regional_sales
type: duckdb.sql
@bruin */

{% for region in var.regions %}
{{ revenue_filter('sales', var.min_revenue) }}
AND region = '{{ region }}'
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
```
:::

### Dynamic Column Generation

Use macros to generate columns dynamically:

```jinja
{% macro revenue_by_days(days_list) -%}
{% for day in days_list %}
SUM(CASE WHEN days_since_start <= {{ day }} THEN revenue ELSE 0 END) AS revenue_{{ day }}_days
{%- if not loop.last %},{% endif %}
{% endfor %}
{%- endmacro %}
```

Usage:
```bruin-sql
SELECT
    cohort_date,
    {{ revenue_by_days([7, 14, 30, 90]) }}
FROM user_cohorts
GROUP BY cohort_date
```

## Complete End-to-End Example

Here's a full example showing how to structure and use macros in a pipeline:

::: tip Complete Example
```
sales-pipeline/
├── pipeline.yml
├── macros/
│   ├── aggregations.sql
│   └── filters.sql
└── assets/
    ├── daily_sales.sql
    └── top_customers.sql
```

`pipeline.yml`
```yaml
name: sales_pipeline
default_connections:
  duckdb: "duckdb-default"
```

`macros/aggregations.sql`
```jinja
{% macro count_by(table, column) -%}
SELECT
    {{ column }},
    COUNT(*) as count
FROM {{ table }}
GROUP BY {{ column }}
ORDER BY count DESC
{%- endmacro %}

{% macro sum_by(table, group_column, sum_column) -%}
SELECT
    {{ group_column }},
    SUM({{ sum_column }}) as total
FROM {{ table }}
GROUP BY {{ group_column }}
ORDER BY total DESC
{%- endmacro %}
```

`macros/filters.sql`
```jinja
{% macro date_range(table, date_column, start_date, end_date) -%}
SELECT *
FROM {{ table }}
WHERE {{ date_column }} >= '{{ start_date }}'
  AND {{ date_column }} < '{{ end_date }}'
{%- endmacro %}
```

`assets/daily_sales.sql`
```bruin-sql
/* @bruin
name: daily_sales
type: duckdb.sql

materialization:
  type: table
@bruin */

WITH filtered_orders AS (
    {{ date_range('raw_orders', 'order_date', start_date, end_date) }}
)
{{ sum_by('filtered_orders', 'order_date::DATE', 'order_amount') }}
```

`assets/top_customers.sql`
```bruin-sql
/* @bruin
name: top_customers
type: duckdb.sql

materialization:
  type: table

depends:
  - daily_sales
@bruin */

{{ sum_by('raw_orders', 'customer_id', 'order_amount') }}
LIMIT 100
```
:::

To see the rendered output:
```bash
bruin render assets/daily_sales.sql
```

## Best Practices

### Organization

- **Group related macros**: Keep aggregations, filters, and transformations in separate files
- **Use descriptive names**: `calculate_revenue_by_region` is better than `calc1`
- **Document complex macros**: Add comments explaining parameters and behavior

```jinja
{#
  Deduplicates records keeping the most recent one per partition.

  Args:
    table: The source table name
    partition_column: Column to partition by (e.g., customer_id)
    order_column: Column to order by (e.g., updated_at)
#}
{% macro deduplicate(table, partition_column, order_column) -%}
...
{%- endmacro %}
```

### Commenting Macros

::: warning Important
SQL comments (`--`) don't prevent Jinja from processing macros. To show example macro code without executing it, use `{% raw %}` blocks:

```sql
-- ❌ Wrong: This will still execute the macro!
-- {{ top_n('orders', 'amount', 10) }}

-- ✅ Correct: Wrap in {% raw %} to show the code without executing
{% raw %}
-- {{ top_n('orders', 'amount', 10) }}
{% endraw %}
```

Everything inside `{% raw %}` ... `{% endraw %}` is treated as literal text, allowing you to document macro examples without executing them.
:::

### Design

- **Keep macros focused**: Each macro should do one thing well
- **Use default parameters**: Make macros easier to use with sensible defaults
- **Make macros reusable**: Avoid hardcoding table names or column names

### Testing

- **Test with `bruin render`**: Always preview the rendered output
```bash
bruin render assets/my_asset.sql
```

- **Start simple**: Build complex macros incrementally
- **Verify rendered SQL**: Ensure the output is valid SQL for your platform

### Performance

- **Avoid over-abstraction**: Don't create macros for everything
- **Consider inline code**: Simple patterns might not need a macro
- **Test performance**: Some complex macros might impact query performance

## Debugging Tips

### Preview Macro Output

Use `bruin render` to see exactly what your macros generate:

```bash
# Render a specific asset
bruin render assets/my_asset.sql

# See the full SQL that will be executed
bruin render assets/my_asset.sql --output json
```

### Check Macro Loading

If your macros aren't working:
1. Verify the `macros/` folder is at the pipeline root
2. Ensure macro files end with `.sql`
3. Check for syntax errors in macro definitions
4. Verify macro names match what you're calling in assets

### Common Issues

**Extra whitespace in output**: Use `{%-` and `-%}` to control whitespace
```jinja
{# Good: strips whitespace #}
{% macro my_macro() -%}
SELECT 1
{%- endmacro %}

{# Bad: includes whitespace #}
{% macro my_macro() %}
SELECT 1
{% endmacro %}
```

**Macro not found**: Ensure the macro is defined before it's used (macro files are loaded in alphabetical order)

## When to Use Macros

**Use macros when:**
- You repeat the same SQL pattern in multiple assets
- You want to standardize certain operations across your pipeline
- You need to abstract away complexity
- You want to make your queries more readable

**Don't use macros when:**
- The pattern is only used once
- The macro would be more complex than the inline code
- You need very dynamic behavior better suited to Python
- Performance is critical and the macro adds overhead

## Related Topics

- [Templating](./templating.md) - Learn about Jinja templating basics
- [Filters](./filters.md) - Use filters to transform variables
- [Pipeline Variables](/getting-started/pipeline-variables) - Define custom variables
- [SQL Assets](../sql.md) - Learn about SQL assets in Bruin
