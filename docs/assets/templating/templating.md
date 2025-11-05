# Templating

Bruin supports [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) as its templating language for SQL assets. This allows you to write dynamic SQL queries that can be parameterized with variables. This is useful when you want to write a query that is parameterized by a date, a user ID, or any other variable.

The following is an example SQL asset that uses Jinja templating for different `start_date` and `end_date` parameters:

```sql
SELECT * FROM my_table WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

Since `start_date` and `end_date` parameters are automatically passed to your assets by Bruin, this allows the same SQL asset definition to be used both as your regular execution, e.g. daily or hourly, as well as backfilling a longer period of time.

You can do more complex stuff such as looping over a list of values, or using conditional logic. Here's an example of a SQL asset that loops over a list of days and dynamically generates column names.
::: tip Example
`pipeline.yaml`
```yaml 
name: sql-pipeline
variables:
  days:
    type: array
    default: [1, 3, 7, 15, 30, 90]
```

`asset.sql`
```sql
SELECT
    conversion_date,
    cohort_id,
    {% for day_n in var.days %}
    SUM(IFF(days_since_install < {{ day_n }}, revenue, 0))
    AS revenue_{{ day_n }}_days
    {% if not loop.last %},{% endif %}
    {% endfor %}
FROM user_cohorts
GROUP BY 1,2
```
:::

> [!TIP]
> Need enumerations, numeric bounds, or nested structures for your variables? Consult the [JSON Schema keyword reference](/getting-started/pipeline-variables#supported-json-schema-keywords) for the full list of `type` values and examples of arrays-of-objects and object-of-arrays patterns you can reuse in templated SQL.

This will render into the following SQL query:

```sql
SELECT
    conversion_date,
    cohort_id,
    SUM(IFF(days_since_install < 1, revenue, 0)) AS revenue_1_days,
    SUM(IFF(days_since_install < 3, revenue, 0)) AS revenue_3_days,
    SUM(IFF(days_since_install < 7, revenue, 0)) AS revenue_7_days,
    SUM(IFF(days_since_install < 15, revenue, 0)) AS revenue_15_days,
    SUM(IFF(days_since_install < 30, revenue, 0)) AS revenue_30_days,
    SUM(IFF(days_since_install < 90, revenue, 0)) AS revenue_90_days
FROM user_cohorts
GROUP BY 1,2
```
You can read more about [Jinja here](https://jinja.palletsprojects.com/en/3.1.x/).

## Builtin variables

Bruin injects various variables by default:
| Variable | Description | Example |
|----------|-------------|---------|
| `start_date` | The start date in YYYY-MM-DD format | "2023-12-01" |
| `start_datetime` | The start date and time in YYYY-MM-DDThh:mm:ss format | "2023-12-01T15:30:00" |
| `start_timestamp` | The start timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | "2023-12-01T15:30:00.000000+07:00" |
| `end_date` | The end date in YYYY-MM-DD format | "2023-12-02" |
| `end_datetime` | The end date and time in YYYY-MM-DDThh:mm:ss format | "2023-12-02T15:30:00" |
| `end_timestamp` | The end timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | "2023-12-02T15:30:00.000000Z07:00" |
| `pipeline` | The name of the currently executing pipeline | `my_pipeline` |
| `run_id` | The unique identifier for the current [pipeline run](../../getting-started/concepts.md#pipeline-run) | `run_1234567890` |
| `full_refresh` | Boolean indicating whether the `--full-refresh` flag was used | `True` or `False` |

You can use these variables in your SQL queries by referencing them with the `{{ }}` syntax:
```sql
SELECT * FROM my_table
WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

The `full_refresh` variable is particularly useful for implementing different logic based on whether a full refresh is being performed:
```sql
SELECT * FROM my_table
{% if full_refresh %}
  -- Full refresh: process all historical data
  WHERE created_at >= '2020-01-01'
{% else %}
  -- Incremental: process only recent data
  WHERE created_at >= '{{ start_date }}'
    AND created_at < '{{ end_date }}'
{% endif %}
```

> [!NOTE]
> Date-related variables are passed in as strings, and they will be driven by the flags given to the `bruin run` command, read more on that [here](../../commands/run.md).

You can modify these variables with the use of [filters](./filters.md).

## Conditional Rendering

Jinja templating allows you to write conditional logic in your SQL queries using `{% if %}` statements. This is particularly useful when you want to change query behavior based on runtime conditions.

### Full Refresh vs Incremental Loads

One of the most common use cases for conditional rendering is handling full refresh versus incremental loads. The `full_refresh` variable allows you to write a single asset definition that behaves differently depending on whether you run it with the `--full-refresh` flag.

::: tip Example: Different Date Ranges
```sql
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
FROM raw_orders
WHERE order_date >= {% if full_refresh %}'2020-01-01'{% else %}'{{ start_date }}'{% endif %}
```

When you run this asset normally, it will process data from the `start_date` (typically yesterday or the date you specify). When you run it with `--full-refresh`, it will process all historical data from 2020-01-01 onwards.
:::

You can also use conditional rendering for more complex scenarios:

::: tip Example: Different Aggregation Logic
```sql
{% if full_refresh %}
-- Full refresh: rebuild the entire aggregation table
CREATE OR REPLACE TABLE user_metrics AS
SELECT
    user_id,
    COUNT(*) as total_orders,
    SUM(amount) as lifetime_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM orders
GROUP BY user_id
{% else %}
-- Incremental: update only recent changes
INSERT INTO user_metrics
SELECT
    user_id,
    COUNT(*) as total_orders,
    SUM(amount) as lifetime_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM orders
WHERE order_date >= '{{ start_date }}'
GROUP BY user_id
ON CONFLICT (user_id) DO UPDATE SET
    total_orders = user_metrics.total_orders + EXCLUDED.total_orders,
    lifetime_value = user_metrics.lifetime_value + EXCLUDED.lifetime_value,
    last_order_date = GREATEST(user_metrics.last_order_date, EXCLUDED.last_order_date)
{% endif %}
```

This pattern is useful when you want to completely rebuild a table during full refresh, but use more efficient incremental updates during regular runs.
:::

### Other Conditional Use Cases

Conditional rendering isn't limited to `full_refresh`. You can use any variable in your conditions:

```sql
SELECT * FROM events
WHERE event_date = '{{ start_date }}'
{% if var.environment == 'production' %}
  AND is_test = false
{% endif %}
```

You can read more about [Jinja conditionals](https://jinja.palletsprojects.com/en/3.1.x/templates/#if) in the official Jinja documentation.

## Custom variables
You can define your own variables and use them across your Assets. See [variables](/getting-started/pipeline-variables) for more information.