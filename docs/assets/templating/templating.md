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

## Adding variables

You can add variables in your `pipeline.yml` file. We support all YAML data types to give you the
maximum flexiblity in your variable configuration. `variables` are declared as [JSON Schema object](https://json-schema.org/draft-07/draft-handrews-json-schema-01#rfc.section.4.2.1). Here's a comprehensive example:

::: code-group 
```yaml [pipeline.yml]
name: var-pipeline
variables:
  users:
    type: array
    items:
      type: string
    default: ["jhon", "nick"]
  env:
    type: string
    default: dev
  tags:
    type: object
    properties:
      team:
        type: string
      tenant:
        type: string
    default:
      team: data
      tenant: acme
```
:::
All user defined variables are accessibe via `var` namespace. For example, if you define a variable called `src` it will be available as  <code>&lbrace;&lbrace; var.src &rbrace;&rbrace;</code> in your Assets.

Additionally all top level variables must define a `default` value. This will be used to render your assets in absence of values supplied on the commandline.


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

You can use these variables in your SQL queries by referencing them with the `{{ }}` syntax:
```sql
SELECT * FROM my_table 
WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

> [!NOTE]
> Date-related variables are passed in as strings, and they will be driven by the flags given to the `bruin run` command, read more on that [here](../../commands/run.md).

You can modify these variables with the use of [filters](./filters.md).