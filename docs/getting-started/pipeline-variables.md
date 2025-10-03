# Variables

Bruin lets you parameterize your pipelines with custom variables. These variables are defined in `pipeline.yml` using [JSON Schema](https://json-schema.org/) and are available in your assets during execution.

## Defining variables

Add a `variables` section to your `pipeline.yml` and describe each variable with JSON Schema keywords. Every variable must provide a `default` value so Bruin can render assets without command line overrides.

```yaml [pipeline.yml]
name: var-pipeline
variables:
  env:
    type: string
    default: dev
  users:
    type: array
    items:
      type: string
    default: ["alice", "bob"]
```

## Referencing variables in assets

All variables are accessible in SQL, `seed`, `sensor`, and `ingestr` assets via the `var` namespace.

In Python assets, variables are exposed under `BRUIN_VARS` environment variable. When a pipeline defines no variables, this environment variable contains `{}`.
::: code-group
```sql [asset.sql]
SELECT * FROM events
WHERE user_id IN ({{ ','.join(var.users) }})
```
:::

::: code-group
```python [asset.py]
import os, json
vars = json.loads(os.environ["BRUIN_VARS"])
print(vars["env"])
```
:::
Sensor and ingestr assets, defined as YAML files, can embed variables in the same way:

::: code-group
```yaml [sensor.asset.yml]
name: wait_for_table
type: bq.sensor.query
parameters:
  query: |
    select count(*) > 0 
    from `{{ var.table }}`
    where load_time > {{ start_datetime }}
```
:::
::: code-group
```yaml [ingestr.asset.yml]
name: public.rates
type: ingestr
parameters:
  source_connection: s3
  source_table: '{{ var.bucket }}/rates.csv'
  destination: postgres
```
:::

::: info NOTE
For YAML-style assets, variables can only be used in the value context of `parameter` field.
:::
## Example

::: code-group
```yaml [pipeline.yml]
name: udharan
variables:
  config:
    type: object
    properties:
      name:
        type: string
      version:
        type: object
    default:
      name: bruin
      version: 1.0.0
```
:::

::: code-group
```bruin-sql [asset.sql]
/* @bruin
name: myschema.example
type: duckdb.sql
@bruin */

SELECT 
  '{{ var.config.name }}' as name,
  '{{ var.config.version }}' as version;
```
:::

When run, the rendered query will like the following:
```sql
SELECT 'bruin' as name, '1.0.0' as version;
```

## Built-in variables

Bruin injects several variables automatically:

| Variable | Description | Example |
|----------|-------------|---------|
| `start_date` | The start date in YYYY-MM-DD format | "2023-12-01" |
| `start_datetime` | The start date and time in YYYY-MM-DDThh:mm:ss format | "2023-12-01T15:30:00" |
| `start_timestamp` | The start timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | "2023-12-01T15:30:00.000000+07:00" |
| `end_date` | The end date in YYYY-MM-DD format | "2023-12-02" |
| `end_datetime` | The end date and time in YYYY-MM-DDThh:mm:ss format | "2023-12-02T15:30:00" |
| `end_timestamp` | The end timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | "2023-12-02T15:30:00.000000Z07:00" |
| `pipeline` | The name of the currently executing pipeline | `my_pipeline` |
| `run_id` | The unique identifier for the current pipeline run | `run_1234567890` |
| `is_full_refresh` | Boolean indicating whether the full refresh flag is set | `True` or `False` |

In Python assets these built-ins are exposed as environment variables (e.g. `BRUIN_START_DATE`). User-defined variables are available as the JSON string `BRUIN_VARS`.

## Overriding variables

During `bruin run` you can override variable values with the `--var` flag:

```bash
bruin run --var env=prod --var '{"users": ["alice", "bob"]}'
```

The flag may be used multiple times. If the same key is specified more than once, the last value wins.

## Further reading

- [Jinja Templating](/assets/templating/templating)