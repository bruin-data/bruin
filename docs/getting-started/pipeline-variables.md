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

### Supported JSON Schema keywords

Bruin follows the [JSON Schema draft-07](https://json-schema.org/draft-07/json-schema-release-notes.html) specification for variable definitions. The `type` keyword accepts the following values:

| `type` value | Description | Example default |
|--------------|-------------|-----------------|
| `string`     | UTF-8 text, including templated snippets | `"dev"` |
| `integer`    | Whole numbers | `42` |
| `number`     | Numeric values, including decimals | `3.14` |
| `boolean`    | `true` / `false` flags | `false` |
| `object`     | Maps with nested schemas described in `properties` | `{ "region": "us-east-1" }` |
| `array`      | Lists of values described by `items` | `["alice", "bob"]` |
| `null`       | Explicitly nullable values | `null` |

In addition to `type`, you can use other JSON Schema keywords such as `enum`, `const`, `minimum`, `maximum`, `pattern`, `items`, `properties`, and `required` to further restrict or describe the variable. Bruin does not automatically enforce these constraints at runtime today, but declaring them documents your intent and unlocks tooling like schema-aware editors and autocompletion.

```yaml [pipeline.yml]
variables:
  target_segment:
    type: string
    enum: ["self_serve", "enterprise", "partner"]
    default: "enterprise"
  forecast_horizon_days:
    type: integer
    minimum: 7
    maximum: 90
    default: 30
  experiment_cohorts:
    type: array
    minItems: 1
    items:
      type: object
      required: [name, weight, channels]
      properties:
        name:
          type: string
        weight:
          type: number
          minimum: 0
          maximum: 1
        channels:
          type: array
          items:
            type: string
      additionalProperties: false
    default:
      - name: enterprise_baseline
        weight: 0.6
        channels: ["email", "customer_success"]
      - name: partner_campaign
        weight: 0.4
        channels: ["webinar", "email"]
  channel_overrides:
    type: object
    properties:
      email:
        type: array
        items:
          type: string
      sales_enablement:
        type: array
        items:
          type: string
    default:
      email: ["enterprise_newsletter"]
      sales_enablement: ["q1_forecast_brief", "expansion_playbook"]
```

The `experiment_cohorts` example shows an **array of structs** where each cohort defines a name, weight, and the list of channels that should receive the tailored analytics. Meanwhile, `channel_overrides` demonstrates a **struct whose fields are arrays**, letting you capture channel-specific campaign IDs or template names without sprinkling that metadata across multiple assets.

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
name: experimentation
variables:
  experiment_cohorts:
    type: array
    items:
      type: object
      required: [name, weight, channels]
      properties:
        name:
          type: string
        weight:
          type: number
        channels:
          type: array
          items:
            type: string
    default:
      - name: enterprise_baseline
        weight: 0.6
        channels: ["email", "customer_success"]
      - name: partner_campaign
        weight: 0.4
        channels: ["webinar", "email"]
  channel_overrides:
    type: object
    properties:
      email:
        type: array
        items:
          type: string
    default:
      email: ["enterprise_newsletter"]
```
:::

::: code-group
```bruin-sql [asset.sql]
/* @bruin
name: analytics.cohort_plan
type: duckdb.sql
@bruin */

SELECT
  cohort.name,
  cohort.weight,
  channel
FROM (
  SELECT *
  FROM {{ var.experiment_cohorts | tojson }}
) AS cohort,
LATERAL UNNEST(cohort.channels) AS channel
WHERE channel NOT IN (
  SELECT value
  FROM UNNEST({{ var.channel_overrides.email | tojson }}) AS value
);
```
:::

When run with the defaults above, Bruin renders the SQL with the array of structs expanded and the overridden email templates filtered out:
```sql
SELECT
  cohort.name,
  cohort.weight,
  channel
FROM (
  SELECT *
  FROM [{"name":"enterprise_baseline","weight":0.6,"channels":["email","customer_success"]},{"name":"partner_campaign","weight":0.4,"channels":["webinar","email"]}]
) AS cohort,
LATERAL UNNEST(cohort.channels) AS channel
WHERE channel NOT IN (
  SELECT value
  FROM UNNEST(["enterprise_newsletter"]) AS value
);
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

In Python assets these built-ins are exposed as environment variables (e.g. `BRUIN_START_DATE`). User-defined variables are available as the JSON string `BRUIN_VARS`.

## Overriding variables

During `bruin run` you can override variable values with the `--var` flag:

```bash
bruin run --var env=prod --var '{"users": ["alice", "bob"]}'
```

The flag may be used multiple times. If the same key is specified more than once, the last value wins.

## Further reading

- [Jinja Templating](/assets/templating/templating)