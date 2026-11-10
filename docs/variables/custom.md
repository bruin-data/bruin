# Custom Variables

Custom variables are user-defined and specified in `pipeline.yml` using [JSON Schema](https://json-schema.org/). Every variable must provide a `default` value.

## Defining Custom Variables

```yaml
# pipeline.yml
name: analytics-pipeline

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

  users:
    type: array
    items:
      type: string
    default: ["alice", "bob"]
```

## Supported JSON Schema Keywords

Bruin accepts [JSON Schema draft-07](https://json-schema.org/draft-07/json-schema-release-notes.html) keywords in variable definitions. At parse time, it enforces that each variable has a `default` value. `bruin validate` also checks each variable's schema and value independently, reporting problems as warnings without failing validation. Values passed with `--var` or selected with `--variant` are checked in place of the declared defaults. These new checks do not run during `bruin run`, and do not change variable overrides or the schemas and values passed to live jobs. Use `bruin validate --exclude-warnings` to skip warning checks.

For this initial rollout, a variable whose schema contains `$ref` (including local references) receives a warning that schema and value validation were skipped. References are not resolved or fetched, avoiding network/file access and recursive-reference failures. Other variables are still checked.

Variable schema validation uses an embedded Draft 7 meta-schema and blocks external loading at the schema loader itself. It cannot fetch schemas over HTTP or read referenced files.

| `type` value | Description | Example default |
|--------------|-------------|-----------------|
| `string`     | UTF-8 text | `"dev"` |
| `integer`    | Whole numbers | `42` |
| `number`     | Numeric values, including decimals | `3.14` |
| `boolean`    | `true` / `false` flags | `false` |
| `object`     | Maps with nested schemas | `{ "region": "us-east-1" }` |
| `array`      | Lists of values | `["alice", "bob"]` |
| `"null"`     | Explicitly nullable values | `null` |

Additional keywords: `enum`, `const`, `minimum`, `maximum`, `pattern`, `items`, `properties`, `required`.

Use the canonical JSON Schema type `integer` for whole numbers. The warning check treats the legacy `int` spelling as `integer` on a private copy and reports a warning; the declared schema remains unchanged.

Quote `"null"` when using it as a type. The warning check treats an unquoted YAML `null` in the `type` field as the JSON Schema type `"null"` on its private copy and reports a warning. Actual values such as `default: null`, `const: null`, and `enum: [null]` remain null values.

Quote date and time defaults used with `format`, for example `default: "2024-01-01"`. YAML reads an unquoted `2024-01-01` as a timestamp, which is passed to jobs as `2024-01-01T00:00:00Z` and does not match `format: date`.

## Complex Variable Examples

**Array of Objects:**

```yaml
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
```

**Object with Array Properties:**

```yaml
variables:
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

## Using Custom Variables in SQL

Custom variables are accessed via the `var` namespace in Jinja templates:

```bruin-sql
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

## Using Custom Variables in Python

Custom variables are available as a JSON string in the `BRUIN_VARS` environment variable. The schema is also available in `BRUIN_VARS_SCHEMA` for validation and type checking.

```python
"""@bruin
name: analytics.segment_report
@bruin"""

import os
import json

# Parse custom variables
vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))

# Access simple variables
segment = vars.get("target_segment", "enterprise")
horizon = vars.get("forecast_horizon_days", 30)

# Access complex variables (arrays, objects)
users = vars.get("users", [])
cohorts = vars.get("experiment_cohorts", [])

for cohort in cohorts:
    print(f"Cohort: {cohort['name']}, weight: {cohort['weight']}")
    for channel in cohort["channels"]:
        print(f"  - {channel}")

# The schema is also available for validation
schema = json.loads(os.environ.get("BRUIN_VARS_SCHEMA", "{}"))
```

When no variables are defined, both `BRUIN_VARS` and `BRUIN_VARS_SCHEMA` are set to `{}`.
