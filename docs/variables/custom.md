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

Bruin accepts [JSON Schema draft-07](https://json-schema.org/draft-07/json-schema-release-notes.html) keywords in variable definitions. At parse time, it currently enforces that each variable has a `default` value; full schema validation is not yet enforced.

| `type` value | Description | Example default |
|--------------|-------------|-----------------|
| `string`     | UTF-8 text | `"dev"` |
| `integer`    | Whole numbers | `42` |
| `number`     | Numeric values, including decimals | `3.14` |
| `boolean`    | `true` / `false` flags | `false` |
| `object`     | Maps with nested schemas | `{ "region": "us-east-1" }` |
| `array`      | Lists of values | `["alice", "bob"]` |
| `null`       | Explicitly nullable values | `null` |

Additional keywords: `enum`, `const`, `minimum`, `maximum`, `pattern`, `items`, `properties`, `required`.

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
