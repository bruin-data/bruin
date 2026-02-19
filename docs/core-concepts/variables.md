# Variables

Variables are dynamic values provided during execution and injected into your asset code. They enable parameterized pipelines—processing data for specific date ranges, customer segments, or configurations without modifying code.

## Overview

There are two types of variables in Bruin:

- **[Built-in Variables](#built-in-variables)**: Automatically provided by Bruin (dates, pipeline info, run IDs)
- **[Custom Variables](#custom-variables)**: User-defined variables specified at the pipeline level

## Referencing Variables in Assets

### SQL Assets

Variables are accessible via the `var` namespace using Jinja templating:

```bruin-sql
/* @bruin
name: analytics.daily_sales
type: duckdb.sql
@bruin */

SELECT *
FROM orders
WHERE order_date >= '{{ start_date }}'
  AND order_date < '{{ end_date }}'
  AND segment = '{{ var.target_segment }}'
```

### Python and R Assets

Built-in variables are exposed as environment variables (for example, `BRUIN_START_DATE`, `BRUIN_PIPELINE`, `BRUIN_FULL_REFRESH`, `BRUIN_ASSET`, `BRUIN_THIS`). Custom variables are available in the `BRUIN_VARS` environment variable as a JSON string:

```python
"""@bruin
name: my_python_asset
@bruin"""

import os
import json

# Built-in variables
start_date = os.environ["BRUIN_START_DATE"]
end_date = os.environ["BRUIN_END_DATE"]
pipeline_name = os.environ["BRUIN_PIPELINE"]
asset_name = os.environ["BRUIN_ASSET"]
full_refresh = os.environ.get("BRUIN_FULL_REFRESH") == "1"

# Custom variables
vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
segment = vars.get("target_segment", "default")
```

Other built-in environment variables include `BRUIN_START_DATETIME`, `BRUIN_START_TIMESTAMP`, `BRUIN_END_DATE`, `BRUIN_END_DATETIME`, `BRUIN_END_TIMESTAMP`, `BRUIN_EXECUTION_DATE`, `BRUIN_EXECUTION_DATETIME`, `BRUIN_EXECUTION_TIMESTAMP`, and `BRUIN_RUN_ID`.

### YAML Assets (Sensor, Ingestr)

Variables can be used in the `parameters` field:

```yaml
name: wait_for_table
type: bq.sensor.query
parameters:
  query: |
    SELECT COUNT(*) > 0 
    FROM `{{ var.table }}`
    WHERE load_time > {{ start_datetime }}
```

> [!NOTE]
> For YAML-style assets, variables can only be used in the value context of the `parameters` field.

## Built-in Variables

Bruin automatically injects these variables into the Jinja context for templated assets (SQL, YAML `parameters`, and other templated fields):

| Variable | Description | Example |
|----------|-------------|---------|
| `start_date` | Start date in YYYY-MM-DD format | `"2023-12-01"` |
| `start_date_nodash` | Start date in YYYYMMDD format | `"20231201"` |
| `start_datetime` | Start date and time in YYYY-MM-DDThh:mm:ss format | `"2023-12-01T15:30:00"` |
| `start_timestamp` | Start timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | `"2023-12-01T15:30:00.000000Z"` |
| `end_date` | End date in YYYY-MM-DD format | `"2023-12-02"` |
| `end_date_nodash` | End date in YYYYMMDD format | `"20231202"` |
| `end_datetime` | End date and time in YYYY-MM-DDThh:mm:ss format | `"2023-12-02T15:30:00"` |
| `end_timestamp` | End timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | `"2023-12-02T15:30:00.000000Z"` |
| `execution_date` | Execution date in YYYY-MM-DD format | `"2023-12-01"` |
| `execution_date_nodash` | Execution date in YYYYMMDD format | `"20231201"` |
| `execution_datetime` | Execution date and time in YYYY-MM-DDThh:mm:ss format | `"2023-12-01T15:30:00"` |
| `execution_timestamp` | Execution timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | `"2023-12-01T15:30:00.000000Z"` |
| `pipeline` | Name of the currently executing pipeline | `"my_pipeline"` |
| `run_id` | Unique identifier for the current pipeline run | `"run_1234567890"` |
| `full_refresh` | Whether the run is a full refresh | `true` |

### Using Built-in Variables in SQL

```bruin-sql
/* @bruin
name: analytics.daily_summary
type: duckdb.sql
@bruin */

SELECT 
    '{{ execution_date }}' as report_date,
    '{{ pipeline }}' as pipeline_name,
    COUNT(*) as total_events
FROM events
WHERE event_date >= '{{ start_date }}'
  AND event_date < '{{ end_date }}'
```

### Using Built-in Variables in Python

```python
"""@bruin
name: my_python_asset
@bruin"""

import os

start_date = os.environ["BRUIN_START_DATE"]
end_date = os.environ["BRUIN_END_DATE"]
pipeline_name = os.environ["BRUIN_PIPELINE"]
run_id = os.environ["BRUIN_RUN_ID"]

print(f"Processing {pipeline_name} for {start_date} to {end_date}")
```

## Custom Variables

Custom variables are user-defined and specified in `pipeline.yml` using [JSON Schema](https://json-schema.org/). Every variable must provide a `default` value.

### Defining Custom Variables

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

### Supported JSON Schema Keywords

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

### Complex Variable Examples

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

### Using Custom Variables in SQL

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

## Overriding Variables at Runtime

Override variable values using the `--var` flag during `bruin run`:

```bash
# Override a simple string variable (use JSON quoting)
bruin run --var env='"prod"'

# Override with JSON for complex types
bruin run --var '{"users": ["alice", "charlie"]}'

# Multiple overrides
bruin run --var target_segment='"self_serve"' --var forecast_horizon_days=60
```

## Related Topics

- [Pipeline Definition](/pipelines/definition#variables) - Variables in pipeline configuration
- [Jinja Templating](/assets/templating/templating) - Template syntax for SQL assets
- [Run Command](/commands/run) - Runtime variable overrides
