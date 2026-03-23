# Variables

Variables are dynamic values provided during execution and injected into your asset code. They enable parameterized pipelines—processing data for specific date ranges, customer segments, or configurations without modifying code.

## Overview

There are two types of variables in Bruin:

- **[Built-in Variables](./built-in)**: Automatically provided by Bruin (dates, pipeline info, run IDs)
- **[Custom Variables](./custom)**: User-defined variables specified at the pipeline level

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

Other built-in environment variables include `BRUIN_START_DATETIME`, `BRUIN_START_TIMESTAMP`, `BRUIN_END_DATE`, `BRUIN_END_DATETIME`, `BRUIN_END_TIMESTAMP`, `BRUIN_EXECUTION_DATE`, `BRUIN_EXECUTION_DATETIME`, `BRUIN_EXECUTION_TIMESTAMP`, `BRUIN_RUN_ID`, and `BRUIN_COMMIT_HASH`.

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
- [Macros](/assets/templating/macros#using-macros-with-pipeline-variables) - Using variables with macros
- [Run Command](/commands/run) - Runtime variable overrides
- [Python Assets](/assets/python#environment-variables) - Python environment variable access
- [R Assets](/assets/r) - R environment variable access
