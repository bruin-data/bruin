# Built-in Variables

Bruin automatically injects these variables into the Jinja context for templated assets (SQL, YAML `parameters`, and other templated fields).

## Variable Reference

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
| `commit_hash` | The current git commit hash of the pipeline's repository | `"abc1234def5678..."` |
| `schema_prefix` | The schema prefix from the selected environment configuration (empty string if not set) | `"dev_"` |
| `this` | Name of the current asset being executed | `"analytics.daily_summary"` |

## Using Built-in Variables in SQL

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

## Using Built-in Variables in Python

Built-in variables are exposed as environment variables with a `BRUIN_` prefix:

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

### Python Environment Variable Mapping

| Jinja Variable | Python Environment Variable |
|----------------|----------------------------|
| `start_date` | `BRUIN_START_DATE` |
| `start_datetime` | `BRUIN_START_DATETIME` |
| `start_timestamp` | `BRUIN_START_TIMESTAMP` |
| `end_date` | `BRUIN_END_DATE` |
| `end_datetime` | `BRUIN_END_DATETIME` |
| `end_timestamp` | `BRUIN_END_TIMESTAMP` |
| `execution_date` | `BRUIN_EXECUTION_DATE` |
| `execution_datetime` | `BRUIN_EXECUTION_DATETIME` |
| `execution_timestamp` | `BRUIN_EXECUTION_TIMESTAMP` |
| `pipeline` | `BRUIN_PIPELINE` |
| `run_id` | `BRUIN_RUN_ID` |
| `full_refresh` | `BRUIN_FULL_REFRESH` (`"1"` when true, `""` when false) |
| `commit_hash` | `BRUIN_COMMIT_HASH` |
| `schema_prefix` | `BRUIN_SCHEMA_PREFIX` |
| `this` | `BRUIN_THIS` |
| - | `BRUIN_ASSET` (same as `BRUIN_THIS`) |

> [!NOTE]
> The `_nodash` variants (`start_date_nodash`, `end_date_nodash`, `execution_date_nodash`) are only available in the Jinja context for SQL assets. They are not exposed as Python environment variables. If you need a nodash format in Python, derive it from the date string: `start_date.replace("-", "")`.
