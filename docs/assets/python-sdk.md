---
outline: deep
---

# Python SDK

The official Python SDK for Bruin CLI. Query databases, access connections, and read pipeline context — all with zero boilerplate.

```bruin-python
"""@bruin
name: my_asset
connection: bigquery_conn
@bruin"""

from bruin import query, context

df = query(f"SELECT * FROM users WHERE dt >= '{context.start_date}'")
```

## Installation

Add `bruin-sdk` to the `requirements.txt` that sits next to your Python assets:

```text [requirements.txt]
bruin-sdk
```

For specific database connections, install the corresponding extras:

| Extra | Database |
|:------|:---------|
| `bruin-sdk[bigquery]` | Google BigQuery |
| `bruin-sdk[snowflake]` | Snowflake |
| `bruin-sdk[postgres]` | PostgreSQL |
| `bruin-sdk[redshift]` | Redshift |
| `bruin-sdk[mssql]` | Microsoft SQL Server |
| `bruin-sdk[mysql]` | MySQL |
| `bruin-sdk[duckdb]` | DuckDB |
| `bruin-sdk[sheets]` | Google Sheets |
| `bruin-sdk[all]` | Everything |

## Quick Start

### Before (manual boilerplate)

```bruin-python
"""@bruin
name: my_asset
connection: bigquery_conn
secrets:
    - key: bigquery_conn
@bruin"""

import os
import json
from google.cloud import bigquery

raw = json.loads(os.environ["bigquery_conn"])
sa_info = json.loads(raw["service_account_json"])

client = bigquery.Client.from_service_account_info(
    sa_info, project=raw["project_id"]
)

start = os.environ["BRUIN_START_DATE"]
df = client.query(f"SELECT * FROM users WHERE dt >= '{start}'").to_dataframe()
```

### After (with SDK)

```bruin-python
"""@bruin
name: my_asset
connection: bigquery_conn
@bruin"""

from bruin import query, context

df = query(f"SELECT * FROM users WHERE dt >= '{context.start_date}'")
```

## API Reference

### `context`

A module-level object that provides access to all `BRUIN_*` environment variables as properly typed Python values. Each property reads the env var fresh on every access — no caching, no stale values.

```python
from bruin import context
```

| Property | Type | Env Var | Description |
|:---------|:-----|:--------|:------------|
| `context.start_date` | `date \| None` | `BRUIN_START_DATE` | Pipeline run start date |
| `context.end_date` | `date \| None` | `BRUIN_END_DATE` | Pipeline run end date |
| `context.start_datetime` | `datetime \| None` | `BRUIN_START_DATETIME` | Start date with time |
| `context.end_datetime` | `datetime \| None` | `BRUIN_END_DATETIME` | End date with time |
| `context.execution_date` | `date \| None` | `BRUIN_EXECUTION_DATE` | Execution date |
| `context.run_id` | `str \| None` | `BRUIN_RUN_ID` | Unique run identifier |
| `context.pipeline` | `str \| None` | `BRUIN_PIPELINE` | Pipeline name |
| `context.asset_name` | `str \| None` | `BRUIN_ASSET` | Current asset name |
| `context.connection` | `str \| None` | `BRUIN_CONNECTION` | Asset's default connection |
| `context.is_full_refresh` | `bool` | `BRUIN_FULL_REFRESH` | `True` when `--full-refresh` flag is set |
| `context.vars` | `dict` | `BRUIN_VARS` | Pipeline variables (types preserved from JSON Schema) |

All properties return `None` when the corresponding env var is missing (except `is_full_refresh` which returns `False`, and `vars` which returns `{}`).

::: code-group

```python [context_example.py]
from bruin import context

# Dates
print(context.start_date)       # datetime.date(2024, 6, 1)
print(context.end_date)         # datetime.date(2024, 6, 2)

# Pipeline variables (types preserved from pipeline.yml JSON Schema)
segment = context.vars["segment"]     # str: "enterprise"
horizon = context.vars["horizon"]     # int: 30

# Conditional logic
if context.is_full_refresh:
    df = query("SELECT * FROM users")
else:
    df = query(f"SELECT * FROM users WHERE dt >= '{context.start_date}'")
```

:::

### `query(sql, connection=None)`

Execute SQL and return results.

```python
from bruin import query
```

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `sql` | `str` | *(required)* | SQL statement to execute |
| `connection` | `str \| None` | `None` | Connection name. When `None`, uses the asset's default connection (`BRUIN_CONNECTION`) |

**Returns:** `pandas.DataFrame` for data-returning statements (`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`), `None` for DDL/DML (`CREATE`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, etc.).

::: code-group

```python [query_example.py]
from bruin import query

# Uses the asset's default connection (from `connection:` field)
df = query("SELECT * FROM users")

# Explicit connection name
df = query("SELECT * FROM users", connection="my_bigquery")

# DDL/DML returns None
query("CREATE TABLE temp_users AS SELECT * FROM users")
query("INSERT INTO audit_log VALUES ('ran_asset', NOW())")
```

:::

Every query is automatically annotated with `@bruin.config` metadata for observability and cost tracking.

### `get_connection(name)`

Get a typed connection object with a lazy database client.

```python
from bruin import get_connection
```

| Parameter | Type | Description |
|:----------|:-----|:------------|
| `name` | `str` | Connection name as defined in `.bruin.yml` |

**Returns:** `Connection` or `GCPConnection` depending on the connection type.

::: code-group

```python [connection_example.py]
conn = get_connection("my_bigquery")
conn.name    # "my_bigquery"
conn.type    # "google_cloud_platform"
conn.raw     # dict — the parsed connection JSON
conn.client  # Lazy-initialized database client
```

:::

#### Connection types

| Type | `.client` returns | Install extra |
|:-----|:------------------|:--------------|
| `google_cloud_platform` | `bigquery.Client` | `bruin-sdk[bigquery]` |
| `snowflake` | `snowflake.connector.Connection` | `bruin-sdk[snowflake]` |
| `postgres` | `psycopg2.connection` | `bruin-sdk[postgres]` |
| `redshift` | `psycopg2.connection` | `bruin-sdk[redshift]` |
| `mssql` | `pymssql.Connection` | `bruin-sdk[mssql]` |
| `mysql` | `mysql.connector.Connection` | `bruin-sdk[mysql]` |
| `duckdb` | `duckdb.DuckDBPyConnection` | `bruin-sdk[duckdb]` |
| `generic` | N/A (raises error) | — |

Client creation is **lazy** — the actual database connection is only established when `.client` is first accessed.

#### GCP connections

GCP connections have extra methods since one connection can access multiple Google services:

```python
conn = get_connection("my_gcp")

# BigQuery (most common — also available as .client)
bq_client = conn.bigquery()
df = bq_client.query("SELECT 1").to_dataframe()

# Google Sheets
sheets_client = conn.sheets()  # requires bruin-sdk[sheets]

# Raw credentials for any Google API
creds = conn.credentials  # google.oauth2.Credentials
```

#### Generic connections

Generic connections hold a raw string value (like an API key or webhook URL). They don't have a database client:

```python
conn = get_connection("slack_webhook")
conn.type    # "generic"
conn.raw     # "https://hooks.slack.com/services/T00/B00/xxx"
conn.client  # raises ConnectionTypeError
```

### `Connection.query(sql)`

Connections also have a `.query()` method — an alternative to the top-level `query()`:

```python
conn = get_connection("my_bigquery")

# These are equivalent:
df = conn.query("SELECT * FROM users")
df = query("SELECT * FROM users", connection="my_bigquery")
```

## Asset Setup

When you set the `connection` field in your asset definition, Bruin automatically injects the connection's credentials — no need to list it in `secrets`:

```bruin-python
"""@bruin
name: my_asset
connection: my_bigquery
@bruin"""

from bruin import query

df = query("SELECT * FROM users")
```

If you need additional connections beyond the default, add them to `secrets`:

```bruin-python
"""@bruin
name: my_asset
connection: my_bigquery
secrets:
    - key: my_postgres
@bruin"""

from bruin import query, get_connection

# Default connection (my_bigquery)
df = query("SELECT * FROM users")

# Additional connection via secrets
pg = get_connection("my_postgres")
```

## Exceptions

All SDK exceptions inherit from `BruinError`:

```python
from bruin.exceptions import (
    BruinError,              # Base class
    ConnectionNotFoundError, # Connection name not found or env var missing
    ConnectionParseError,    # Invalid JSON in connection env var
    ConnectionTypeError,     # Unsupported or generic connection type
    QueryError,              # SQL execution failed
)
```

Missing optional dependencies give clear install instructions:

```python
conn = get_connection("my_snowflake")
conn.client
# ImportError: Install bruin-sdk[snowflake] to use Snowflake connections:
#   pip install 'bruin-sdk[snowflake]'
```

## Examples

### Incremental load with date filtering

```bruin-python
"""@bruin
name: analytics.daily_events
connection: my_bigquery
@bruin"""

from bruin import query, context

if context.is_full_refresh:
    df = query("SELECT * FROM raw.events")
else:
    df = query(f"""
        SELECT * FROM raw.events
        WHERE event_date BETWEEN '{context.start_date}' AND '{context.end_date}'
    """)

print(f"Loaded {len(df)} events")
```

### Cross-database ETL

```bruin-python
"""@bruin
name: sync.postgres_to_bigquery
secrets:
    - key: my_postgres
    - key: my_bigquery
@bruin"""

from bruin import query, get_connection

# Read from Postgres
df = query("SELECT * FROM users WHERE active = true", connection="my_postgres")

# Write to BigQuery
bq = get_connection("my_bigquery")
df.to_gbq(
    "staging.active_users",
    project_id=bq.raw["project_id"],
    credentials=bq.credentials,
    if_exists="replace",
)
```

### Using pipeline variables

::: code-group

```yaml [pipeline.yml]
name: marketing
variables:
  segment:
    type: string
    default: "enterprise"
  lookback_days:
    type: integer
    default: 30
```

```bruin-python [segment_report.py]
"""@bruin
name: marketing.segment_report
connection: my_snowflake
@bruin"""

from bruin import query, context

segment = context.vars["segment"]
lookback = context.vars["lookback_days"]

df = query(f"""
    SELECT * FROM customers
    WHERE segment = '{segment}'
    AND created_at >= DATEADD(day, -{lookback}, CURRENT_DATE())
""")

print(f"Found {len(df)} {segment} customers in last {lookback} days")
```

:::

### DDL operations

```bruin-python
"""@bruin
name: setup.create_tables
connection: my_postgres
@bruin"""

from bruin import query

# DDL returns None
query("CREATE TABLE IF NOT EXISTS audit_log (event TEXT, ts TIMESTAMP)")
query("INSERT INTO audit_log VALUES ('setup_complete', NOW())")

# SELECT returns DataFrame
df = query("SELECT COUNT(*) as cnt FROM audit_log")
print(f"Audit log has {df['cnt'][0]} entries")
```
