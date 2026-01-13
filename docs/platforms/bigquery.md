# Google BigQuery

Google BigQuery is a fully-managed, serverless data platform that enables super-fast SQL queries using the processing power of Google's infrastructure.

Bruin supports BigQuery as a data platform.

## Connection

Google BigQuery requires a Google Cloud Platform connection, which can be added as a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
    connections:
      google_cloud_platform:
        - name: "connection_name"
          project_id: "project-id"
          location: 'your-gcp-region' # see https://cloud.google.com/compute/docs/regions-zones
          
          # Authentication options (choose one):
          
          # Option 1: Use Application Default Credentials (ADC)
          use_application_default_credentials: true
          
          # Option 2: Specify a path to the service account file
          service_account_file: "path/to/file.json"
          
          # Option 3: Specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```

### Authentication Options

Bruin supports three authentication methods for BigQuery connections, listed in order of precedence:

#### 1. Application Default Credentials (ADC) 
When `use_application_default_credentials: true` is set, Bruin will use Google Cloud's [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials#personal).

**Setup:**
```bash
# Authenticate with gcloud, with GOOGLE_APPLICATION_CREDENTIALS environment variable set
gcloud auth login

# Authenticate with gcloud by creating default credential file
gcloud auth application-default login
```

With ADC login there is no need to manage service account files, since it automatically works with gcloud authentication.

**Note:** If you have both ADC enabled and explicit credentials (service account file/JSON), ADC take precedence.

#### 2. Service Account File
Point to a service account JSON file on your filesystem:
```yaml
service_account_file: "/path/to/service-account.json"
```

#### 3. Service Account JSON (Inline)
Embed the service account credentials directly in your configuration:
```yaml
service_account_json: |
  {
    "type": "service_account",
    ...
  }
```
## BigQuery Assets

### `bq.sql`
Runs a materialized BigQuery asset or a BigQuery script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: bq.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a BigQuery script
```bruin-sql
/* @bruin
name: events.install
type: bq.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel,
from first_installs as i
join marketing.attribution as a
    using(user_id)
```


### `bq.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.


Checks if a table exists in BigQuery, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
```
**Parameters**:
- `table`: `project-id.dataset_id.table_id` format, requires all of the identifiers as a full name.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

#### Examples
```yaml
# Google Analytics Events that checks if the recent date table is available
name: analytics_123456789.events
type: bq.sensor.table
parameters:
    table: "your-project-id.analytics_123456789.events_{{ end_date | date_format('%Y%m%d') }}"
```

### `bq.sensor.query`

Checks if a query returns any results in BigQuery, runs by default every 30 seconds until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
```

**Parameters**:
- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `bq.seed`
`bq.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your BigQuery database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the BigQuery database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.


####  Examples: Load csv into a BigQuery database

The examples below show how to load a CSV into a BigQuery database.
```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
