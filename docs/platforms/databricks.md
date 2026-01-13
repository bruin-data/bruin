# Databricks

Databricks is a unified data analytics platform that provides a collaborative environment for data scientists, data engineers, and business analysts. It is built on top of Apache Spark, which makes it easy to scale and process big data workloads.

Bruin supports Databricks as a data platform.

## Connection

Bruin supports two authentication methods for Databricks:
- **Personal Access Token (PAT)**: Simple token-based authentication
- **OAuth M2M (Machine-to-Machine)**: Service principal authentication using OAuth 2.0

### Option 1: Personal Access Token (PAT)

```yaml
    connections:
      databricks:
        - name: "connection_name"
          token: "your-databricks-token"
          path: "your-databricks-endpoint-path"
          host: "your-databricks-host"
          port: 443
          catalog: "your-databricks-catalog"
          schema: "your-databricks-schema"
```

#### Step 1: Generate a token

Click on your Databricks username in the top bar, and then select "Settings" from the dropdown menu. Click on the "Developer" tab in the column "Settings" on the left. Next to "Access tokens," click "Manage." Click the "Generate new token" button. Enter Token Details and click "Generate".

#### Step 2: Retrieve HTTP path

Click on "SQL Warehouses" in the bar on the left. Select your warehouse from the list. Click on "Connection details" towards the top of the page. Then copy the HTTP path. It should look something like /sql/1.0/warehouses/3748325bf498i274

#### Step 3: Retrieve host

The host URL is typically visible in the browser's address bar. It should look something like: {databricks-instance}.cloud.databricks.com.

Alternatively, you can find the host URL in the workspace settings.

#### Step 4: Enter port, catalog and schema

Databricks APIs and SQL warehouse endpoints use 443 (HTTPS). So port will usually be 443. The catalog and schema can be found under the section "Catalog" in the bar on the left. 

The Databricks configuration in `.bruin.yml` should like something like this:

```yaml
    connections:
      databricks:
        - name: databricks-default
          token: XXXXXXXXXXXXXXX
          path: /sql/1.0/warehouses/3748325bf498i274
          host: dbc-example-host.cloud.databricks.com
          port: 443
          catalog: default
          schema: example_schema
```

### Option 2: OAuth M2M (Service Principal)

OAuth M2M authentication is recommended for automated workflows and service accounts. It uses a service principal with a client ID and secret instead of a personal access token.

#### Step 1: Create a Service Principal

In your Databricks account console, add a new service principal. Go to the Configuration tab and select the entitlements it should have for your workspace.

#### Step 2: Create an OAuth Secret

On the service principal's details page, open the Secrets tab. Under OAuth secrets, click "Generate secret." Set the secret's lifetime (up to 730 days). Copy the displayed secret and client ID - the secret is only shown once.

#### Step 3: Grant Access to SQL Warehouse

Ensure the service principal has `CAN USE` permission on the SQL warehouse you want to use.

#### Step 4: Configure the Connection

```yaml
    connections:
      databricks:
        - name: databricks-default
          host: dbc-example-host.cloud.databricks.com
          path: /sql/1.0/warehouses/3748325bf498i274
          port: 443
          catalog: default
          schema: example_schema
          client_id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
          client_secret: dosexxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

For more details on OAuth M2M authentication, see the [Databricks documentation](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html).

## Databricks Assets

### `databricks.sql`
Runs a materialized Databricks asset or a Databricks SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: databricks.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a script
```bruin-sql
/* @bruin
name: events.install
type: databricks.sql
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

### `databricks.sensor.query`

Checks if a query returns any results in Databricks, runs by default every 30 seconds until this query returns any results.

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

### `databricks.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.


Checks if a table exists in Databricks, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
```
**Parameters**:
- `table`: `schema_id.table_id`.
- `poke_interval`: The interval between retries in seconds (default 30 seconds). 


#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: databricks.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: databricks.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `databricks.seed`
`databricks.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Databricks database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Databricks database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: databricks.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.


####  Examples: Load csv into a Databricks database

The examples below show how to load a CSV into a Databricks database.
```yaml
name: dashboard.hello
type: databricks.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
