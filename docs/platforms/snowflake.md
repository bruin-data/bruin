# Snowflake Assets

Bruin supports Snowflake as a data warehouse. 

## Connection
In order to have set up a Snowflake connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

In order to have set up a Snowflake connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

There's 2 different ways to fill it in

```yaml
    connections:
      snowflake:
        - name: "connection_name"
          username: "sfuser"
          password: "XXXXXXXXXX"
          account: "AAAAAAA-AA00000"
          database: "dev"
          schema: "schema_name" # optional
          warehouse: "warehouse_name" # optional
          role: "data_analyst" # optional
          region: "eu-west1" # optional
```

Where account is the identifier that you can copy here:

![Snowflake Account](/snowflake.png)



## Snowflake Assets

### `sf.sql`
Runs a materialized Snowflake asset or a Snowflake script. For detailed parameters, you can check [Definition Schema](definition-schema.md) page.


#### Example: Create a table using table materialization
```sql
/* @bruin
name: events.install
type: sf.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Snowflake script
```sql
/* @bruin
name: events.install
type: sf.sql
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

### `sf.sensor.query`

> [!DANGER]
> BigQuery sensors are not supported yet in Bruin CLI, and they only work on Bruin Cloud.

Checks if a query returns any results in Snowflake, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters:**
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table
Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table
Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```
