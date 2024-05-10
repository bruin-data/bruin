# BigQuery Assets
## bq.sql
Coming soon...

# BigQuery Sensors
## `bq.sensor.table`
Checks if a table exists in BigQuery, runs every 5 minutes until this table is available.
### Parameters
**`table`**: `project-id.dataset_id.table_id` format, requires all of the identifiers as a full name.
### Template
```yaml
name: string
type: string
parameters:
    table: string
```
### Examples
```yaml
# Google Analytics Events that checks if the recent date table is available
name: analytics_123456789.events
type: bq.sensor.table
parameters:
    table: "your-project-id.analytics_123456789.events_{{ end_date_nodash }}"
```

## `bq.sensor.query`
Checks if a query returns any results in BigQuery, runs every 5 minutes until this query returns any results.
### Parameters
**`query`**: Query you expect to return any results
### Template
```yaml
name: string
type: string
parameters:
    table: string
```
### Examples
#### Partitioned upstream table example
Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Streaming upstream table example
Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```
