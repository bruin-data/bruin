# Databricks Assets
## databricks.sql
Runs a materialized Databricks asset or an sql script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.
Note that the asset name must comply with the following schema: `[catalog].[schema].[asset_name]`.


### Examples
Create a table using table materialization
```sql
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
