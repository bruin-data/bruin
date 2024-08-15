# Synapse Assets
## synapse.sql
Runs a materialized Microsoft Synapse asset or an sql script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.


### Examples
Create a table using table materialization
```sql
/* @bruin
name: events.install
type: synapse.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```
