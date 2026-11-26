/* @bruin
name: first_run.events
type: duckdb.sql
materialization:
  type: table
  strategy: append
@bruin */

-- The incremental branch reads the table, so the first run has to take the full_refresh branch.
{% if full_refresh %}
SELECT 1 AS event_id
{% else %}
SELECT max(event_id) + 1 AS event_id FROM first_run.events
{% endif %}
