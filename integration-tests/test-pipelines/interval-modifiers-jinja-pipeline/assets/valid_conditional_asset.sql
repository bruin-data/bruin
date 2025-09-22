/* @bruin
name: valid_conditional_asset
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace
  
interval_modifiers:
  start: '{% if start_timestamp|date_format("%H") == "00" %}-2h{% else %}0{% endif %}'
  end: '{% if end_timestamp|date_format("%H") == "23" %}-1h{% else %}1h{% endif %}'

columns:
  - name: id
    type: INTEGER
    description: "Unique identifier"
    primary_key: true
  - name: name
    type: VARCHAR
    description: "Name of the item"
  - name: created_at
    type: TIMESTAMP
    description: "When the item was created"
@bruin */

SELECT 
    1 AS id,
    'Test Item' AS name,
    TIMESTAMP '2025-01-15 12:00:00' AS created_at
WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'
