/* @bruin
name: month_end_asset
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace
  
interval_modifiers:
  start: '{% if start_datetime|date_format("%d") == "01" %}-30d{% else %}-2h{% endif %}'
  end: '{% if end_datetime|date_format("%d") == "01" %}-1d{% else %}1h{% endif %}'

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
    'Month End Item' AS name,
    TIMESTAMP '2025-01-15 12:00:00' AS created_at
WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'
