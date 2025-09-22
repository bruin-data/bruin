/* @bruin
name: business_hours_asset
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace
  
interval_modifiers:
  start: '{% if start_datetime >= "2025-01-15T09:00:00" and start_datetime <= "2025-01-15T17:00:00" %}-6h{% else %}-12h{% endif %}'
  end: '{% if end_datetime >= "2025-01-15T09:00:00" and end_datetime <= "2025-01-15T17:00:00" %}-1h{% else %}-3h{% endif %}'

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
    'Business Hours Item' AS name,
    TIMESTAMP '2025-01-15 14:30:00' AS created_at
WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'
