/* @bruin
name: raw.tickets
type: duckdb.sql
materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 AS ticket_id, 'I was charged twice for my subscription.' AS body
UNION ALL
SELECT 7 AS ticket_id, 'The application crashes when I open settings.' AS body
