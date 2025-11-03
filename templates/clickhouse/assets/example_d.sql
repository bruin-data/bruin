/* @bruin

type: clickhouse.sql

materialization:
  type: table

depends:
  - example_c

columns:
  - name: id
    type: integer
    primary_key: true
  - name: name
    type: varchar

@bruin */

SELECT
    id,
    name
FROM example_c
WHERE country_name = 'germany'
