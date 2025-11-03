/* @bruin

type: clickhouse.sql

materialization:
  type: table

depends:
  - example_a
  - example_b

columns:
  - name: id
    type: integer
    description: Just a number
    primary_key: true
    checks:
      - name: not_null
      - name: positive
      - name: non_negative
  - name: country
    type: varchar
    description: the country
    checks:
      - name: not_null
  - name: name
    type: varchar
    description: Just a name
    update_on_merge: true

@bruin */

SELECT *
FROM example_a AS a
INNER JOIN example_b AS b ON a.country = b.country_name
