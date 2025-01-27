/* @bruin
name: example_c
type: clickhouse.sql
materialization:
   type: table
depends:
    - example_a
    - example_b

columns:
  - name: id
    type: integer
    description: "Just a number"
    primary_key: true
    checks:
        - name: not_null
        - name: positive
        - name: non_negative
  - name: country
    type: varchar
    description: "the country"
    checks:
        - name: not_null
  - name: name
    type: varchar
    update_on_merge: true
    description: "Just a name"
    checks:
@bruin */

SELECT * FROM example_a a JOIN example_b b ON a.country = b.country_name