/* @bruin
name: example_d
type: clickhouse.sql
materialization:
   type: table

depends:
    - example_c

columns:
  - name: id
    type: integer
    primary_key: true
  - name: german_person
    type: varchar
   @bruin */

SELECT id, name from example_c WHERE country_name = 'germany'
