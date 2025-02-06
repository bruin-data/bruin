/* @bruin
name: example_b
type: clickhouse.sql
materialization:
   type: table

columns:
  - name: id
    type: integer
    description: "Just a number"
    primary_key: true
    checks:
        - name: not_null
        - name: positive
        - name: non_negative
  - name: country_name
    type: varchar
    description: "the country"
    checks:
        - name: not_null
 @bruin */

SELECT 1 as id, 'belgium' as country_name
union all
SELECT 2 as id, 'germany' as country_name
union all
SELECT 3 as id, 'denmark' as country_name
