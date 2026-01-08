/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
  type: table

depends:
  - hello_python

columns:
  - name: one
    type: integer
    description: Just a number
    checks:
      - name: unique
      - name: not_null
      - name: positive
      - name: accepted_values
        value:
          - 1
          - 2

@bruin */

select 1 as one
union all
select 2 as one
