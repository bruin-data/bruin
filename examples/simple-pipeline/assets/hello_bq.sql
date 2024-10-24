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
    primary_key: true
    checks:
      - name: unique
      - name: not_null
      - name: positive
      - name: accepted_values
        value:
          - 1
          - 2

custom_checks:
  - name: This is a custom check name
    value: 2
    query: select count(*) from dashboard.hello_bq

@bruin */

select 1 as one, col1 from dashboard.hello_sf
--     and {{ start_date }}
--     and {{ end_timestamp }}
--     and {{ end_timestamp | add_days(2) }}
--     and {{ end_timestamp | add_days(2) | date_format('%Y-%m-%d') }}
