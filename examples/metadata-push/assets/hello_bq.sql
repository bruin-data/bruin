/* @bruin

name: dashboard.hello_bq
type: bq.sql
materialization:
    type: table

depends:
   - basic

custom_checks:
  - name: This is a custom check name
    value: 2
    query: select count(*) from dashboard.hello_bq


@bruin */

select 1 as one
union all
select 2 as one
--     and {{ start_date }}
--     and {{ end_timestamp }}
--     and {{ end_timestamp | add_days(2) }}
--     and {{ end_timestamp | add_days(2) | date_format('%Y-%m-%d') }}
