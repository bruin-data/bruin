/* @bruin

name: dashboard.hello_bq
type: bq.sql
description: A simple dashboard table that demonstrates basic BigQuery SQL materialization, containing a single integer column for testing and demonstration purposes
tags:
  - demo
  - dashboard
  - bigquery
  - example
domains:
  - analytics
  - dashboard

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
