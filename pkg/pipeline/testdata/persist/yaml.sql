/* @bruin
name: some-sql-task
description: some description goes here
type: bq.sql
depends:
  - task1
  - task2
  - task3
  - task4
  - task5
  - task3
parameters:
    param1: first-parameter
    param2: second-parameter
    s3_file_path: s3://bucket/path
connection: conn1
materialization:
    type: table
    partition_by: dt
    cluster_by:
        - event_name
    strategy: delete+insert
    incremental_key: dt

columns:
    - name: col1
      type: string
    - name: col2
      extends: Customer.ID
    - name: col3
      description: xyz
      checks:
        - name: not_null
        - name: accepted_values
          value:
            - val1
            - val2

custom_checks:
  - name: check1
    value: 16
    query: select * from table1

@bruin */

select *
from foo;
