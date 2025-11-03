/* @bruin

name: some-sql-task
type: bq.sql
description: some description goes here
connection: conn2

depends:
  - task1
  - task2
  - task3
  - task4
  - task5

parameters:
  param1: first-parameter
  param2: second-parameter

@bruin */

select *
from foo;
