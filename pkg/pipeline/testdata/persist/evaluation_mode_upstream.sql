/* @bruin
name: some-sql-task
type: bq.sql

materialization:
  type: table

depends:
  - uri: upstream.a
    mode: last

  - uri: upstream.b
    mode: first

  - uri: upstream.c
    mode: any

  - uri: upstream.d
    mode: full

@bruin */

select *
from foo;
