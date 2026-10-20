/* @bruin

name: some-sql-task
type: bq.sql

materialization:
  type: table

depends:
  - mode: last
    uri: upstream.a
  - mode: first
    uri: upstream.b
  - mode: any
    uri: upstream.c
  - uri: upstream.d

@bruin */

select *
from foo;
