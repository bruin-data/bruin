/* @bruin

name: some-sql-task
type: bq.sql

materialization:
  type: table

depends:
  - mode: symbolic
    uri: upstream.a
  - uri: upstream.b
  - uri: upstream.c
  - upstream.d
  - asset: upstream.e
    mode: symbolic
  - upstream.f

@bruin */

select *
from foo;
