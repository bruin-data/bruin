/* @bruin
name: people
type: duckdb.sql
materialization:
   type: table

depends:
  - users
@bruin */

select 
    id, 
    name, 
    last_name,
    created_at
from users where country = 'UK';;