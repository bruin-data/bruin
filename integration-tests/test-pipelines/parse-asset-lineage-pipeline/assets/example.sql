/* @bruin
name: example
type: duckdb.sql
materialization:
   type: table

depends:
  - country
  - people

@bruin */

select 
    a.name, 
    a.last_name,
    a.created_at as updated_at,
    b.country 
from people a 
join country b on a.id = b.id;