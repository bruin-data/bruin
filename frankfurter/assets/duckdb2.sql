/* @bruin
name: five.asset
type: duckdb.sql
materialization:
    type: table

depends:
    - frankfurter.currencies    
@bruin */

select *
from frankfurter.currencies


