/* @bruin

name: dev1.example
type: pg.sql

materialization:
   type: table

@bruin */

SELECT 1 as id, 'Spain' as country, 'Juan' as name
union all
SELECT 2 as id, 'Germany' as country, 'Markus' as name
union all
SELECT 3 as id, 'France' as country, 'Antoine' as name
union all
SELECT 4 as id, 'Poland' as country, 'Franciszek' as name
