/* @bruin
name: travellers
type: athena.sql
materialization:
   type: table
   strategy: append
columns:
  - name: id
    type: integer
    description: "identifier of the driver"
    primary_key: true
    checks:
        - name: not_null
        - name: positive
  - name: name
    type: varchar
    description: "Just a name"
    checks:
        - name: not_null
@bruin */

SELECT 1 as id,  'valentino' as name
union all
SELECT 2 as id, 'alonso' as name
union all
SELECT 3 as id, 'senna' as name
union all
SELECT 4 as id, 'lewis' as name
