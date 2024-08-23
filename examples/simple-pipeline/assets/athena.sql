/* @bruin
name: travellers
type: athena.sql
materialization:
   type: table
   # strategy: append
   strategy: merge
   # incremental_key: id
columns:
  - name: id
    type: integer
    description: "Just a number"
    primary_key: true
    checks:
        - name: not_null
        - name: positive
        - name: non_negative
  - name: country
    type: varchar
    description: "the country"
    primary_key: true
    checks:
        - name: not_null
  - name: name
    type: varchar
    update_on_merge: true
    description: "Just a name"
    checks:
        - name: unique
        - name: not_null
@bruin */

SELECT 1 as id, 'spain' as country , 'juan1' as name
union all
SELECT 2 as id, 'germany' as country , 'frank' as name
union all
SELECT 3 as id, 'germany' as country , 'franz3' as name
union all
SELECT 4 as id, 'france' as country , 'petit' as name
union all
SELECT 5 as id, 'poland' as country , 'polski' as name
union all
SELECT 6 as id, 'russia' as country , 'ivan' as name