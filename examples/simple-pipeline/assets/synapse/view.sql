/* @bruin

name: synapse_view
type: synapse.sql

materialization:
   type: view

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

SELECT 1 as id, 'spain' as country , 'juan' as name
union all
SELECT 2 as id, 'germany' as country , 'markus' as name
union all
SELECT 3 as id, 'germany' as country , 'franz' as name
union all
SELECT 4 as id, 'france' as country , 'antoine' as name
union all
SELECT 5 as id, 'poland' as country , 'maciej' as name