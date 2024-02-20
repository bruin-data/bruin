/* @bruin

name: hello_synapse_table_create_replace
type: synapse.sql

materialization:
   type: table
   strategy: create+replace

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

SELECT 1 as id, 'morocco' as country , 'mohammed' as name
union all
SELECT 2 as id, 'japan' as country , 'hiroshi' as name
union all
SELECT 3 as id, 'russia' as country , 'vladimir' as name
union all
SELECT 4 as id, 'italy' as country , 'gianni' as name
union all
SELECT 5 as id, 'united kindgom' as country , 'john' as name