/* @bruin

name: hello_synapse_table_merge
type: synapse.sql
upstream:
  - table_create_replace

materialization:
   type: table
   strategy: merge

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
-- hiroshi from "table_create_replace.sql" should be replaced by kentaro
SELECT 2 as id, 'japan' as country , 'kentaro' as name
union all
-- vladimir from "table_create_replace.sql" should be replaced by ilya
SELECT 3 as id, 'russia' as country , 'ilya' as name
union all
SELECT 3 as id, 'japan' as country , 'satoshi' as name
union all
SELECT 4 as id, 'italy' as country , 'gianni' as name
union all
SELECT 5 as id, 'united kindgom' as country , 'john' as name