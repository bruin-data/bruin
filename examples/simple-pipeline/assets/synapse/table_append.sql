/* @bruin

name: hello_synapse_table_append
type: synapse.sql

materialization:
   type: table
   strategy: append

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

SELECT RAND()*(1000-1)+1 as id, 'morocco' as country , 'mohammed' as name
