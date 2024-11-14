/* @bruin
name: example2
type: duckdb.sql
materialization:
   type: table

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
#        - name: pattern
#        value: "^[A-Z][a-z]*$"
   @bruin */

select id,name, country from example2;