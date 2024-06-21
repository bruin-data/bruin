/* @bruin

name: public.hello_pg
type: pg.sql
connection: postgres-default

materialization:
   type: table

columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: negative
        - name: accepted_values
          value: [-1, -6]
  - name: name
    type: string
    description: "Just a name"
    checks:
        - name: pattern
          value: "^Al[A-Za-z]*$"

custom_checks:
  - name: This is a custom check name
    value: 2
    query: select count(*) from public.hello_pg

@bruin */

select -1 as one, 'Alberto' as "name"
union all
select -6 as one , 'Alfredo' as "name"
