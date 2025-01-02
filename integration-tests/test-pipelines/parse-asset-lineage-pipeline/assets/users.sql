/* @bruin
name: users
type: duckdb.sql
materialization:
   type: table


columns:
  - name: id
    type: integer
    description: "Just a number"
    primary_key: true
  - name: name
    type: varchar
    description: "Just a name"
  - name: last_name
    type: varchar
    description: "Just a last name"
  - name: country
    type: varchar
    description: "Just a country"
  - name: created_at
    type: timestamp
    description: "Just a timestamp"
@bruin */

select *  from user_data;