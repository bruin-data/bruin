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

SELECT 1 as id, 'John' as name, 'Doe' as last_name, 'USA' as country, '2021-01-01' as created_at
UNION ALL
SELECT 2 as id, 'Jane' as name, 'Smith' as last_name, 'Canada' as country, '2021-01-02' as created_at
UNION ALL
SELECT 3 as id, 'Jim' as name, 'Beam' as last_name, 'UK' as country, '2021-01-03' as created_at
UNION ALL
SELECT 4 as id, 'Jill' as name, 'Johnson' as last_name, 'Australia' as country, '2021-01-04' as created_at