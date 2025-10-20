/* @bruin
name: public.target_table
type: pg.sql

materialization:
  type: table
  strategy: merge

columns:
  - name: pk
    type: INTEGER
    primary_key: true
  - name: col_a
    type: INTEGER
    merge_sql: GREATEST(target.col_a, source.col_a)
  - name: col_b
    type: INTEGER
    merge_sql: target.col_b + source.col_b
  - name: col_c
    type: VARCHAR
    update_on_merge: true
  - name: col_d
    type: VARCHAR

@bruin */

-- New data that will merge with existing target_table
SELECT 1 AS pk, 15 AS col_a, 50 AS col_b, 'updated_a' AS col_c, 'default' AS col_d
UNION ALL
SELECT 2 AS pk, 5 AS col_a, 150 AS col_b, 'updated_b' AS col_c, 'default' AS col_d
UNION ALL
SELECT 3 AS pk, 30 AS col_a, 300 AS col_b, 'new_c' AS col_c, 'default' AS col_d