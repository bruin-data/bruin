/* @bruin

name: dev1.mytable2
type: pg.sql

materialization:
   type: table
columns:
   - name: col1
     checks:
        - name: not_null

@bruin */

SELECT * from dev1.mytable
