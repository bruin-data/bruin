/* @bruin
name: bruin_test.ddl_table
type: doris.sql

materialization:
  type: table
  strategy: ddl

columns:
  - name: id
    type: INT
    description: "Primary identifier"
  - name: name
    type: STRING
    description: "Name column"
  - name: created_at
    type: DATETIME
    description: "Creation timestamp"
@bruin */
