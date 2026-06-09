/* @bruin
name: bruin_test.ddl_table
type: sail.sql

materialization:
  type: table
  strategy: ddl

columns:
  - name: id
    type: INT
    description: "Primary identifier"
    primary_key: true
  - name: name
    type: STRING
    description: "Name column"
  - name: created_at
    type: TIMESTAMP
    description: "Creation timestamp"
@bruin */
