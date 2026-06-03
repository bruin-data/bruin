/* @bruin
name: bruin_test.ddl_table
type: flight.sql

materialization:
  type: table
  strategy: ddl

columns:
  - name: id
    type: INTEGER
    description: "Primary identifier"
    primary_key: true
  - name: name
    type: VARCHAR
    description: "Name column"
  - name: created_at
    type: TIMESTAMP
    description: "Creation timestamp"
@bruin */
