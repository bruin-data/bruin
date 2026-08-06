/* @bruin
name: local.bruin_test.ddl_table
type: spark.sql

materialization:
  type: table
  strategy: ddl
  partition_by: days(created_at)
  cluster_by:
    - name
    - id

columns:
  - name: id
    type: INT
    description: "Primary identifier"
  - name: name
    type: STRING
    description: "Name column"
  - name: created_at
    type: TIMESTAMP
    description: "Creation timestamp"
@bruin */
