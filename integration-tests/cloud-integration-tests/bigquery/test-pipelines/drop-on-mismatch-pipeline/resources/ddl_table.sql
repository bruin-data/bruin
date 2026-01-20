/* @bruin

name: cloud_integration_test.ddl_drop_pipeline_ddl
type: bq.sql

materialization:
  type: table
  strategy: ddl
  partition_by: TIMESTAMP_TRUNC(logged_at, DAY)
  cluster_by: 
    - channel, site_name

columns:
  - name: logged_at
    type: TIMESTAMP
    description: "Timestamp of the log"
  - name: company
    type: STRING
    description: "Company name"
  - name: site_name
    type: STRING
    description: "Site name"
  - name: channel
    type: STRING 
    description: "Channel name"

@bruin */
