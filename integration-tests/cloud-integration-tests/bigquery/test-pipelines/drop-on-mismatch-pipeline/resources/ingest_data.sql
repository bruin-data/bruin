/* @bruin

name: cloud_integration_test.ddl_drop_pipeline_ddl
type: bq.sql

materialization:
  type: table
  strategy: append
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

SELECT TIMESTAMP '2024-01-01 00:00:05' AS logged_at, 'Company A' AS company, 'Site A' AS site_name, 'Channel A' AS channel
UNION ALL
SELECT TIMESTAMP '2024-01-01 00:00:05' AS logged_at, 'Company B' AS company, 'Site B' AS site_name, 'Channel B' AS channel
UNION ALL
SELECT TIMESTAMP '2024-01-01 00:00:05' AS logged_at, 'Company C' AS company, 'Site C' AS site_name, 'Channel C' AS channel
UNION ALL
SELECT TIMESTAMP '2024-01-01 00:00:05' AS logged_at, 'Company D' AS company, 'Site D' AS site_name, 'Channel D' AS channel
