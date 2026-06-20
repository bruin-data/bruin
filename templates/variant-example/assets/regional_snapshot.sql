/* @bruin
name: "{{ var.client }}_regional_snapshot_{{ var.region }}"
type: duckdb.sql
enabled: "{{ var.include_regional_snapshot }}"
materialization:
  type: table
depends:
  - "{{ var.client }}_raw_users_{{ var.region }}"
@bruin */

SELECT
  client,
  region,
  COUNT(*) AS active_users
FROM {{ var.client }}_raw_users_{{ var.region }}
GROUP BY 1, 2;
