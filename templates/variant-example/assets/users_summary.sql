/* @bruin
name: "{{ var.client }}_users_summary_{{ var.region }}"
type: duckdb.sql
materialization:
  type: table
depends:
  - "{{ var.client }}_raw_users_{{ var.region }}"
@bruin */

SELECT
  client,
  region,
  CAST(signed_up_at AS DATE) AS signup_date,
  COUNT(*) AS users_signed_up
FROM {{ var.client }}_raw_users_{{ var.region }}
WHERE signed_up_at >= CURRENT_DATE - INTERVAL '{{ var.forecast_days }}' DAY
GROUP BY 1, 2, 3;
