/* @bruin
name: "{{ var.client }}_raw_users_{{ var.region }}"
type: duckdb.sql
materialization:
  type: table
depends:
  - "{{ var.client }}_seed_{{ var.region }}"
@bruin */

SELECT
  user_id,
  email,
  signed_up_at,
  '{{ var.client }}' AS client,
  '{{ var.region }}' AS region
FROM analytics_{{ var.region }}.raw_users
WHERE tenant = '{{ var.client }}';
