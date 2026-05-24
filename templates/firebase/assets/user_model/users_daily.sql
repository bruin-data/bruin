/* @bruin

type: bq.sql
description: |
  Enriched daily user rollup. Joins user_model.stg_users_daily (raw daily aggregates)
  with user_model.users (install-time attributes) so each daily row is tagged with
  install_dt, install_country, days_since_install, nth_active_day, etc.
  Enables day-N analysis without re-aggregating.

materialization:
  type: table
  strategy: create+replace
  partition_by: dt
  cluster_by:
    - user_id

depends:
  - user_model.stg_users_daily
  - user_model.users

@bruin */

select
  ud.*,
  u.install_dt,
  u.install_country,
  u.install_app_version,
  u.install_platform,
  u.install_device_brand,
  date_diff(dt, u.install_dt, day) as days_since_install,
  row_number() over (partition by ud.user_id order by ud.dt) as nth_active_day,
from `user_model.stg_users_daily` as ud
join `user_model.users` as u using(user_id)
