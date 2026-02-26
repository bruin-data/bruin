/* @bruin

name: user_model.users
type: bq.sql
materialization:
  type: table
  cluster_by: 
    - user_id

depends:
  - user_model.users_daily

description:
  The users table contains user-level metrics and dimensions.
  The underlying table is clustered by user_id.
  This table is used for reporting and ad-hoc analysis.

columns:
  - name: user_id
    type: STRING
    description: The user ID
    primary_key: true

@bruin */

{%- set cohort_metrics = [
    'session_starts',
    'session_duration',
    'ad_imp_cnt',
    'ad_rev',
    'iap_rev',
    'total_rev',
] -%}
{%- set cohort_days = (range(0,8)|list) + [14,21,28,30,60,90] -%}

WITH
t1 as
(
  SELECT
    user_id,
    min(dt) as install_dt,

    min_by(platform, dt) as install_platform,
    max_by(platform, dt) as recent_platform,
    array_agg(daily_first_app_version ignore nulls order by dt limit 1)[safe_offset(0)] as install_app_version,
    array_agg(daily_last_app_version ignore nulls order by dt desc limit 1)[safe_offset(0)] as recent_app_version,
    array_agg(daily_first_country ignore nulls order by dt limit 1)[safe_offset(0)] as install_country,
    array_agg(daily_last_country ignore nulls order by dt desc limit 1)[safe_offset(0)] as recent_country,
    coalesce(array_agg(daily_first_device_brand ignore nulls order by dt limit 1)[safe_offset(0)], 'unknown') as install_device_brand,
    coalesce(array_agg(daily_last_device_brand ignore nulls order by dt desc limit 1)[safe_offset(0)], 'unknown') as recent_device_brand,
    coalesce(array_agg(daily_first_device_model ignore nulls order by dt limit 1)[safe_offset(0)], 'unknown') as install_device_model,
    coalesce(array_agg(daily_last_device_model ignore nulls order by dt desc limit 1)[safe_offset(0)], 'unknown') as recent_device_model,
    coalesce(array_agg(daily_first_device_language ignore nulls order by dt limit 1)[safe_offset(0)], 'unknown') as install_device_language,
    coalesce(array_agg(daily_last_device_language ignore nulls order by dt desc limit 1)[safe_offset(0)], 'unknown') as recent_device_language,
    array_agg(daily_first_os_version ignore nulls order by dt limit 1)[safe_offset(0)] as install_os_version,
    array_agg(daily_last_os_version ignore nulls order by dt desc limit 1)[safe_offset(0)] as recent_os_version,
    array_agg(daily_first_event ignore nulls order by dt limit 1)[safe_offset(0)] as install_event,
    array_agg(daily_last_event ignore nulls order by dt desc limit 1)[safe_offset(0)] as recent_event,

    sum(events) as events,
    array_agg(dt order by dt) as active_dates,
    array_agg(case when engaged then dt end ignore nulls order by dt) as active_dates_engaged,
    min(min_session_number) as min_session_number,
    max(max_session_number) as max_session_number,
    sum(session_starts) as session_starts,
    sum(session_duration) as session_duration,

    min(min_ts) as min_ts,
    max(max_ts) as max_ts,

    -- REVENUE
    sum(ad_imp_cnt) as ad_imp_cnt,
    sum(ad_inter_imp_cnt) as ad_inter_imp_cnt,
    sum(ad_rv_imp_cnt) as ad_rv_imp_cnt,
    sum(ad_banner_imp_cnt) as ad_banner_imp_cnt,
    sum(ad_rev) as ad_rev,
    sum(ad_inter_rev) as ad_inter_rev,
    sum(ad_rv_rev) as ad_rv_rev,
    sum(ad_banner_rev) as ad_banner_rev,
    sum(iap_cnt) as iap_cnt,
    sum(iap_rev) as iap_rev,
    sum(total_rev) as total_rev,

    -- DAILY METRICS for cohort calculations
    array_agg(struct(
      dt,
      {%- for metric in cohort_metrics %}
      {{ metric }}{{ "," if not loop.last }}
      {%- endfor %}
    ) order by dt) as daily_metrics,

  from `user_model.users_daily`
  group by 1
)
select * except(daily_metrics),
  -- RETENTION (did the user come back on day N?)
  {%- for day_n in (range(1,8)|list) + [14,21,28,30,60,90] %}
  case when install_dt < current_date - {{ day_n }} then if(install_dt + {{ day_n }} in unnest(active_dates), 1, 0) end as ret_d{{day_n}},
  {%- endfor %}

  -- COHORTED METRICS (cumulative by day-N since install)
  {%- for metric in cohort_metrics %}
  {%- for day_n in cohort_days %}
  case when install_dt < current_date - {{ day_n }} then coalesce((select sum({{ metric }}) from unnest(daily_metrics) where dt <= install_dt + {{ day_n }}), 0) end as {{ metric }}_d{{ day_n }},
  {%- endfor %}
  {%- endfor %}
from t1
