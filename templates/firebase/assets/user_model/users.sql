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

WITH
t1 as 
( 
  SELECT
    user_id,
    min(dt) as install_dt,

    min_by(platform, dt) as platform,
    max_by(platform, dt) as last_platform,
    array_agg(first_app_version ignore nulls order by dt limit 1)[safe_offset(0)] as first_app_version,
    array_agg(last_app_version ignore nulls order by dt desc limit 1)[safe_offset(0)] as last_app_version,
    array_agg(first_country ignore nulls order by dt limit 1)[safe_offset(0)] as first_country,
    array_agg(last_country ignore nulls order by dt desc limit 1)[safe_offset(0)] as last_country,
    coalesce(array_agg(first_device_brand ignore nulls order by dt limit 1)[safe_offset(0)], 'unknown') as first_device_brand,
    coalesce(array_agg(last_device_brand ignore nulls order by dt desc limit 1)[safe_offset(0)], 'unknown') as last_device_brand,
    coalesce(array_agg(first_device_model ignore nulls order by dt limit 1)[safe_offset(0)], 'unknown') as first_device_model,
    coalesce(array_agg(last_device_model ignore nulls order by dt desc limit 1)[safe_offset(0)], 'unknown') as last_device_model,
    coalesce(array_agg(first_device_language ignore nulls order by dt limit 1)[safe_offset(0)], 'unknown') as first_device_language,
    coalesce(array_agg(last_device_language ignore nulls order by dt desc limit 1)[safe_offset(0)], 'unknown') as last_device_language,
    array_agg(first_os_version ignore nulls order by dt limit 1)[safe_offset(0)] as first_os_version,
    array_agg(last_os_version ignore nulls order by dt desc limit 1)[safe_offset(0)] as last_os_version,
    array_agg(first_event ignore nulls order by dt limit 1)[safe_offset(0)] as first_event,
    array_agg(last_event ignore nulls order by dt desc limit 1)[safe_offset(0)] as last_event,
    
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
 
  from `user_model.users_daily`
  group by 1
)
select *,
  {%- for day_n in (range(1,8)|list) + [14,21,28,30,60,90] %}
  case when install_dt < current_date - {{ day_n }} then if(install_dt + {{ day_n }} in unnest(active_dates), 1, 0) end as ret_d{{day_n}},
  {%- endfor %}
from t1
