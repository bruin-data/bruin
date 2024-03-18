/* @bruin

name: user_model.cohorts
type: bq.sql
materialization:
    type: table
    partition_by: install_dt
    cluster_by: 
        - platform

depends: 
    - user_model.users
    - user_model.users_daily

@bruin */

{%- set dimensions = [
    'platform',
    'first_country',
    'install_dt',
    'first_app_version',
    'first_device_brand',
    'first_os_version',
] -%}

{% set metrics = [
    'sessions',
    'session_duration',
    'ad_inter_imp_cnt',
    'ad_rv_imp_cnt',
    'ad_imp_cnt',
    'ad_rev',
    'iap_rev',
    'total_rev'
] -%}

WITH
player_details AS
(
  SELECT 
    user_id,
    {% for dimension in dimensions -%}
    {{ dimension }},
    {% endfor %}
    date_diff(current_date-1, install_dt, day) as days_since_install,
  FROM user_model.users
  WHERE true
),
player_daily_stats AS
(
  SELECT 
    user_id,
    dt,
    engaged,
    {% for metric in metrics -%}
    {{ metric }},
    {% endfor %}
    if(iap_rev > 0, 1, 0) as paying_user
  FROM user_model.users_daily
)
SELECT 
    {% for dimension in dimensions -%}
    {{ dimension }},
    {% endfor %}
    days as nth_day,
    days = p.days_since_install as recent,
    p.install_dt + days as dt,

    count(distinct p.user_id) as cohort_size,
    countif(p.install_dt = s.dt and days = 0) as installs,
    countif(s.engaged and s.dt = p.install_dt + days) as retained_users,
    sum(if(s.dt = p.install_dt + days, s.paying_user, 0)) as paying_user,
    count(distinct if(s.dt = p.install_dt + days and s.paying_user = 1, p.user_id, null)) as converted_user,

    {% for metric in metrics -%}
    sum(s.{{ metric }}) as {{ metric }}_agg,
    sum(if(s.dt = p.install_dt + days, s.{{ metric }}, 0)) as {{ metric }},
    {% endfor %}
FROM player_details p 
CROSS JOIN unnest(generate_array(0,days_since_install)) as days
LEFT JOIN player_daily_stats s
    on p.user_id = s.user_id
    and s.dt <= p.install_dt + days
GROUP BY 1,2 
{%- for dim in dimensions -%}
  ,{{ loop.index + 2 }}
{%- endfor %}

;
