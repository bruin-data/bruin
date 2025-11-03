/* @bruin

type: bq.sql
description: The users table contains user-level metrics and dimensions. The underlying table is clustered by user_id. This table is used for reporting and ad-hoc analysis.

materialization:
  type: table
  cluster_by:
    - user_id

depends:
  - user_model.users_daily

columns:
  - name: user_id
    type: STRING
    description: The user ID
    primary_key: true

@bruin */

WITH
t1 AS (
    SELECT
        user_id,
        min(dt) AS install_dt,

        min_by(platform, dt) AS platform,
        max_by(platform, dt) AS last_platform,
        array_agg(first_app_version IGNORE NULLS ORDER BY dt limit 1)[
            safe_offset(0)
        ] AS first_app_version,
        array_agg(last_app_version IGNORE NULLS ORDER BY dt DESC limit 1)[
            safe_offset(0)
        ] AS last_app_version,
        array_agg(first_country IGNORE NULLS ORDER BY dt limit 1)[
            safe_offset(0)
        ] AS first_country,
        array_agg(last_country IGNORE NULLS ORDER BY dt DESC limit 1)[
            safe_offset(0)
        ] AS last_country,
        coalesce(
            array_agg(first_device_brand IGNORE NULLS ORDER BY dt limit 1)[
                safe_offset(0)
            ],
            'unknown'
        ) AS first_device_brand,
        coalesce(
            array_agg(last_device_brand IGNORE NULLS ORDER BY dt DESC limit 1)[
                safe_offset(0)
            ],
            'unknown'
        ) AS last_device_brand,
        coalesce(
            array_agg(first_device_model IGNORE NULLS ORDER BY dt limit 1)[
                safe_offset(0)
            ],
            'unknown'
        ) AS first_device_model,
        coalesce(
            array_agg(last_device_model IGNORE NULLS ORDER BY dt DESC limit 1)[
                safe_offset(0)
            ],
            'unknown'
        ) AS last_device_model,
        coalesce(
            array_agg(first_device_language IGNORE NULLS ORDER BY dt limit 1)[
                safe_offset(0)
            ],
            'unknown'
        ) AS first_device_language,
        coalesce(
            array_agg(
                last_device_language IGNORE NULLS ORDER BY dt DESC limit 1
            )[safe_offset(0)],
            'unknown'
        ) AS last_device_language,
        array_agg(first_os_version IGNORE NULLS ORDER BY dt limit 1)[
            safe_offset(0)
        ] AS first_os_version,
        array_agg(last_os_version IGNORE NULLS ORDER BY dt DESC limit 1)[
            safe_offset(0)
        ] AS last_os_version,
        array_agg(first_event IGNORE NULLS ORDER BY dt limit 1)[
            safe_offset(0)
        ] AS first_event,
        array_agg(last_event IGNORE NULLS ORDER BY dt DESC limit 1)[
            safe_offset(0)
        ] AS last_event,

        sum(events) AS events,
        array_agg(dt ORDER BY dt) AS active_dates,
        array_agg(CASE WHEN engaged THEN dt END IGNORE NULLS ORDER BY dt)
            AS active_dates_engaged,
        min(min_session_number) AS min_session_number,
        max(max_session_number) AS max_session_number,
        sum(session_starts) AS session_starts,
        sum(session_duration) AS session_duration,

        min(min_ts) AS min_ts,
        max(max_ts) AS max_ts,

        -- REVENUE
        sum(ad_imp_cnt) AS ad_imp_cnt,
        sum(ad_inter_imp_cnt) AS ad_inter_imp_cnt,
        sum(ad_rv_imp_cnt) AS ad_rv_imp_cnt,
        sum(ad_banner_imp_cnt) AS ad_banner_imp_cnt,
        sum(ad_rev) AS ad_rev,
        sum(ad_inter_rev) AS ad_inter_rev,
        sum(ad_rv_rev) AS ad_rv_rev,
        sum(ad_banner_rev) AS ad_banner_rev,
        sum(iap_cnt) AS iap_cnt,
        sum(iap_rev) AS iap_rev,
        sum(total_rev) AS total_rev

    FROM `user_model.users_daily`
    GROUP BY 1
)

SELECT
    *,
    -- TODO: Add more days if needed
{%- for day_n in (range(1,8)|list) + [14,21,28,30,60,90] %}
    CASE
        WHEN
            install_dt < current_date - {{ day_n }}
            THEN if(install_dt + {{ day_n }} IN unnest(active_dates), 1, 0)
    END AS ret_d{{ day_n }}{%- endfor %}
FROM t1
