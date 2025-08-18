-- environment: default
-- connection: gcp

with
all_params as
(
    select
        array_concat(
            array(
                select as struct
                    'event_params' as type,
                    ep.key,
                    struct(ep.value.string_value, ep.value.int_value, coalesce(ep.value.float_value, ep.value.double_value) as float_value) as value
                from unnest(event_params) as ep
            ),
            array(
                select as struct
                    'user_properties' as type,
                    up.key,
                    struct(up.value.string_value, up.value.int_value, coalesce(up.value.float_value, up.value.double_value) as float_value) as value
                from unnest(user_properties) as up
                where not starts_with(key, '_ltv')
                    and not starts_with(key, "firebase_exp")
            )
        ) as params
    from `analytics_123456789.events_*` --TODO: fix 123456789 to your analytics ID
    where replace(_TABLE_SUFFIX, 'intraday_', '') between greatest('{{ start_date_nodash }}', '20200101') and least('{{ end_date_nodash }}', '21000101')

),
params_unnested as 
(
    select 
        p.key,
        p.type,
        count(*) as cnt,
        count(distinct to_json_string(p.value)) as cnt_distinct,
        count(p.value.string_value) as cnt_str,
        count(p.value.int_value) as cnt_int64,
        count(p.value.float_value) as cnt_float64,
        array_agg(distinct to_json_string(p.value) limit 3) as example_values,
    from all_params
    join unnest(params) as p
    group by 1,2
)
select
    key,
    type,
    case
        when cnt_str = cnt then 'string'
        when cnt_int64 = cnt then 'int64'
        when cnt_float64 = cnt then 'float64'
        else '?'
    end as data_type,
    * except(key, type)
from params_unnested
order by 1
