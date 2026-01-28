{# Common data transformation patterns for BigQuery #}

{% macro pivot_sum(table, row_column, pivot_column, value_column, pivot_values) -%}
SELECT
    {{ row_column }},
    {%- for val in pivot_values %}
    SUM(CASE WHEN {{ pivot_column }} = '{{ val }}' THEN {{ value_column }} ELSE 0 END) as {{ val | replace(' ', '_') | replace('-', '_') }}
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
FROM {{ table }}
GROUP BY {{ row_column }}
{%- endmacro %}

{% macro deduplicate(table, partition_column, order_column) -%}
SELECT * EXCEPT(rn) FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY {{ partition_column }}
               ORDER BY {{ order_column }} DESC
           ) as rn
    FROM {{ table }}
)
WHERE rn = 1
{%- endmacro %}

{% macro generate_surrogate_key(columns) -%}
TO_HEX(MD5(CONCAT(
    {%- for col in columns %}
        CAST({{ col }} AS STRING)
        {%- if not loop.last %}, '||', {% endif %}
    {%- endfor %}
))) as surrogate_key
{%- endmacro %}

{% macro safe_divide(numerator, denominator, default=0) -%}
IFNULL(SAFE_DIVIDE({{ numerator }}, {{ denominator }}), {{ default }})
{%- endmacro %}
