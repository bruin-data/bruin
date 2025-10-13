{# Common data transformation patterns for DuckDB #}

{% macro pivot_sum(table, row_column, pivot_column, value_column) -%}
PIVOT {{ table }}
ON {{ pivot_column }}
USING SUM({{ value_column }})
GROUP BY {{ row_column }}
{%- endmacro %}

{% macro deduplicate(table, partition_column, order_column) -%}
SELECT * FROM (
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
MD5(CONCAT_WS('||',
    {%- for col in columns %}
        CAST({{ col }} AS VARCHAR)
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
)) as surrogate_key
{%- endmacro %}

{% macro safe_divide(numerator, denominator, default=0) -%}
CASE
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
    THEN {{ default }}
    ELSE {{ numerator }}::DOUBLE / {{ denominator }}::DOUBLE
END
{%- endmacro %}
