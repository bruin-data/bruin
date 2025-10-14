{# Common aggregation patterns for DuckDB #}

{% macro count_by(table, column, order_by='count') -%}
SELECT
    {{ column }},
    COUNT(*) as count
FROM {{ table }}
GROUP BY {{ column }}
ORDER BY {{ order_by }} DESC
{%- endmacro %}

{% macro sum_by(table, group_column, sum_column) -%}
SELECT
    {{ group_column }},
    SUM({{ sum_column }}) as total
FROM {{ table }}
GROUP BY {{ group_column }}
ORDER BY total DESC
{%- endmacro %}

{% macro top_n(table, column, n=10) -%}
SELECT *
FROM {{ table }}
ORDER BY {{ column }} DESC
LIMIT {{ n }}
{%- endmacro %}
