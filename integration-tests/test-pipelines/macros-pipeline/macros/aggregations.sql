{% macro count_by_column(table_name, group_column) -%}
SELECT {{ group_column }}, COUNT(*) as count
FROM {{ table_name }}
GROUP BY {{ group_column }}
ORDER BY count DESC
{%- endmacro %}
