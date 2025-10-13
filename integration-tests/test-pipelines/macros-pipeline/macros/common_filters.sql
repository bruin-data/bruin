{% macro filter_by_date(table_name, date_column, date_value) -%}
SELECT *
FROM {{ table_name }}
WHERE {{ date_column }} = '{{ date_value }}'
{%- endmacro %}

{% macro simple_select(columns) -%}
SELECT {{ columns }}
{%- endmacro %}
