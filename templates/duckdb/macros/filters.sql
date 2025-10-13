{# Common filtering patterns for DuckDB #}

{% macro date_range(table, date_column, start_date, end_date) -%}
SELECT *
FROM {{ table }}
WHERE {{ date_column }} >= '{{ start_date }}'
  AND {{ date_column }} < '{{ end_date }}'
{%- endmacro %}

{% macro recent_records(table, date_column, days=7) -%}
SELECT *
FROM {{ table }}
WHERE {{ date_column }} >= CURRENT_DATE - INTERVAL '{{ days }} days'
{%- endmacro %}

{% macro filter_null(table, columns) -%}
SELECT *
FROM {{ table }}
WHERE {% for col in columns %}
    {{- col }} IS NOT NULL
    {%- if not loop.last %} AND {% endif %}
{%- endfor %}
{%- endmacro %}

{% macro in_list(table, column, values) -%}
SELECT *
FROM {{ table }}
WHERE {{ column }} IN (
    {%- for val in values %}
        '{{ val }}'
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
)
{%- endmacro %}
