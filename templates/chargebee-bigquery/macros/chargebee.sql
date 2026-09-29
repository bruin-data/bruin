{#
  Shared Jinja macros for the chargebee-bigquery pipeline. Files in macros/ are
  auto-discovered, so any asset SQL can call these without importing them.
#}

{# Render a scalar as a safely single-quoted SQL string literal. #}
{% macro sql_literal(value) -%}
'{{ value | replace("'", "''") }}'
{%- endmacro %}

{#
  Render `column IN ('a', 'b', ...)` from a list variable. An empty or cleared
  list collapses to FALSE so the predicate still parses and matches nothing,
  rather than producing an invalid `IN ()`.
#}
{% macro in_string_list(column, values) -%}
{% if values %}{{ column }} IN ({% for value in values %}{{ sql_literal(value) }}{% if not loop.last %}, {% endif %}{% endfor %}){% else %}FALSE{% endif %}
{%- endmacro %}
