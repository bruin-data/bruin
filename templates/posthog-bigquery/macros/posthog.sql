{# Helpers shared by the staging and reporting models. #}

{# Renders a configured value as a BigQuery string literal.

   BigQuery does not accept the doubled-quote escape most SQL dialects use:
   'it''s' parses as two adjacent literals and fails. Backslash escaping is what
   BigQuery wants, but the template engine cannot emit a backslash here, so the
   literal is triple-double-quoted instead. That leaves any apostrophe alone,
   which matters for event names and plan labels carrying one. A value
   containing a double quote is not supported. #}
{% macro sql_literal(value) -%}
"""{{ value }}"""
{%- endmacro %}

{# Renders a SQL IN predicate from a list variable, collapsing to FALSE when the
   list is empty so a pipeline with the list cleared still parses and runs. #}
{% macro in_string_list(column, values) -%}
{% if values %}{{ column }} IN ({% for value in values %}{{ sql_literal(value) }}{% if not loop.last %}, {% endif %}{% endfor %}){% else %}FALSE{% endif %}
{%- endmacro %}

{# Renders a list variable as a BigQuery array literal, for the cases that need
   the values themselves rather than a predicate — ARRAY_LENGTH for a breadth
   denominator, for instance. An empty list renders as a typed empty array so
   ARRAY_LENGTH still returns 0 instead of failing to infer a type. #}
{% macro string_array(values) -%}
{% if values %}[{% for value in values %}{{ sql_literal(value) }}{% if not loop.last %}, {% endif %}{% endfor %}]{% else %}CAST([] AS ARRAY<STRING>){% endif %}
{%- endmacro %}
