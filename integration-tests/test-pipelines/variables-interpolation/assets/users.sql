/* @bruin
name: public.users
type: duckdb.sql
connection: duckdb-variables
materialization:
  type: table
@bruin */
{% for user in var.users -%}
  SELECT '{{ user }}' as name {% if not loop.last %} UNION ALL {% endif -%}
{%- endfor -%};