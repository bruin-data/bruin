/* @bruin
name: user_info
type: bq.sql
@bruin */

SELECT * FROM {{ var.env }}.users WHERE 
{% for user in var.users -%}
  user_id = '{{ user }}'{% if not loop.last %} OR {% endif -%}
{%- endfor -%};