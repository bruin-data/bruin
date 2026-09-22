{% macro stable_alert_key(content_id, destination, payload_version) -%}
sha256({{ content_id }} || ':' || {{ destination }} || ':' || {{ payload_version }})
{%- endmacro %}

{% macro safe_score_component(expression) -%}
LEAST(GREATEST(COALESCE({{ expression }}, 0), 0), 1)
{%- endmacro %}
