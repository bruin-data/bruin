{#
  Term configuration rendered from pipeline variables into portable VALUES rows.
  Each macro takes `var` explicitly because macros do not see the asset context.
#}

{% macro sl_slug(value) -%}
{{ value | lower | replace(' ', '-') | replace("'", '') | replace('"', '') | replace('/', '-') | replace('.', '-') }}
{%- endmacro %}

{% macro sl_phrase_row(term_id, term_name, category, phrase, platforms, valid_from, valid_to, active, origin) -%}
({{ sl_str(term_id) }}, {{ sl_str(term_name) }}, {{ sl_str(category) }}, {{ sl_str(phrase | trim) }}, {{ sl_str(phrase | trim | lower) }}, {% if platforms %}{{ sl_str(',' ~ (platforms | join(',') | lower) ~ ',') }}{% else %}''{% endif %}, CAST({{ sl_str(valid_from) }} AS DATE), CAST({{ sl_str(valid_to) }} AS DATE), {{ sl_bool(active) }}, {{ sl_str(origin) }})
{%- endmacro %}

{# One row per (term, phrase). brand_name is required, so the list is never empty. #}
{% macro sl_term_phrase_rows(v) -%}
SELECT * FROM (VALUES
  {{ sl_phrase_row('brand', v.brand_name, 'brand', v.brand_name, [], '1900-01-01', '2999-12-31', true, 'brand_name') }}
  {%- for alias in v.brand_aliases %},
  {{ sl_phrase_row('brand', v.brand_name, 'brand', alias, [], '1900-01-01', '2999-12-31', true, 'brand_aliases') }}
  {%- endfor %}
  {%- for competitor in v.competitors %},
  {{ sl_phrase_row('competitor-' ~ sl_slug(competitor), competitor, 'competitor', competitor, [], '1900-01-01', '2999-12-31', true, 'competitors') }}
  {%- endfor %}
  {%- for term in v.tracked_terms %},
  {{ sl_phrase_row('topic-' ~ sl_slug(term), term, 'topic', term, [], '1900-01-01', '2999-12-31', true, 'tracked_terms') }}
  {%- endfor %}
  {%- for rule in v.term_rules %}{% for phrase in rule.phrases %},
  {{ sl_phrase_row(rule.term_id, rule.phrases[0], rule.category, phrase, rule.platforms | default([]), rule.valid_from | default('1900-01-01'), rule.valid_to | default('2999-12-31'), rule.active | default(true), 'term_rules') }}
  {%- endfor %}{% endfor %}
) AS t(term_id, term_name, category, phrase, phrase_lower, platform_scope, valid_from, valid_to, is_active, term_origin)
{%- endmacro %}

{# Context rules for ambiguous phrases. A sentinel row keeps VALUES non-empty. #}
{% macro sl_term_context_rows(v) -%}
SELECT * FROM (VALUES
  ('__none__', 'require', '')
  {%- for rule in v.term_rules %}
  {%- for phrase in rule.requires_context_any | default([]) %},
  ({{ sl_str(rule.term_id) }}, 'require', {{ sl_str(phrase | trim | lower) }})
  {%- endfor %}
  {%- for phrase in rule.excludes_context_any | default([]) %},
  ({{ sl_str(rule.term_id) }}, 'exclude', {{ sl_str(phrase | trim | lower) }})
  {%- endfor %}
  {%- endfor %}
) AS t(term_id, context_type, phrase_lower)
WHERE term_id <> '__none__'
{%- endmacro %}

{# Intent keyword rules. intent_taxonomy has minItems: 1. #}
{% macro sl_intent_rows(v) -%}
SELECT * FROM (VALUES
  {%- for rule in v.intent_taxonomy %}{% set outer = loop %}
  {%- for keyword in rule.keywords %}
  ({{ sl_str(rule.intent) }}, {{ sl_str(keyword | lower) }}, CAST({{ rule.score }} AS DOUBLE), {{ outer.index }}){% if not (outer.last and loop.last) %},{% endif %}
  {%- endfor %}
  {%- endfor %}
) AS t(intent, keyword_lower, intent_score, taxonomy_rank)
{%- endmacro %}

{# Model identifier recorded with assessments. #}
{% macro sl_model_id(v) -%}
{%- if v.llm_model %}{{ v.llm_model }}{% elif v.llm_provider == 'fixture' %}fixture-model{% endif -%}
{%- endmacro %}

{# The version key of the current assessment configuration. #}
{% macro sl_assessment_version(v) -%}
{{ v.scoring_version }}|{{ v.matcher_version }}|{% if v.llm_enabled %}{{ v.llm_provider }}:{{ sl_model_id(v) }}:{{ v.llm_prompt_version }}{% else %}rules{% endif %}
{%- endmacro %}
