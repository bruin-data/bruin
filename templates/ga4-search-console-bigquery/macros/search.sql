{# Search Console metric and query-classification helpers. #}

{# Search Console stores position as a zero-based sum, so the documented
   average is the summed position divided by impressions, plus one. Rows without
   impressions return NULL rather than a misleading position of 1. #}
{% macro average_position(position_sum, impressions) -%}
SAFE_DIVIDE({{ position_sum }}, NULLIF({{ impressions }}, 0)) + 1
{%- endmacro %}

{# Google withholds rare queries to protect user privacy. Those rows still carry
   real impressions and clicks, so they are labelled instead of dropped: daily
   totals stay correct and query-grain reports can exclude them explicitly.

   The brand pattern is interpolated into a string literal, and plenty of brands
   carry an apostrophe: Levi's, O'Reilly, Dunkin'. A bare apostrophe would close
   the literal and break every asset that classifies a query, so two things
   protect it. The literal is triple-quoted, and each apostrophe in the pattern
   becomes the RE2 character class ['], which still matches one apostrophe but
   cannot end the literal or form a closing triple quote. #}
{% macro query_brand_type(query_column, is_anonymized_column, brand_pattern) -%}
CASE
  WHEN {{ is_anonymized_column }} OR {{ query_column }} IS NULL THEN 'anonymized'
  WHEN REGEXP_CONTAINS(LOWER({{ query_column }}), r'''{{ brand_pattern | replace("'", "[']") }}''') THEN 'branded'
  ELSE 'non_branded'
END
{%- endmacro %}

{% macro query_word_count(query_column) -%}
IF(
  {{ query_column }} IS NULL OR TRIM({{ query_column }}) = '',
  0,
  ARRAY_LENGTH(SPLIT(REGEXP_REPLACE(TRIM(LOWER({{ query_column }})), r'\s+', ' '), ' '))
)
{%- endmacro %}

{# Renders a SQL IN predicate from a list variable, collapsing to FALSE when the
   list is empty so an unconfigured pipeline still parses. #}
{% macro in_string_list(column, values) -%}
{% if values %}{{ column }} IN ({% for value in values %}'{{ value }}'{% if not loop.last %}, {% endif %}{% endfor %}){% else %}FALSE{% endif %}
{%- endmacro %}
