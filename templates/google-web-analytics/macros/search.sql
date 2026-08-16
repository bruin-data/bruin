{# Search Console metric and query-classification helpers. #}

{# Search Console stores position as a zero-based sum, so the documented
   average is the summed position divided by impressions, plus one. Rows without
   impressions return NULL rather than a misleading position of 1. #}
{% macro average_position(position_sum, impressions) -%}
SAFE_DIVIDE({{ position_sum }}, NULLIF({{ impressions }}, 0)) + 1
{%- endmacro %}

{# Renders a configured pattern as a BigQuery raw string literal.

   Patterns arrive from pipeline variables, and plenty of them need an
   apostrophe: brands such as Levi's and O'Reilly, competitors, path rules. The
   triple-quoted raw literal carries those through unchanged, because only three
   consecutive apostrophes can close it. The pattern is therefore emitted exactly
   as configured, so what you write is what RE2 matches.

   Do not "escape" the apostrophe by rewriting it to a character class. That
   changes the regex wherever an apostrophe already sits inside one: a pattern
   such as [^']+ would become [^[']]+, which stops matching what it used to and
   starts matching a bracket, silently misclassifying rows.

   The one shape this cannot express is a pattern ending in an apostrophe, which
   would meet the closing quotes and form a fourth. No brand, competitor, or path
   pattern needs that. #}
{% macro re_literal(pattern) -%}
r'''{{ pattern }}'''
{%- endmacro %}

{# Renders a configured value as a BigQuery string literal.

   BigQuery does not accept the doubled-quote escape that most SQL dialects use:
   'o''reilly' parses as two adjacent literals and fails. Backslash escaping is
   what BigQuery wants, but the template engine cannot emit a backslash here, so
   the literal is triple-double-quoted instead. That leaves any apostrophe alone,
   including a trailing one, which matters for competitor names like O'Reilly. A
   value containing a double quote is not supported. #}
{% macro sql_literal(value) -%}
"""{{ value }}"""
{%- endmacro %}

{# True when the column matches any pattern in the list. Collapses to FALSE for
   an empty list so an unconfigured pipeline still parses. #}
{% macro matches_any(column, patterns) -%}
{% if patterns %}({% for pattern in patterns %}REGEXP_CONTAINS(LOWER({{ column }}), {{ re_literal(pattern | lower) }}){% if not loop.last %} OR {% endif %}{% endfor %}){% else %}FALSE{% endif %}
{%- endmacro %}

{# Google withholds rare queries to protect user privacy. Those rows still carry
   real impressions and clicks, so they are labelled instead of dropped: daily
   totals stay correct and query-grain reports can exclude them explicitly. #}
{% macro query_brand_type(query_column, is_anonymized_column, brand_pattern) -%}
CASE
  WHEN {{ is_anonymized_column }} OR {{ query_column }} IS NULL THEN 'anonymized'
  WHEN REGEXP_CONTAINS(LOWER({{ query_column }}), {{ re_literal(brand_pattern) }}) THEN 'branded'
  ELSE 'non_branded'
END
{%- endmacro %}

{# Commercial intent, which is the split that decides where SEO effort
   pays back. A comparison or pricing query is close to a buying decision; an
   informational query is someone learning. Neither Search Console nor GA4 knows
   the difference, so ranking pages by clicks treats a glossary post and a
   pricing page as equivalent when their pipeline contribution is nothing alike.

   Competitor queries are tested before branded ones on purpose: "acme vs
   notion" is a comparison query, and its value lies in the comparison rather
   than in the brand mention. This classification is deliberately independent of
   query_brand_type, so the two can be crossed — branded informational and
   branded commercial demand behave very differently. #}
{% macro query_intent_type(query_column, is_anonymized_column, brand_pattern, competitor_names, commercial_pattern) -%}
CASE
  WHEN {{ is_anonymized_column }} OR {{ query_column }} IS NULL THEN 'anonymized'
  WHEN {{ matches_any(query_column, competitor_names) }} THEN 'competitor'
  WHEN REGEXP_CONTAINS(LOWER({{ query_column }}), {{ re_literal(brand_pattern) }}) THEN 'branded'
  WHEN REGEXP_CONTAINS(LOWER({{ query_column }}), {{ re_literal(commercial_pattern) }}) THEN 'commercial'
  ELSE 'informational'
END
{%- endmacro %}

{# Labels which competitor a query mentions, so competitor visibility can be
   tracked per rival rather than as one undifferentiated bucket. #}
{% macro competitor_name(query_column, names) -%}
{% if names %}CASE
{% for name in names %}  WHEN REGEXP_CONTAINS(LOWER({{ query_column }}), {{ re_literal(name | lower) }}) THEN {{ sql_literal(name) }}
{% endfor %}  ELSE NULL
END{% else %}CAST(NULL AS STRING){% endif %}
{%- endmacro %}

{# Economic value of a single GA4 event, from the key_event_values variable.

   When revenue is recognized outside GA4 — in a CRM, a biller, or an offline
   sale — the export carries no purchase amount. Without these
   weights every value metric in the reports reads zero and nothing can be
   ranked by worth. Assign each key event the average value of the outcome it
   creates — a quote request is worth far more than a newsletter signup — and
   the value metrics become comparable across queries and pages. #}
{% macro key_event_value(event_name_column, value_map) -%}
{% if value_map %}CASE {{ event_name_column }}
{% for name, value in value_map.items() %}  WHEN {{ sql_literal(name) }} THEN {{ value }}
{% endfor %}  ELSE 0
END{% else %}0{% endif %}
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
{% if values %}{{ column }} IN ({% for value in values %}{{ sql_literal(value) }}{% if not loop.last %}, {% endif %}{% endfor %}){% else %}FALSE{% endif %}
{%- endmacro %}

{# Window over the GA4 streaming export tables.

   The template reads events_intraday_* only. Selecting through that wildcard
   rather than events_* means _TABLE_SUFFIX is just the eight-digit date, so the
   comparison below is a plain literal range that BigQuery can prune, and the
   daily tables cannot be picked up by accident.

   One thing to know: the current day's table exists and is still filling, so a
   run whose end_date is today reads a fraction of it. The default run window ends
   yesterday, and whole days are replaced on every run, so it corrects itself on
   the next one. #}
{% macro ga4_intraday_window(start_date, end_date, lookback_days) -%}
_TABLE_SUFFIX
      BETWEEN FORMAT_DATE(
        '%Y%m%d',
        DATE_SUB(DATE('{{ start_date }}'), INTERVAL {{ lookback_days }} DAY)
      )
      AND FORMAT_DATE('%Y%m%d', DATE('{{ end_date }}'))
{%- endmacro %}
