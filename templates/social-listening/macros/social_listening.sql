{#
  Dialect seam for the social-listening template.

  Every warehouse-specific expression used by the SQL assets goes through one of
  these macros. They are written for DuckDB (and so MotherDuck). To run the
  template on Postgres, BigQuery, Snowflake or ClickHouse, reimplement the
  macros in this file for that dialect and change `default.type` in
  pipeline.yml; the assets themselves only use ANSI SQL plus these macros.
  docs/index.md#porting-to-another-warehouse lists the equivalent functions.
#}

{# A SQL string literal from a Jinja string. #}
{% macro sl_str(value) -%}
'{{ (value ~ '') | replace("'", "''") }}'
{%- endmacro %}

{# An IN-list of string literals; an empty list matches nothing. #}
{% macro sl_in_list(values) -%}
{% if values %}({% for v in values %}{{ sl_str(v | lower) }}{% if not loop.last %}, {% endif %}{% endfor %}){% else %}(SELECT NULL WHERE FALSE){% endif %}
{%- endmacro %}

{# TRUE/FALSE literal from a Jinja boolean. #}
{% macro sl_bool(value) -%}
{{ 'TRUE' if value else 'FALSE' }}
{%- endmacro %}

{# Stable text hash used for content IDs, alert keys and draft IDs. #}
{% macro sl_hash(expr) -%}
md5({{ expr }})
{%- endmacro %}

{# Extract a scalar from a JSON string column as text / number. #}
{% macro sl_json_str(col, path) -%}
json_extract_string({{ col }}, '{{ path }}')
{%- endmacro %}

{% macro sl_json_num(col, path) -%}
TRY_CAST(json_extract_string({{ col }}, '{{ path }}') AS DOUBLE)
{%- endmacro %}

{# Build a JSON object string. `pairs` is a flat list: ['key', 'sql expr', ...]. #}
{% macro sl_json_object(pairs) -%}
CAST(json_object({% for item in pairs %}{% if loop.index % 2 == 1 %}{{ sl_str(item) }}{% else %}{{ item }}{% endif %}{% if not loop.last %}, {% endif %}{% endfor %}) AS VARCHAR)
{%- endmacro %}

{# JSON object for nesting inside sl_json_object (not cast to text). #}
{% macro sl_json_object_nested(pairs) -%}
json_object({% for item in pairs %}{% if loop.index % 2 == 1 %}{{ sl_str(item) }}{% else %}{{ item }}{% endif %}{% if not loop.last %}, {% endif %}{% endfor %})
{%- endmacro %}

{# ISO 8601 UTC text for a naive UTC timestamp. #}
{% macro sl_iso_utc(ts) -%}
strftime({{ ts }}, '%Y-%m-%dT%H:%M:%SZ')
{%- endmacro %}

{# JSON array string of the non-null values of `expr` (aggregate). #}
{% macro sl_json_array_agg(expr, order_by) -%}
CAST(to_json(list({{ expr }} ORDER BY {{ order_by }}) FILTER (WHERE {{ expr }} IS NOT NULL)) AS VARCHAR)
{%- endmacro %}

{# JSON array string from a list of SQL expressions, dropping NULLs (row-level). #}
{% macro sl_json_array_of(exprs) -%}
CAST(to_json(list_filter([{% for e in exprs %}{{ e }}{% if not loop.last %}, {% endif %}{% endfor %}], x -> x IS NOT NULL)) AS VARCHAR)
{%- endmacro %}

{# Delimited string of distinct values (aggregate). #}
{% macro sl_string_agg(expr, sep) -%}
string_agg(DISTINCT {{ expr }}, '{{ sep }}' ORDER BY {{ expr }})
{%- endmacro %}

{# Case-insensitive substring test. Both sides are lower-cased by the caller. #}
{% macro sl_contains(haystack, needle) -%}
(strpos({{ haystack }}, {{ needle }}) > 0)
{%- endmacro %}

{% macro sl_regex_match(text, pattern) -%}
regexp_matches({{ text }}, {{ pattern }})
{%- endmacro %}

{% macro sl_regex_extract(text, pattern, group) -%}
regexp_extract({{ text }}, {{ pattern }}, {{ group }})
{%- endmacro %}

{% macro sl_regex_replace(text, pattern, replacement) -%}
regexp_replace({{ text }}, {{ pattern }}, {{ replacement }}, 'g')
{%- endmacro %}

{# Escape a SQL string expression for use inside a regular expression. #}
{% macro sl_regex_escape(expr) -%}
regexp_replace({{ expr }}, '([.+*?()\[\]{}^$|\\])', '\\\1', 'g')
{%- endmacro %}

{# Hours from a to b as a double. #}
{% macro sl_hours_between(a, b) -%}
(date_diff('second', {{ a }}, {{ b }}) / 3600.0)
{%- endmacro %}

{% macro sl_minutes_between(a, b) -%}
(date_diff('second', {{ a }}, {{ b }}) / 60.0)
{%- endmacro %}

{% macro sl_month(ts) -%}
CAST(date_trunc('month', {{ ts }}) AS DATE)
{%- endmacro %}

{% macro sl_days_in_month(date_expr) -%}
EXTRACT(DAY FROM last_day({{ date_expr }}))
{%- endmacro %}

{# One row per calendar day touched by [start_ts, end_ts) (set-returning, use in SELECT). #}
{% macro sl_days_in_range(start_ts, end_ts) -%}
CAST(unnest(range(CAST({{ start_ts }} AS DATE), CAST({{ end_ts }} - INTERVAL 1 SECOND AS DATE) + INTERVAL 1 DAY, INTERVAL 1 DAY)) AS DATE)
{%- endmacro %}

{% macro sl_ts(literal) -%}
CAST('{{ literal }}' AS TIMESTAMP)
{%- endmacro %}

{# Wall-clock time in UTC (naive), for audit timestamps and lag metrics. #}
{% macro sl_now_utc() -%}
CAST(timezone('UTC', current_timestamp) AS TIMESTAMP)
{%- endmacro %}

{# Median (aggregate). #}
{% macro sl_median(expr) -%}
median({{ expr }})
{%- endmacro %}

{# Idempotent table bootstrap for merge assets (merge needs the target to exist). #}
{% macro sl_create_schema(schema) -%}
CREATE SCHEMA IF NOT EXISTS {{ schema }}
{%- endmacro %}
