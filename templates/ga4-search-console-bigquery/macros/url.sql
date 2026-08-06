{# URL handling shared by the Search Console and GA4 models.

   Search Console reports a canonical absolute URL while GA4 reports whatever
   page_location the tag collected, including query strings and fragments.
   Joining the two only works when both sides are reduced by exactly the same
   rules, so every model derives its join key through page_path below rather
   than normalizing inline. #}

{% macro page_path(url_column) -%}
COALESCE(
  NULLIF(
    LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE({{ url_column }}, r'[?].*$|[#].*$', ''),
          r'^https?://[^/]+',
          ''
        ),
        r'/+$',
        ''
      )
    ),
    ''
  ),
  '/'
)
{%- endmacro %}

{% macro url_hostname(url_column) -%}
LOWER(REGEXP_EXTRACT({{ url_column }}, r'^https?://([^/:]+)'))
{%- endmacro %}
