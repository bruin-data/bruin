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

{# A path alone does not identify a page. A property that spans hosts — a blog or
   docs subdomain, a country subdomain — commonly serves the same path on several
   of them, and treating those as one page sums unrelated impressions and revenue
   together. Use this wherever a page has to be counted or ranked as a unit. #}
{% macro page_identity(hostname_column, path_column) -%}
CONCAT(COALESCE({{ hostname_column }}, ''), {{ path_column }})
{%- endmacro %}

{# Splits a site into the three roles its pages actually play, because
   organic traffic to each means something different and mixing them corrupts
   every acquisition metric.

   'product' pages — pricing, features, integrations, comparisons — are where
   buying decisions happen. 'content' is the blog and guides that earn top-of-
   funnel demand. 'support' is documentation and help, and its organic traffic is
   overwhelmingly existing customers looking something up. A docs site can easily
   out-rank the marketing site and account for most organic sessions while
   contributing nothing to pipeline, so counting it as acquisition makes
   conversion rates look terrible for reasons that have nothing to do with
   acquisition. #}
{% macro page_role(path_column, support_pattern, content_pattern) -%}
CASE
  WHEN {{ path_column }} IS NULL THEN 'unknown'
  WHEN REGEXP_CONTAINS({{ path_column }}, {{ re_literal(support_pattern) }}) THEN 'support'
  WHEN REGEXP_CONTAINS({{ path_column }}, {{ re_literal(content_pattern) }}) THEN 'content'
  ELSE 'product'
END
{%- endmacro %}
