/* @bruin
name: web_stage.gsc_url_query_daily
type: bq.sql
description: >
  URL-level Search Console impressions by day, query, country, device, and search
  type, read from the searchdata_url_impression export table. This is the only
  grain that carries both the query and the page, and page_path is the key every
  join against GA4 uses. The search-appearance flags in the export are collapsed
  here so the model keeps working as Google adds new ones.

  URL-level impressions are counted per URL, so summing them across pages yields
  a larger total than the property-level model. Use web_stage.gsc_site_query_daily
  for property totals and this model for page and query analysis.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: data_date
  partition_by: data_date
  cluster_by:
    - page_path
    - query_intent_type
    - query_brand_type

depends:
  - sources.gsc_searchdata_url_impression

tags:
  - web_stage
  - search_console
  - seo

columns:
  - name: data_date
    type: DATE
    description: Search Console reporting date, in Pacific Time.
    primary_key: true
    checks:
      - name: not_null
  - name: site_url
    type: STRING
    description: Search Console property that reported the impression.
    primary_key: true
    checks:
      - name: not_null
  - name: url
    type: STRING
    description: Canonical absolute URL Google showed in search results.
    primary_key: true
    checks:
      - name: not_null
  - name: page_path
    type: STRING
    description: >
      Lowercased path with the scheme, host, query string, fragment, and trailing
      slashes removed. Shared join key with the GA4 models.
    checks:
      - name: not_null
  - name: page_hostname
    type: STRING
    description: Lowercased hostname of the URL, useful for splitting subdomains.
  - name: page_role
    type: STRING
    description: >
      Whether the ranking page is a 'product' page, 'content' marketing, or
      'support' documentation. Documentation frequently out-ranks the marketing
      site, and its clicks are mostly existing customers.
    checks:
      - name: not_null
  - name: query
    type: STRING
    description: Search query, NULL when Google anonymized it.
    primary_key: true
  - name: query_brand_type
    type: STRING
    description: Query classification of 'branded', 'non_branded', or 'anonymized'.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["branded", "non_branded", "anonymized"]
  - name: query_intent_type
    type: STRING
    description: >
      Commercial intent of the query: 'competitor' for a rival comparison,
      'branded' for the property's own brand, 'commercial' for pricing and
      evaluation modifiers, 'informational' otherwise, 'anonymized' when withheld.
      Independent of query_brand_type so the two can be crossed.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["competitor", "branded", "commercial", "informational", "anonymized"]
  - name: competitor_name
    type: STRING
    description: >
      Which competitor the query mentions, from the competitor_names variable.
      NULL when the query names none.
  - name: query_word_count
    type: INT64
    description: Whitespace-delimited word count of the query, 0 when anonymized.
  - name: is_anonymized_query
    type: BOOL
    description: Whether Google withheld the query to protect user privacy.
    primary_key: true
    checks:
      - name: not_null
  - name: is_anonymized_discover
    type: BOOL
    description: Whether Google withheld Discover detail for this row.
  - name: country
    type: STRING
    description: Three-letter country code of the searcher.
    primary_key: true
  - name: search_type
    type: STRING
    description: Search surface, such as WEB, IMAGE, VIDEO, NEWS, or DISCOVER.
    primary_key: true
  - name: device
    type: STRING
    description: Device class of the searcher, such as DESKTOP, MOBILE, or TABLET.
    primary_key: true
  - name: impressions
    type: INT64
    description: URL-level impressions.
    checks:
      - name: non_negative
  - name: clicks
    type: INT64
    description: Clicks through to this URL.
    checks:
      - name: non_negative
  - name: sum_position
    type: NUMERIC
    description: >
      Summed zero-based position of the URL across its impressions. Kept so
      downstream reports can re-aggregate position correctly.
  - name: avg_position
    type: FLOAT64
    description: >
      One-based average position for this row, NULL without impressions.
      Averaging this column across rows is wrong; re-derive it from sum_position
      and impressions instead.

custom_checks:
  - name: daily url and query grain is unique
    description: >
      The export is already unique on its full dimension set, so collapsing the
      search-appearance columns must not introduce duplicate rows.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          data_date,
          site_url,
          url,
          query,
          is_anonymized_query,
          country,
          search_type,
          device
        FROM {{ this }}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: clicks never exceed impressions
    description: A click always follows an impression, so clicks cannot be the larger measure.
    query: |
      SELECT COUNT(*)
      FROM {{ this }}
      WHERE clicks > impressions
    value: 0
@bruin */

WITH source_rows AS (
  SELECT
    data_date,
    site_url,
    url,
    {{ page_path('url') }} AS page_path,
    {{ url_hostname('url') }} AS page_hostname,
    -- Withheld queries are nulled once here, so every classification below reads
    -- the same column instead of repeating the anonymization test.
    IF(is_anonymized_query, NULL, query) AS query,
    is_anonymized_query,
    is_anonymized_discover,
    country,
    search_type,
    device,
    impressions,
    clicks,
    sum_position
  FROM `{{ var.search_console_dataset }}.searchdata_url_impression`
  WHERE data_date
    BETWEEN DATE_SUB(DATE('{{ start_date }}'), INTERVAL {{ var.source_lookback_days }} DAY)
    AND DATE('{{ end_date }}')
)

SELECT
  data_date,
  site_url,
  url,
  page_path,
  page_hostname,
  {{ page_role('page_path', var.support_path_pattern, var.content_path_pattern) }} AS page_role,
  query,
  {{ query_brand_type('query', 'is_anonymized_query', var.brand_query_pattern) }} AS query_brand_type,
  {{ query_intent_type('query', 'is_anonymized_query', var.brand_query_pattern, var.competitor_names, var.commercial_query_pattern) }}
    AS query_intent_type,
  {{ competitor_name('query', var.competitor_names) }} AS competitor_name,
  {{ query_word_count('query') }} AS query_word_count,
  is_anonymized_query,
  is_anonymized_discover,
  country,
  search_type,
  device,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(sum_position) AS sum_position,
  {{ average_position('SUM(sum_position)', 'SUM(impressions)') }} AS avg_position
FROM source_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16;
