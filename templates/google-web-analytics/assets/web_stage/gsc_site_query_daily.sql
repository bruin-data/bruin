/* @bruin
name: web_stage.gsc_site_query_daily
type: bq.sql
description: >
  Property-level Search Console impressions by day, query, country, device, and
  search type, read from the searchdata_site_impression export table. Impressions
  here are counted once per property per query, which is the grain the Search
  Console performance report uses, so property totals reconcile with the UI.
  Queries Google anonymized keep their impressions and clicks but carry a NULL
  query and a query_brand_type of 'anonymized'.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: data_date
  partition_by: data_date
  cluster_by:
    - query_brand_type
    - device

depends:
  - sources.gsc_searchdata_site_impression

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
    description: >
      Search Console property, either a URL prefix such as https://example.com/
      or a domain property such as sc-domain:example.com.
    primary_key: true
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
      Commercial intent of the query: 'competitor', 'branded', 'commercial',
      'informational', or 'anonymized'. Independent of query_brand_type.
    checks:
      - name: not_null
      - name: accepted_values
        value: ["competitor", "branded", "commercial", "informational", "anonymized"]
  - name: competitor_name
    type: STRING
    description: Which competitor the query mentions, NULL when it names none.
  - name: query_word_count
    type: INT64
    description: Whitespace-delimited word count of the query, 0 when anonymized.
  - name: is_anonymized_query
    type: BOOL
    description: Whether Google withheld the query to protect user privacy.
    primary_key: true
    checks:
      - name: not_null
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
    description: Property-level impressions.
    checks:
      - name: non_negative
  - name: clicks
    type: INT64
    description: Clicks through to the property.
    checks:
      - name: non_negative
  - name: sum_top_position
    type: NUMERIC
    description: >
      Summed zero-based topmost position of the property across its impressions.
      Kept so downstream reports can re-aggregate position correctly.
  - name: avg_position
    type: FLOAT64
    description: >
      One-based average top position for this row, NULL without impressions.
      Averaging this column across rows is wrong; re-derive it from
      sum_top_position and impressions instead.

custom_checks:
  - name: daily search console grain is unique
    description: >
      The export is already unique on its full dimension set, so collapsing the
      search-appearance columns must not introduce duplicate rows.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          data_date,
          site_url,
          query,
          is_anonymized_query,
          country,
          search_type,
          device
        FROM {{ this }}
        GROUP BY 1, 2, 3, 4, 5, 6, 7
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
    IF(is_anonymized_query, NULL, query) AS query,
    is_anonymized_query,
    country,
    search_type,
    device,
    impressions,
    clicks,
    sum_top_position
  FROM `{{ var.search_console_dataset }}.searchdata_site_impression`
  WHERE data_date
    BETWEEN DATE_SUB(DATE('{{ start_date }}'), INTERVAL {{ var.source_lookback_days }} DAY)
    AND DATE('{{ end_date }}')
)

SELECT
  data_date,
  site_url,
  query,
  {{ query_brand_type('query', 'is_anonymized_query', var.brand_query_pattern) }} AS query_brand_type,
  {{ query_intent_type('query', 'is_anonymized_query', var.brand_query_pattern, var.competitor_names, var.commercial_query_pattern) }}
    AS query_intent_type,
  {{ competitor_name('query', var.competitor_names) }} AS competitor_name,
  {{ query_word_count('query') }} AS query_word_count,
  is_anonymized_query,
  country,
  search_type,
  device,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(sum_top_position) AS sum_top_position,
  {{ average_position('SUM(sum_top_position)', 'SUM(impressions)') }} AS avg_position
FROM source_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;
