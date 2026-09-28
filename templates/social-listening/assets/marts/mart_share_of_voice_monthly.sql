/* @bruin
name: marts.mart_share_of_voice_monthly
description: |
  Monthly mention counts for the brand, each competitor and each tracked topic,
  per source and across all sources (source = 'all').

  * Scope: content with an accepted term match that was not excluded as spam,
    a bot, a duplicate, an excluded author or community, unsupported or stale.
    Team and self content stays in scope but is counted separately.
  * organic_* columns exclude team and self content (team_authors).
  * denominator_organic_mentions: organic brand plus competitor mentions for
    the same month and source. share_of_voice = organic_mentions / that
    denominator, for brand and competitor rows only (NULL for topics).
  * coverage: days with at least one successful collection run for the source
    divided by days elapsed in the month (as of the run), and the list of
    sources that contributed. Low coverage means the share is not comparable
    across months.
tags: [report, mart]
depends:
  - enrichment.fct_mention_assessment
  - operations.fct_source_run
  - config.dim_term
  - enrichment.fct_term_match
  - staging.stg_content_item
materialization:
  type: table
columns:
  - name: month
    type: date
    checks:
      - name: not_null
  - name: source
    type: varchar
    checks:
      - name: not_null
  - name: term_id
    type: varchar
    checks:
      - name: not_null
  - name: mentions
    type: integer
    checks:
      - name: non_negative
  - name: organic_mentions
    type: integer
    checks:
      - name: non_negative
  - name: self_mentions
    type: integer
    checks:
      - name: non_negative
  - name: share_of_voice
    type: double
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: coverage_ratio
    type: double
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
custom_checks:
  - name: organic and self mentions add up
    query: SELECT COUNT(*) FROM marts.mart_share_of_voice_monthly WHERE organic_mentions + self_mentions <> mentions
    value: 0
@bruin */

{%- set assessment_version = sl_assessment_version(var) | trim %}

WITH in_scope AS (
    SELECT a.content_id, a.is_team_content, s.source, s.author, s.published_at
    FROM enrichment.fct_mention_assessment a
    JOIN staging.stg_content_item s ON s.content_id = a.content_id
    WHERE a.assessment_version = {{ sl_str(assessment_version) }}
      AND (a.is_eligible OR a.exclusion_reason = 'team_or_self_content')
),

mentions AS (
    SELECT
        {{ sl_month("i.published_at") }} AS month,
        i.source,
        m.term_id,
        t.term_name,
        m.category,
        i.content_id,
        i.author,
        i.is_team_content
    FROM in_scope i
    JOIN enrichment.fct_term_match m ON m.content_id = i.content_id AND m.is_accepted
    JOIN config.dim_term t ON t.term_id = m.term_id
),

by_source AS (
    SELECT month, source, term_id, term_name, category, content_id, author, is_team_content FROM mentions
    UNION ALL
    SELECT month, 'all' AS source, term_id, term_name, category, content_id, author, is_team_content FROM mentions
),

counts AS (
    SELECT
        month,
        source,
        term_id,
        MIN(term_name) AS term_name,
        MIN(category) AS category,
        COUNT(DISTINCT content_id) AS mentions,
        COUNT(DISTINCT CASE WHEN NOT is_team_content THEN content_id END) AS organic_mentions,
        COUNT(DISTINCT CASE WHEN is_team_content THEN content_id END) AS self_mentions,
        COUNT(DISTINCT author) AS distinct_authors,
        COUNT(DISTINCT CASE WHEN NOT is_team_content THEN author END) AS organic_distinct_authors
    FROM by_source
    GROUP BY month, source, term_id
),

denominator AS (
    SELECT
        month,
        source,
        SUM(CASE WHEN category IN ('brand', 'competitor') THEN organic_mentions ELSE 0 END) AS denominator_organic_mentions,
        SUM(CASE WHEN category IN ('brand', 'competitor') THEN mentions ELSE 0 END) AS denominator_all_mentions
    FROM counts
    GROUP BY month, source
),

collection_days AS (
    SELECT month, source, COUNT(DISTINCT run_day) AS days_collected
    FROM (
        SELECT {{ sl_month("collected_at") }} AS month, source, CAST(collected_at AS DATE) AS run_day
        FROM operations.fct_source_run
        WHERE run_status IN ('ok', 'partial')
        UNION ALL
        SELECT {{ sl_month("collected_at") }} AS month, 'all' AS source, CAST(collected_at AS DATE) AS run_day
        FROM operations.fct_source_run
        WHERE run_status IN ('ok', 'partial')
    ) runs
    GROUP BY month, source
),

contributing AS (
    SELECT month, {{ sl_string_agg("source", ", ") }} AS contributing_sources
    FROM counts
    WHERE source <> 'all'
    GROUP BY month
)

SELECT
    c.month,
    c.source,
    c.term_id,
    c.term_name,
    c.category,
    CAST(c.mentions AS INTEGER) AS mentions,
    CAST(c.organic_mentions AS INTEGER) AS organic_mentions,
    CAST(c.self_mentions AS INTEGER) AS self_mentions,
    CAST(c.distinct_authors AS INTEGER) AS distinct_authors,
    CAST(c.organic_distinct_authors AS INTEGER) AS organic_distinct_authors,
    CAST(d.denominator_organic_mentions AS INTEGER) AS denominator_organic_mentions,
    CAST(d.denominator_all_mentions AS INTEGER) AS denominator_all_mentions,
    CASE
        WHEN c.category IN ('brand', 'competitor') AND d.denominator_organic_mentions > 0
        THEN ROUND(c.organic_mentions * 1.0 / d.denominator_organic_mentions, 4)
    END AS share_of_voice,
    CAST(COALESCE(cd.days_collected, 0) AS INTEGER) AS days_collected,
    CAST(
        CASE WHEN c.month = {{ sl_month(sl_ts(end_datetime)) }}
             THEN EXTRACT(DAY FROM {{ sl_ts(end_datetime) }})
             ELSE {{ sl_days_in_month('c.month') }}
        END AS INTEGER) AS days_in_period,
    LEAST(1.0, ROUND(COALESCE(cd.days_collected, 0) * 1.0 / NULLIF(
        CASE WHEN c.month = {{ sl_month(sl_ts(end_datetime)) }}
             THEN EXTRACT(DAY FROM {{ sl_ts(end_datetime) }})
             ELSE {{ sl_days_in_month('c.month') }}
        END, 0), 4)) AS coverage_ratio,
    ct.contributing_sources,
    {{ sl_ts(end_datetime) }} AS data_as_of
FROM counts c
JOIN denominator d ON d.month = c.month AND d.source = c.source
LEFT JOIN collection_days cd ON cd.month = c.month AND cd.source = c.source
LEFT JOIN contributing ct ON ct.month = c.month
