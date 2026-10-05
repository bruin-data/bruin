/* @bruin
name: staging.stg_content_item
description: |
  Normalized posts, comments, stories, issues and questions from every raw
  source table. One row per (source, external ID): the latest collected
  version wins, earlier versions stay in raw. content_id is a stable hash of
  source and external ID.

  Rows in config.redaction_request are removed here. Secondary de-duplication
  is deliberately narrow: only a Reddit cross-post whose original post is also
  present is marked as a syndicated duplicate. Links shared on several
  platforms are kept as separate conversations.
tags: [enrich, staging]
depends:
  - raw.raw_reddit_content
  - raw.raw_hackernews_content
  - raw.raw_github_content
  - raw.raw_stackoverflow_content
  - raw.raw_authorised_export_content
  - config.redaction_request
materialization:
  type: table
columns:
  - name: content_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: source
    type: varchar
    checks:
      - name: not_null
  - name: external_id
    type: varchar
    checks:
      - name: not_null
  - name: content_type
    type: varchar
    checks:
      - name: accepted_values
        value: [post, comment, story, issue, pull_request, question, export_item]
  - name: url
    type: varchar
    checks:
      - name: not_null
      - name: pattern
        value: "^https://.+$"
  - name: published_at
    type: timestamp
    checks:
      - name: not_null
  - name: collected_at
    type: timestamp
    checks:
      - name: not_null
  - name: version_count
    type: integer
    checks:
      - name: positive
  - name: engagement_raw
    type: double
    checks:
      - name: non_negative
custom_checks:
  - name: no source record appears twice
    query: |
      SELECT COUNT(*) FROM (
        SELECT source, external_id FROM staging.stg_content_item GROUP BY 1, 2 HAVING COUNT(*) > 1
      )
    value: 0
  - name: redacted records are absent
    query: |
      SELECT COUNT(*) FROM staging.stg_content_item s
      JOIN config.redaction_request r ON r.source = s.source AND r.external_id = s.external_id
    value: 0
@bruin */

WITH raw_content AS (
    SELECT event_key, source, external_id, content_type, payload_json, CAST(published_at AS TIMESTAMP) AS published_at, CAST(collected_at AS TIMESTAMP) AS collected_at, collection_mode
    FROM raw.raw_reddit_content WHERE record_kind = 'content'
    UNION ALL
    SELECT event_key, source, external_id, content_type, payload_json, CAST(published_at AS TIMESTAMP), CAST(collected_at AS TIMESTAMP), collection_mode
    FROM raw.raw_hackernews_content WHERE record_kind = 'content'
    UNION ALL
    SELECT event_key, source, external_id, content_type, payload_json, CAST(published_at AS TIMESTAMP), CAST(collected_at AS TIMESTAMP), collection_mode
    FROM raw.raw_github_content WHERE record_kind = 'content'
    UNION ALL
    SELECT event_key, source, external_id, content_type, payload_json, CAST(published_at AS TIMESTAMP), CAST(collected_at AS TIMESTAMP), collection_mode
    FROM raw.raw_stackoverflow_content WHERE record_kind = 'content'
    UNION ALL
    SELECT event_key, source, external_id, content_type, payload_json, CAST(published_at AS TIMESTAMP), CAST(collected_at AS TIMESTAMP), collection_mode
    FROM raw.raw_authorised_export_content WHERE record_kind = 'content'
),

versioned AS (
    -- Demo fixtures and live data never mix: only rows from the current mode are used.
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY source, external_id ORDER BY collected_at DESC, event_key DESC) AS version_rank,
        COUNT(*) OVER (PARTITION BY source, external_id) AS version_count,
        MIN(collected_at) OVER (PARTITION BY source, external_id) AS first_collected_at
    FROM raw_content
    WHERE collection_mode = {% if var.demo_mode %}'demo'{% else %}'live'{% endif %}
),

latest AS (
    SELECT v.*
    FROM versioned v
    LEFT JOIN config.redaction_request r
        ON r.source = v.source AND r.external_id = v.external_id
    WHERE v.version_rank = 1
      AND r.external_id IS NULL
),

normalized AS (
    SELECT
        source,
        external_id,
        content_type,
        published_at,
        first_collected_at,
        collected_at,
        version_count,
        collection_mode,
        CASE source
            WHEN 'reddit' THEN 'https://www.reddit.com' || {{ sl_json_str('payload_json', '$.permalink') }}
            WHEN 'hackernews' THEN 'https://news.ycombinator.com/item?id=' || external_id
            WHEN 'github' THEN {{ sl_json_str('payload_json', '$.html_url') }}
            WHEN 'stackoverflow' THEN {{ sl_json_str('payload_json', '$.link') }}
            ELSE {{ sl_json_str('payload_json', '$.url') }}
        END AS url,
        CASE source
            WHEN 'authorised_export' THEN lower({{ sl_json_str('payload_json', '$.platform') }})
            ELSE source
        END AS platform,
        {{ sl_json_str('payload_json', '$.author') }} AS author,
        CASE source
            WHEN 'reddit' THEN {{ sl_json_str('payload_json', '$.subreddit') }}
            WHEN 'github' THEN replace({{ sl_json_str('payload_json', '$.repository_url') }}, 'https://api.github.com/repos/', '')
            WHEN 'authorised_export' THEN lower({{ sl_json_str('payload_json', '$.platform') }})
            ELSE source
        END AS community,
        CASE source
            WHEN 'reddit' THEN {{ sl_json_str('payload_json', '$.parent_id') }}
            WHEN 'hackernews' THEN {{ sl_json_str('payload_json', '$.parent_id') }}
        END AS parent_external_id,
        CASE source
            WHEN 'hackernews' THEN COALESCE({{ sl_json_str('payload_json', '$.title') }}, {{ sl_json_str('payload_json', '$.story_title') }})
            ELSE {{ sl_json_str('payload_json', '$.title') }}
        END AS title,
        CASE source
            WHEN 'reddit' THEN COALESCE(NULLIF({{ sl_json_str('payload_json', '$.selftext') }}, ''), {{ sl_json_str('payload_json', '$.body') }})
            WHEN 'hackernews' THEN COALESCE({{ sl_json_str('payload_json', '$.story_text') }}, {{ sl_json_str('payload_json', '$.comment_text') }})
            ELSE {{ sl_json_str('payload_json', '$.body') }}
        END AS body_html,
        CASE source
            WHEN 'reddit' THEN {{ sl_json_object(['score', sl_json_num('payload_json', '$.score'), 'comments', sl_json_num('payload_json', '$.num_comments')]) }}
            WHEN 'hackernews' THEN {{ sl_json_object(['points', sl_json_num('payload_json', '$.points'), 'comments', sl_json_num('payload_json', '$.num_comments')]) }}
            WHEN 'github' THEN {{ sl_json_object(['reactions', sl_json_num('payload_json', '$.reactions'), 'comments', sl_json_num('payload_json', '$.comments')]) }}
            WHEN 'stackoverflow' THEN {{ sl_json_object(['score', sl_json_num('payload_json', '$.score'), 'answers', sl_json_num('payload_json', '$.answer_count'), 'views', sl_json_num('payload_json', '$.view_count')]) }}
            ELSE CAST(json_extract(payload_json, '$.metrics') AS VARCHAR)
        END AS metrics_json,
        CASE source
            WHEN 'reddit' THEN COALESCE({{ sl_json_num('payload_json', '$.score') }}, 0) + 2 * COALESCE({{ sl_json_num('payload_json', '$.num_comments') }}, 0)
            WHEN 'hackernews' THEN COALESCE({{ sl_json_num('payload_json', '$.points') }}, 0) + 2 * COALESCE({{ sl_json_num('payload_json', '$.num_comments') }}, 0)
            WHEN 'github' THEN COALESCE({{ sl_json_num('payload_json', '$.reactions') }}, 0) + 2 * COALESCE({{ sl_json_num('payload_json', '$.comments') }}, 0)
            WHEN 'stackoverflow' THEN COALESCE({{ sl_json_num('payload_json', '$.score') }}, 0) + 2 * COALESCE({{ sl_json_num('payload_json', '$.answer_count') }}, 0)
            ELSE COALESCE({{ sl_json_num('payload_json', '$.metrics.likes') }}, 0) + 2 * COALESCE({{ sl_json_num('payload_json', '$.metrics.comments') }}, 0)
        END AS engagement_raw_signed,
        {{ sl_json_object([
            'removed_by_category', sl_json_str('payload_json', '$.removed_by_category'),
            'crosspost_parent', sl_json_str('payload_json', '$.crosspost_parent'),
            'over_18', sl_json_str('payload_json', '$.over_18'),
            'stickied', sl_json_str('payload_json', '$.stickied'),
            'distinguished', sl_json_str('payload_json', '$.distinguished'),
            'author_type', sl_json_str('payload_json', '$.author_type'),
            'tags', sl_json_str('payload_json', '$.tags'),
            'platform', sl_json_str('payload_json', '$.platform')
        ]) }} AS metadata_json,
        {{ sl_json_str('payload_json', '$.crosspost_parent') }} AS crosspost_parent,
        {{ sl_json_str('payload_json', '$.removed_by_category') }} AS removed_by_category,
        {{ sl_json_str('payload_json', '$.author_type') }} AS author_type
    FROM latest
),

cleaned AS (
    SELECT
        *,
        -- Hacker News and Stack Exchange return HTML; keep readable text only.
        trim({{ sl_regex_replace(sl_regex_replace("replace(replace(replace(replace(COALESCE(body_html, ''), '&#x27;', ''''), '&quot;', '\"'), '&amp;', '&'), '&#x2F;', '/')", "'<[^>]+>'", "' '"), "'\\s+'", "' '") }}) AS body
    FROM normalized
)

SELECT
    {{ sl_hash("c.source || ':' || c.external_id") }} AS content_id,
    c.source,
    c.external_id,
    c.platform,
    c.content_type,
    c.url,
    c.author,
    c.community,
    c.parent_external_id,
    CASE WHEN c.parent_external_id IS NOT NULL THEN {{ sl_hash("c.source || ':' || c.parent_external_id") }} END AS parent_content_id,
    c.title,
    NULLIF(c.body, '') AS body,
    c.published_at,
    c.first_collected_at,
    c.collected_at,
    CAST(c.version_count AS INTEGER) AS version_count,
    c.collection_mode,
    c.metrics_json,
    c.metadata_json,
    GREATEST(c.engagement_raw_signed, 0) AS engagement_raw,
    (c.removed_by_category IS NOT NULL OR c.body IN ('[removed]', '[deleted]')) AS is_removed,
    (c.author_type = 'Bot') AS is_platform_bot,
    (orig.external_id IS NOT NULL) AS is_syndicated_duplicate,
    CASE WHEN orig.external_id IS NOT NULL THEN {{ sl_hash("orig.source || ':' || orig.external_id") }} END AS duplicate_of_content_id
FROM cleaned c
LEFT JOIN cleaned orig
    ON c.source = 'reddit'
   AND orig.source = 'reddit'
   AND c.crosspost_parent IS NOT NULL
   AND orig.external_id = c.crosspost_parent
