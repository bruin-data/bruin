/* @bruin
name: enrichment.fct_mention_candidate
description: |
  Every content item with at least one term match, and whether it is eligible
  for assessment. Exclusions run here, before any model work, in this order:
  no accepted term match, syndicated duplicate, excluded author, excluded or
  out-of-scope community, unsupported content type, bot or automated author,
  excluded term, stale content, and team or self content (unless
  route_team_content is true). The first rule that applies is recorded.

  Team and self content keeps is_team_content = true so share of voice can
  report it separately from organic content.
tags: [enrich]
depends:
  - staging.stg_content_item
  - enrichment.fct_term_match
materialization:
  type: table
columns:
  - name: content_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
      - name: relationships
    foreign_key:
      table: staging.stg_content_item
      column: content_id
  - name: is_eligible
    type: boolean
    checks:
      - name: not_null
  - name: exclusion_reason
    type: varchar
    checks:
      - name: accepted_values
        value: [no_accepted_term_match, syndicated_duplicate, excluded_author, excluded_community, community_not_included, unsupported_content_type, bot_or_automated_author, excluded_term, stale_content, team_or_self_content]
  - name: is_team_content
    type: boolean
    checks:
      - name: not_null
custom_checks:
  - name: eligible rows have no exclusion reason
    query: SELECT COUNT(*) FROM enrichment.fct_mention_candidate WHERE is_eligible = (exclusion_reason IS NOT NULL)
    value: 0
@bruin */

WITH matches AS (
    SELECT
        content_id,
        SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) AS accepted_terms,
        COUNT(*) AS considered_terms,
        {{ sl_string_agg("CASE WHEN is_accepted THEN term_id END", ",") }} AS accepted_term_ids,
        {{ sl_string_agg("CASE WHEN is_accepted THEN category END", ",") }} AS accepted_categories,
        {{ sl_string_agg("CASE WHEN is_accepted THEN matched_text END", " | ") }} AS matched_terms
    FROM enrichment.fct_term_match
    GROUP BY content_id
),

scored AS (
    SELECT
        s.*,
        m.accepted_terms,
        m.considered_terms,
        m.accepted_term_ids,
        m.accepted_categories,
        m.matched_terms,
        lower(COALESCE(s.title, '') || ' ' || COALESCE(s.body, '')) AS text_lower,
        lower(COALESCE(s.author, '')) AS author_lower,
        lower(COALESCE(s.community, '')) AS community_lower,
        {{ sl_hours_between("s.published_at", sl_ts(end_datetime)) }} AS age_hours
    FROM staging.stg_content_item s
    JOIN matches m ON m.content_id = s.content_id
),

flagged AS (
    SELECT
        *,
        author_lower IN {{ sl_in_list(var.team_authors) }} AS is_team_content,
        (
            is_platform_bot
            {%- for pattern in var.bot_author_patterns %}
            OR {{ sl_regex_match("COALESCE(author, '')", sl_str(pattern)) }}
            {%- endfor %}
        ) AS is_bot_author,
        (
            FALSE
            {%- for term in var.excluded_terms %}
            OR {{ sl_contains("text_lower", sl_str(term | lower)) }}
            {%- endfor %}
        ) AS has_excluded_term
    FROM scored
)

SELECT
    content_id,
    source,
    external_id,
    platform,
    content_type,
    url,
    author,
    community,
    title,
    body,
    published_at,
    collected_at,
    age_hours,
    accepted_terms,
    considered_terms,
    accepted_term_ids,
    accepted_categories,
    matched_terms,
    is_team_content,
    is_bot_author,
    exclusion_reason,
    exclusion_reason IS NULL AS is_eligible
FROM (
    SELECT
        *,
        CASE
            WHEN accepted_terms = 0 THEN 'no_accepted_term_match'
            WHEN is_syndicated_duplicate THEN 'syndicated_duplicate'
            WHEN author_lower IN {{ sl_in_list(var.excluded_authors) }} THEN 'excluded_author'
            WHEN source = 'reddit' AND community_lower IN {{ sl_in_list(var.reddit_communities_exclude) }} THEN 'excluded_community'
            {%- if var.reddit_communities_include %}
            WHEN source = 'reddit' AND community_lower NOT IN {{ sl_in_list(var.reddit_communities_include) }} THEN 'community_not_included'
            {%- endif %}
            WHEN content_type NOT IN {{ sl_in_list(var.supported_content_types) }} THEN 'unsupported_content_type'
            WHEN is_bot_author THEN 'bot_or_automated_author'
            WHEN has_excluded_term THEN 'excluded_term'
            WHEN age_hours > {{ var.max_content_age_days }} * 24 THEN 'stale_content'
            WHEN is_team_content AND NOT {{ sl_bool(var.route_team_content) }} THEN 'team_or_self_content'
        END AS exclusion_reason
    FROM flagged
) AS decided
