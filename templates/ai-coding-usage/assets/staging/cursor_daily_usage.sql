/* @bruin

name: staging.cursor_daily_usage
type: duckdb.sql

description: Typed daily Cursor activity, productivity, request, and acceptance metrics.

materialization:
  type: table

depends:
  - raw.cursor_daily_usage

columns:
  - name: usage_date
    type: date
    description: UTC usage date.
    primary_key: true
    checks:
      - name: not_null
  - name: user_id
    type: varchar
    description: Lowercase Cursor team member email address.
    primary_key: true
    checks:
      - name: not_null
  - name: total_requests
    type: bigint
    description: Composer, chat, and agent requests combined.
    checks:
      - name: non_negative
  - name: suggestion_acceptance_rate
    type: double
    description: Accepted suggestions divided by accepted plus rejected suggestions.
  - name: tab_acceptance_rate
    type: double
    description: Accepted tab completions divided by tab completions shown.

@bruin */

SELECT
  CASE
    WHEN TRY_CAST(date AS BIGINT) IS NOT NULL
      THEN CAST(TO_TIMESTAMP(TRY_CAST(date AS BIGINT) / 1000.0) AS DATE)
    ELSE TRY_CAST(date AS DATE)
  END AS usage_date,
  LOWER(TRIM(CAST(email AS VARCHAR))) AS user_id,
  COALESCE(TRY_CAST("isActive" AS BOOLEAN), FALSE) AS is_active,
  COALESCE(TRY_CAST("totalLinesAdded" AS BIGINT), 0) AS total_lines_added,
  COALESCE(TRY_CAST("totalLinesDeleted" AS BIGINT), 0) AS total_lines_removed,
  COALESCE(TRY_CAST("acceptedLinesAdded" AS BIGINT), 0) AS ai_lines_added,
  COALESCE(TRY_CAST("acceptedLinesDeleted" AS BIGINT), 0) AS ai_lines_removed,
  COALESCE(TRY_CAST("totalApplies" AS BIGINT), 0) AS apply_operations,
  COALESCE(TRY_CAST("totalAccepts" AS BIGINT), 0) AS suggestions_accepted,
  COALESCE(TRY_CAST("totalRejects" AS BIGINT), 0) AS suggestions_rejected,
  COALESCE(TRY_CAST("totalTabsShown" AS BIGINT), 0) AS tabs_shown,
  COALESCE(TRY_CAST("totalTabsAccepted" AS BIGINT), 0) AS tabs_accepted,
  COALESCE(TRY_CAST("composerRequests" AS BIGINT), 0) AS composer_requests,
  COALESCE(TRY_CAST("chatRequests" AS BIGINT), 0) AS chat_requests,
  COALESCE(TRY_CAST("agentRequests" AS BIGINT), 0) AS agent_requests,
  COALESCE(TRY_CAST("composerRequests" AS BIGINT), 0)
    + COALESCE(TRY_CAST("chatRequests" AS BIGINT), 0)
    + COALESCE(TRY_CAST("agentRequests" AS BIGINT), 0) AS total_requests,
  COALESCE(TRY_CAST("cmdkUsages" AS BIGINT), 0) AS cmdk_usages,
  COALESCE(TRY_CAST("subscriptionIncludedReqs" AS BIGINT), 0) AS subscription_included_requests,
  COALESCE(TRY_CAST("apiKeyReqs" AS BIGINT), 0) AS api_key_requests,
  COALESCE(TRY_CAST("usageBasedReqs" AS BIGINT), 0) AS usage_based_requests,
  COALESCE(TRY_CAST("bugbotUsages" AS BIGINT), 0) AS bugbot_usages,
  COALESCE(TRY_CAST("totalAccepts" AS DOUBLE), 0)
    / NULLIF(
      COALESCE(TRY_CAST("totalAccepts" AS DOUBLE), 0)
        + COALESCE(TRY_CAST("totalRejects" AS DOUBLE), 0),
      0
    ) AS suggestion_acceptance_rate,
  COALESCE(TRY_CAST("totalTabsAccepted" AS DOUBLE), 0)
    / NULLIF(COALESCE(TRY_CAST("totalTabsShown" AS DOUBLE), 0), 0) AS tab_acceptance_rate,
  CAST("mostUsedModel" AS VARCHAR) AS primary_model,
  CAST("applyMostUsedExtension" AS VARCHAR) AS primary_apply_extension,
  CAST("tabMostUsedExtension" AS VARCHAR) AS primary_tab_extension,
  CAST("clientVersion" AS VARCHAR) AS client_version
FROM raw.cursor_daily_usage;
