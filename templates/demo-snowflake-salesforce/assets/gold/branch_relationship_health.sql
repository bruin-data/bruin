/* @bruin

name: gold.branch_relationship_health
type: sf.sql
description: Branch-market relationship health scorecard for credit union CRM demo accounts and activities.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - member_relationships
  - gold
  - dashboard
domains:
  - crm
  - member_relationships
meta:
  asset_grain: One row per account billing city used as demo branch market.
  load_pattern: View over silver account health and activity timeline.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: view

depends:
  - silver.salesforce_account_health
  - silver.salesforce_activity_timeline

columns:
  - name: branch_market
    type: VARCHAR
    description: Account billing city used as branch-market proxy.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: account_count
    type: INTEGER
    description: Number of accounts.
    checks:
      - name: non_negative
  - name: active_pipeline_accounts
    type: INTEGER
    description: Accounts with open opportunities.
    checks:
      - name: non_negative
  - name: open_pipeline_amount_usd
    type: DOUBLE
    description: Open pipeline amount.
    checks:
      - name: non_negative
  - name: won_amount_usd
    type: DOUBLE
    description: Closed-won amount.
    checks:
      - name: non_negative
  - name: activity_count
    type: INTEGER
    description: Task and event activity count.
    checks:
      - name: non_negative
  - name: completed_activity_count
    type: INTEGER
    description: Completed task and event activity count.
    checks:
      - name: non_negative
  - name: relationship_health_score
    type: DOUBLE
    description: Simple branch health score from pipeline coverage and activity completion.
    checks:
      - name: non_negative

@bruin */

WITH activity AS (
    SELECT
        account_id,
        COUNT(*) AS activity_count,
        COUNT_IF(completed_flag) AS completed_activity_count
    FROM silver.salesforce_activity_timeline
    GROUP BY 1
)

SELECT
    COALESCE(ah.billing_city, 'Unknown') AS branch_market,
    COUNT(*) AS account_count,
    COUNT_IF(ah.open_opportunity_count > 0) AS active_pipeline_accounts,
    SUM(ah.open_pipeline_amount_usd) AS open_pipeline_amount_usd,
    SUM(ah.won_amount_usd) AS won_amount_usd,
    SUM(COALESCE(a.activity_count, 0)) AS activity_count,
    SUM(COALESCE(a.completed_activity_count, 0)) AS completed_activity_count,
    ROUND(
        100
        * (
            0.45 * COUNT_IF(ah.open_opportunity_count > 0) / NULLIF(COUNT(*), 0)
            + 0.35 * COALESCE(SUM(COALESCE(a.completed_activity_count, 0)) / NULLIF(SUM(COALESCE(a.activity_count, 0)), 0), 0)
            + 0.20 * COUNT_IF(ah.relationship_segment IN ('High won value', 'High open pipeline')) / NULLIF(COUNT(*), 0)
        ),
        2
    ) AS relationship_health_score
FROM silver.salesforce_account_health AS ah
LEFT JOIN activity AS a
    ON a.account_id = ah.account_id
GROUP BY 1
