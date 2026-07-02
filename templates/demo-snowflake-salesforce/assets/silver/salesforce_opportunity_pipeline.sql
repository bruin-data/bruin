/* @bruin

name: silver.salesforce_opportunity_pipeline
type: sf.sql
description: |
  Normalizes Salesforce Opportunity records for credit union demo analysis with
  derived product family, stage group, weighted amount, and temporal fields.
  Normal runs merge only opportunities touched by Salesforce changes in the
  Bruin interval; full-refresh runs rebuild all opportunities.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - lending
  - silver
  - mart
  - incremental_merge
domains:
  - crm
  - lending
meta:
  asset_grain: One row per current Salesforce Opportunity.
  load_pattern: Incremental merge of opportunities touched by source SystemModstamp in the Bruin interval.
  refresh_cadence: Daily batch pipeline.
  source_system: Salesforce Sales Cloud via bronze ingestr assets.

materialization:
  type: table
  strategy: merge
  cluster_by:
    - source_system_modstamp
    - close_month

depends:
  - bronze.salesforce_accounts
  - bronze.salesforce_opportunities
  - bronze.salesforce_tasks

columns:
  - name: opportunity_id
    type: VARCHAR
    description: Salesforce Opportunity identifier.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: account_id
    type: VARCHAR
    description: Salesforce Account identifier.
    update_on_merge: true
  - name: account_name
    type: VARCHAR
    description: Account display name.
    update_on_merge: true
  - name: opportunity_name
    type: VARCHAR
    description: Opportunity display name.
    update_on_merge: true
  - name: product_family
    type: VARCHAR
    description: Derived credit union product family.
    update_on_merge: true
  - name: lead_source
    type: VARCHAR
    description: Salesforce opportunity lead source.
    update_on_merge: true
  - name: credit_union_tier
    type: VARCHAR
    description: Credit union segmentation tier from Salesforce Opportunity field Credit_Union_Tier__c, such as Silver, Gold, or Platinum.
    update_on_merge: true
  - name: opportunity_test_tier
    type: VARCHAR
    description: Dashboard-compatible Opportunity tier that prefers Credit_Union_Tier__c and falls back to legacy Credit_Union_Agent_Test_Tier_June15__c when blank.
    update_on_merge: true
  - name: stage_name
    type: VARCHAR
    description: Salesforce stage name.
    update_on_merge: true
  - name: stage_group
    type: VARCHAR
    description: Derived funnel grouping for the stage.
    update_on_merge: true
  - name: amount_usd
    type: DOUBLE
    description: Opportunity amount in USD.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: probability_pct
    type: DOUBLE
    description: Salesforce probability percentage.
    update_on_merge: true
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: weighted_amount_usd
    type: DOUBLE
    description: Amount multiplied by probability in USD.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: agent_test_score
    type: VARCHAR
    description: Self-healing scenario score from Salesforce, preserved as source text.
    update_on_merge: true
  - name: close_date
    type: DATE
    description: Expected or actual close date.
    update_on_merge: true
  - name: close_month
    type: DATE
    description: First day of the opportunity close month.
    update_on_merge: true
  - name: is_closed
    type: BOOLEAN
    description: Whether the opportunity is closed.
    update_on_merge: true
  - name: is_won
    type: BOOLEAN
    description: Whether the opportunity is won.
    update_on_merge: true
  - name: activity_count
    type: INTEGER
    description: Number of tasks linked to the opportunity.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: completed_activity_count
    type: INTEGER
    description: Number of completed tasks linked to the opportunity.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: source_system_modstamp
    type: TIMESTAMP
    description: Latest Salesforce SystemModstamp among source rows used for this opportunity mart row.
    update_on_merge: true
    checks:
      - name: not_null

@bruin */

WITH changed_opportunities AS (
    SELECT DISTINCT opportunity_id
    FROM (
        {% if full_refresh %}
        SELECT id AS opportunity_id
        FROM bronze.salesforce_opportunities
        WHERE id IS NOT NULL
        {% else %}
        SELECT id AS opportunity_id
        FROM bronze.salesforce_opportunities
        WHERE system_modstamp >= TO_TIMESTAMP_NTZ('{{ start_date }}')
          AND system_modstamp < DATEADD(day, 1, TO_TIMESTAMP_NTZ('{{ end_date }}'))

        UNION ALL

        SELECT o.id AS opportunity_id
        FROM bronze.salesforce_opportunities AS o
        INNER JOIN bronze.salesforce_accounts AS a
            ON a.id = o.account_id
        WHERE a.system_modstamp >= TO_TIMESTAMP_NTZ('{{ start_date }}')
          AND a.system_modstamp < DATEADD(day, 1, TO_TIMESTAMP_NTZ('{{ end_date }}'))

        UNION ALL

        SELECT what_id AS opportunity_id
        FROM bronze.salesforce_tasks
        WHERE system_modstamp >= TO_TIMESTAMP_NTZ('{{ start_date }}')
          AND system_modstamp < DATEADD(day, 1, TO_TIMESTAMP_NTZ('{{ end_date }}'))
        {% endif %}
    )
    WHERE opportunity_id IS NOT NULL
),

opportunities AS (
    SELECT
        o.id,
        o.account_id,
        o.name,
        COALESCE(o.lead_source, 'Unknown') AS lead_source,
        NULLIF(TRIM(TO_VARCHAR(o.credit_union_tier__c)), '') AS credit_union_tier,
        COALESCE(
            NULLIF(TRIM(TO_VARCHAR(o.credit_union_tier__c)), ''),
            NULLIF(TRIM(TO_VARCHAR(o.credit_union_agent_test_tier_june15__c)), ''),
            'Unspecified'
        ) AS opportunity_test_tier,
        COALESCE(o.stage_name, 'Unknown') AS stage_name,
        COALESCE(o.amount::DOUBLE, 0) AS amount,
        COALESCE(o.probability::DOUBLE, 0) AS probability,
        TO_VARCHAR(o.credit_union_agent_test_score__c) AS agent_test_score,
        o.close_date::DATE AS close_date,
        COALESCE(o.is_closed::BOOLEAN, FALSE) AS is_closed,
        COALESCE(o.is_won::BOOLEAN, FALSE) AS is_won,
        o.system_modstamp
    FROM bronze.salesforce_opportunities AS o
    INNER JOIN changed_opportunities AS co
        ON co.opportunity_id = o.id
    WHERE o.id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY o.system_modstamp DESC) = 1
),

accounts AS (
    SELECT
        a.id,
        a.name,
        a.system_modstamp
    FROM bronze.salesforce_accounts AS a
    INNER JOIN opportunities AS o
        ON o.account_id = a.id
    WHERE a.id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.system_modstamp DESC) = 1
),

tasks AS (
    SELECT
        t.id,
        t.what_id,
        t.status,
        t.system_modstamp
    FROM bronze.salesforce_tasks AS t
    INNER JOIN changed_opportunities AS co
        ON co.opportunity_id = t.what_id
    WHERE t.id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY t.system_modstamp DESC) = 1
),

task_rollup AS (
    SELECT
        what_id AS opportunity_id,
        COUNT(*) AS activity_count,
        COUNT_IF(LOWER(status) = 'completed') AS completed_activity_count,
        MAX(system_modstamp) AS max_system_modstamp
    FROM tasks
    GROUP BY 1
)

SELECT
    o.id AS opportunity_id,
    o.account_id,
    a.name AS account_name,
    o.name AS opportunity_name,
    CASE
        WHEN LOWER(o.name) LIKE '%auto%' THEN 'Auto lending'
        WHEN LOWER(o.name) LIKE '%home equity%' OR LOWER(o.name) LIKE '%mortgage%' THEN 'Home lending'
        WHEN LOWER(o.name) LIKE '%credit card%' THEN 'Cards'
        WHEN LOWER(o.name) LIKE '%business%' THEN 'Business banking'
        WHEN LOWER(o.name) LIKE '%wellness%' THEN 'Financial wellness'
        ELSE 'Other'
    END AS product_family,
    o.lead_source,
    o.credit_union_tier,
    o.opportunity_test_tier,
    o.stage_name,
    CASE
        WHEN o.is_won THEN 'Closed won'
        WHEN o.is_closed THEN 'Closed lost'
        WHEN LOWER(o.stage_name) IN ('proposal/price quote', 'negotiation/review') THEN 'Late stage'
        WHEN LOWER(o.stage_name) IN ('qualification', 'needs analysis') THEN 'Qualified'
        ELSE 'Early stage'
    END AS stage_group,
    o.amount AS amount_usd,
    o.probability AS probability_pct,
    o.agent_test_score,
    o.amount * o.probability / 100 AS weighted_amount_usd,
    o.close_date,
    DATE_TRUNC('MONTH', o.close_date)::DATE AS close_month,
    o.is_closed,
    o.is_won,
    COALESCE(t.activity_count, 0) AS activity_count,
    COALESCE(t.completed_activity_count, 0) AS completed_activity_count,
    GREATEST(
        COALESCE(o.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(a.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(t.max_system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01'))
    ) AS source_system_modstamp
FROM opportunities AS o
LEFT JOIN accounts AS a
    ON a.id = o.account_id
LEFT JOIN task_rollup AS t
    ON t.opportunity_id = o.id
