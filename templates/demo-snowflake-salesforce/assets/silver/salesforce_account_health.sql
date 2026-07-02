/* @bruin

name: silver.salesforce_account_health
type: sf.sql
description: |
  Builds an account-level Salesforce health mart for the credit union demo by
  joining deduplicated Accounts, Contacts, Opportunities, and Tasks. Normal
  runs merge only accounts touched by Salesforce changes in the Bruin interval;
  full-refresh runs rebuild all account health rows.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - silver
  - mart
  - incremental_merge
domains:
  - crm
  - member_relationships
  - lending
meta:
  asset_grain: One row per current Salesforce Account.
  load_pattern: Incremental merge of accounts touched by source SystemModstamp in the Bruin interval.
  refresh_cadence: Daily batch pipeline.
  source_system: Salesforce Sales Cloud via bronze ingestr assets.

materialization:
  type: table
  strategy: merge
  cluster_by:
    - source_system_modstamp
    - billing_state

depends:
  - bronze.salesforce_accounts
  - bronze.salesforce_contacts
  - bronze.salesforce_opportunities
  - bronze.salesforce_tasks

columns:
  - name: account_id
    type: VARCHAR
    description: Salesforce Account identifier.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: account_number
    type: VARCHAR
    description: Account number used as the deterministic demo external key.
    update_on_merge: true
  - name: account_name
    type: VARCHAR
    description: Account display name.
    update_on_merge: true
  - name: industry
    type: VARCHAR
    description: Salesforce industry or relationship segment.
    update_on_merge: true
  - name: billing_city
    type: VARCHAR
    description: Account billing city.
    update_on_merge: true
  - name: billing_state
    type: VARCHAR
    description: Account billing state.
    update_on_merge: true
  - name: contact_count
    type: INTEGER
    description: Number of contacts linked to the account.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: opportunity_count
    type: INTEGER
    description: Number of opportunities linked to the account.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: open_opportunity_count
    type: INTEGER
    description: Number of non-closed opportunities linked to the account.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: won_opportunity_count
    type: INTEGER
    description: Number of closed-won opportunities linked to the account.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: total_pipeline_amount_usd
    type: DOUBLE
    description: Total opportunity amount in USD.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: open_pipeline_amount_usd
    type: DOUBLE
    description: Open opportunity amount in USD.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: weighted_open_pipeline_amount_usd
    type: DOUBLE
    description: Open opportunity amount weighted by Salesforce probability in USD.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: won_amount_usd
    type: DOUBLE
    description: Closed-won opportunity amount in USD.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: activity_count
    type: INTEGER
    description: Number of tasks linked to the account's opportunities.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: completed_activity_count
    type: INTEGER
    description: Number of completed tasks linked to the account's opportunities.
    update_on_merge: true
    checks:
      - name: non_negative
  - name: next_close_date
    type: DATE
    description: Earliest future close date among open opportunities.
    update_on_merge: true
  - name: dominant_stage
    type: VARCHAR
    description: Highest-value opportunity stage for the account.
    update_on_merge: true
  - name: relationship_segment
    type: VARCHAR
    description: Derived account segment based on won and open pipeline amount.
    update_on_merge: true
  - name: source_system_modstamp
    type: TIMESTAMP
    description: Latest Salesforce SystemModstamp among source rows used for this account mart row.
    update_on_merge: true
    checks:
      - name: not_null

@bruin */

WITH changed_accounts AS (
    SELECT DISTINCT account_id
    FROM (
        {% if full_refresh %}
        SELECT id AS account_id
        FROM bronze.salesforce_accounts
        WHERE id IS NOT NULL
        {% else %}
        SELECT id AS account_id
        FROM bronze.salesforce_accounts
        WHERE system_modstamp >= TO_TIMESTAMP_NTZ('{{ start_date }}')
          AND system_modstamp < DATEADD(day, 1, TO_TIMESTAMP_NTZ('{{ end_date }}'))

        UNION ALL

        SELECT account_id
        FROM bronze.salesforce_contacts
        WHERE system_modstamp >= TO_TIMESTAMP_NTZ('{{ start_date }}')
          AND system_modstamp < DATEADD(day, 1, TO_TIMESTAMP_NTZ('{{ end_date }}'))

        UNION ALL

        SELECT account_id
        FROM bronze.salesforce_opportunities
        WHERE system_modstamp >= TO_TIMESTAMP_NTZ('{{ start_date }}')
          AND system_modstamp < DATEADD(day, 1, TO_TIMESTAMP_NTZ('{{ end_date }}'))

        UNION ALL

        SELECT o.account_id
        FROM bronze.salesforce_opportunities AS o
        INNER JOIN bronze.salesforce_tasks AS t
            ON t.what_id = o.id
        WHERE t.system_modstamp >= TO_TIMESTAMP_NTZ('{{ start_date }}')
          AND t.system_modstamp < DATEADD(day, 1, TO_TIMESTAMP_NTZ('{{ end_date }}'))
        {% endif %}
    )
    WHERE account_id IS NOT NULL
),

accounts AS (
    SELECT a.*
    FROM bronze.salesforce_accounts AS a
    INNER JOIN changed_accounts AS ca
        ON ca.account_id = a.id
    WHERE a.id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.system_modstamp DESC) = 1
),

contacts AS (
    SELECT c.*
    FROM bronze.salesforce_contacts AS c
    INNER JOIN changed_accounts AS ca
        ON ca.account_id = c.account_id
    WHERE c.id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.system_modstamp DESC) = 1
),

opportunities AS (
    SELECT
        o.id,
        o.account_id,
        o.name,
        o.stage_name,
        COALESCE(o.amount::DOUBLE, 0) AS amount,
        COALESCE(o.probability::DOUBLE, 0) AS probability,
        o.close_date::DATE AS close_date,
        COALESCE(o.is_closed::BOOLEAN, FALSE) AS is_closed,
        COALESCE(o.is_won::BOOLEAN, FALSE) AS is_won,
        o.system_modstamp
    FROM bronze.salesforce_opportunities AS o
    INNER JOIN changed_accounts AS ca
        ON ca.account_id = o.account_id
    WHERE o.id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY o.system_modstamp DESC) = 1
),

tasks AS (
    SELECT t.*
    FROM bronze.salesforce_tasks AS t
    INNER JOIN opportunities AS o
        ON o.id = t.what_id
    WHERE t.id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY t.system_modstamp DESC) = 1
),

contact_rollup AS (
    SELECT
        account_id,
        COUNT(*) AS contact_count,
        MAX(system_modstamp) AS max_system_modstamp
    FROM contacts
    GROUP BY 1
),

task_rollup AS (
    SELECT
        o.account_id,
        COUNT(t.id) AS activity_count,
        COUNT_IF(LOWER(t.status) = 'completed') AS completed_activity_count,
        MAX(t.system_modstamp) AS max_system_modstamp
    FROM opportunities AS o
    LEFT JOIN tasks AS t
        ON t.what_id = o.id
    GROUP BY 1
),

stage_rank AS (
    SELECT
        account_id,
        stage_name,
        SUM(amount) AS stage_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY SUM(amount) DESC, stage_name) AS stage_rank
    FROM opportunities
    GROUP BY 1, 2
),

opportunity_rollup AS (
    SELECT
        account_id,
        COUNT(*) AS opportunity_count,
        COUNT_IF(NOT is_closed) AS open_opportunity_count,
        COUNT_IF(is_won) AS won_opportunity_count,
        SUM(amount) AS total_pipeline_amount_usd,
        SUM(IFF(NOT is_closed, amount, 0)) AS open_pipeline_amount_usd,
        SUM(IFF(NOT is_closed, amount * probability / 100, 0)) AS weighted_open_pipeline_amount_usd,
        SUM(IFF(is_won, amount, 0)) AS won_amount_usd,
        MIN(IFF(NOT is_closed AND close_date >= CURRENT_DATE(), close_date, NULL)) AS next_close_date,
        MAX(system_modstamp) AS max_system_modstamp
    FROM opportunities
    GROUP BY 1
)

SELECT
    a.id AS account_id,
    a.account_number,
    a.name AS account_name,
    a.industry,
    a.billing_city,
    a.billing_state,
    COALESCE(c.contact_count, 0) AS contact_count,
    COALESCE(o.opportunity_count, 0) AS opportunity_count,
    COALESCE(o.open_opportunity_count, 0) AS open_opportunity_count,
    COALESCE(o.won_opportunity_count, 0) AS won_opportunity_count,
    COALESCE(o.total_pipeline_amount_usd, 0) AS total_pipeline_amount_usd,
    COALESCE(o.open_pipeline_amount_usd, 0) AS open_pipeline_amount_usd,
    COALESCE(o.weighted_open_pipeline_amount_usd, 0) AS weighted_open_pipeline_amount_usd,
    COALESCE(o.won_amount_usd, 0) AS won_amount_usd,
    COALESCE(t.activity_count, 0) AS activity_count,
    COALESCE(t.completed_activity_count, 0) AS completed_activity_count,
    o.next_close_date,
    COALESCE(sr.stage_name, 'No Opportunity') AS dominant_stage,
    CASE
        WHEN COALESCE(o.won_amount_usd, 0) >= 100000 THEN 'High won value'
        WHEN COALESCE(o.open_pipeline_amount_usd, 0) >= 100000 THEN 'High open pipeline'
        WHEN COALESCE(o.open_opportunity_count, 0) > 0 THEN 'Active pipeline'
        ELSE 'Nurture'
    END AS relationship_segment,
    GREATEST(
        COALESCE(a.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(c.max_system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(o.max_system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(t.max_system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01'))
    ) AS source_system_modstamp
FROM accounts AS a
LEFT JOIN contact_rollup AS c
    ON c.account_id = a.id
LEFT JOIN opportunity_rollup AS o
    ON o.account_id = a.id
LEFT JOIN task_rollup AS t
    ON t.account_id = a.id
LEFT JOIN stage_rank AS sr
    ON sr.account_id = a.id
    AND sr.stage_rank = 1
