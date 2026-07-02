/* @bruin

name: silver.salesforce_product_pipeline
type: sf.sql
description: |
  Product-grain Salesforce pipeline mart joining Opportunities, Opportunity
  Line Items, Pricebook Entries, Products, Pricebooks, and Users for credit union
  lending, deposit, card, and financial wellness pipeline analysis.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - salesforce
  - silver
  - product_pipeline
domains:
  - crm
  - lending
meta:
  asset_grain: One row per current Salesforce OpportunityLineItem.
  load_pattern: Full rebuild over current bronze CRM product and pipeline objects.
  refresh_cadence: Daily batch pipeline.
  source_system: Salesforce Sales Cloud via bronze ingestr assets.

materialization:
  type: table
  strategy: create+replace
  cluster_by:
    - close_month
    - product_family

depends:
  - bronze.salesforce_opportunities
  - bronze.salesforce_opportunity_line_items
  - bronze.salesforce_pricebook_entries
  - bronze.salesforce_products
  - bronze.salesforce_pricebooks
  - bronze.salesforce_users

columns:
  - name: line_item_id
    type: VARCHAR
    description: Salesforce OpportunityLineItem identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: opportunity_id
    type: VARCHAR
    description: Salesforce Opportunity identifier.
  - name: account_id
    type: VARCHAR
    description: Salesforce Account identifier.
  - name: owner_id
    type: VARCHAR
    description: Salesforce User identifier for the opportunity owner.
  - name: owner_name
    type: VARCHAR
    description: Opportunity owner name.
  - name: product_id
    type: VARCHAR
    description: Salesforce Product2 identifier.
  - name: product_code
    type: VARCHAR
    description: Product code.
  - name: product_name
    type: VARCHAR
    description: Product display name.
  - name: product_family
    type: VARCHAR
    description: Product family.
  - name: pricebook_name
    type: VARCHAR
    description: Pricebook name.
  - name: credit_union_tier
    type: VARCHAR
    description: Credit union segmentation tier from Salesforce Opportunity field Credit_Union_Tier__c, such as Silver, Gold, or Platinum.
  - name: opportunity_test_tier
    type: VARCHAR
    description: Dashboard-compatible Opportunity tier that prefers Credit_Union_Tier__c and falls back to legacy Credit_Union_Agent_Test_Tier_June15__c when blank.
  - name: stage_name
    type: VARCHAR
    description: Salesforce opportunity stage.
  - name: stage_group
    type: VARCHAR
    description: Derived funnel stage group.
  - name: close_date
    type: DATE
    description: Expected or actual close date.
  - name: close_month
    type: DATE
    description: First day of the close month.
  - name: quantity
    type: DOUBLE
    description: Line item quantity.
    checks:
      - name: non_negative
  - name: unit_price_usd
    type: DOUBLE
    description: Unit price in USD.
    checks:
      - name: non_negative
  - name: line_amount_usd
    type: DOUBLE
    description: Line item amount in USD.
    checks:
      - name: non_negative
  - name: weighted_line_amount_usd
    type: DOUBLE
    description: Line item amount weighted by opportunity probability.
    checks:
      - name: non_negative
  - name: is_closed
    type: BOOLEAN
    description: Whether the opportunity is closed.
  - name: is_won
    type: BOOLEAN
    description: Whether the opportunity is closed won.
  - name: source_system_modstamp
    type: TIMESTAMP
    description: Latest Salesforce SystemModstamp across joined source rows.
    checks:
      - name: not_null

@bruin */

WITH opportunities AS (
    SELECT *
    FROM bronze.salesforce_opportunities
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

line_items AS (
    SELECT *
    FROM bronze.salesforce_opportunity_line_items
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

pricebook_entries AS (
    SELECT *
    FROM bronze.salesforce_pricebook_entries
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

products AS (
    SELECT *
    FROM bronze.salesforce_products
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

pricebooks AS (
    SELECT *
    FROM bronze.salesforce_pricebooks
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

users AS (
    SELECT *
    FROM bronze.salesforce_users
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY name) = 1
)

SELECT
    li.id AS line_item_id,
    o.id AS opportunity_id,
    o.account_id,
    o.owner_id,
    u.name AS owner_name,
    p.id AS product_id,
    p.product_code,
    COALESCE(p.name, 'Unknown product') AS product_name,
    COALESCE(p.family, 'Other') AS product_family,
    pb.name AS pricebook_name,
    NULLIF(TRIM(TO_VARCHAR(o.credit_union_tier__c)), '') AS credit_union_tier,
    COALESCE(
        NULLIF(TRIM(TO_VARCHAR(o.credit_union_tier__c)), ''),
        NULLIF(TRIM(TO_VARCHAR(o.credit_union_agent_test_tier_june15__c)), ''),
        'Unspecified'
    ) AS opportunity_test_tier,
    o.stage_name,
    CASE
        WHEN COALESCE(o.is_won::BOOLEAN, FALSE) THEN 'Closed won'
        WHEN COALESCE(o.is_closed::BOOLEAN, FALSE) THEN 'Closed lost'
        WHEN LOWER(o.stage_name) IN ('proposal/price quote', 'negotiation/review') THEN 'Late stage'
        WHEN LOWER(o.stage_name) IN ('qualification', 'needs analysis') THEN 'Qualified'
        ELSE 'Early stage'
    END AS stage_group,
    o.close_date::DATE AS close_date,
    DATE_TRUNC('MONTH', o.close_date::DATE)::DATE AS close_month,
    COALESCE(li.quantity::DOUBLE, 0) AS quantity,
    COALESCE(li.unit_price::DOUBLE, 0) AS unit_price_usd,
    COALESCE(li.total_price::DOUBLE, li.quantity::DOUBLE * li.unit_price::DOUBLE, o.amount::DOUBLE, 0) AS line_amount_usd,
    COALESCE(li.total_price::DOUBLE, li.quantity::DOUBLE * li.unit_price::DOUBLE, o.amount::DOUBLE, 0)
    * COALESCE(o.probability::DOUBLE, 0) / 100 AS weighted_line_amount_usd,
    COALESCE(o.is_closed::BOOLEAN, FALSE) AS is_closed,
    COALESCE(o.is_won::BOOLEAN, FALSE) AS is_won,
    GREATEST(
        COALESCE(o.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(li.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(pbe.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(p.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(pb.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01'))
    ) AS source_system_modstamp
FROM line_items AS li
INNER JOIN opportunities AS o
    ON o.id = li.opportunity_id
LEFT JOIN pricebook_entries AS pbe
    ON pbe.id = li.pricebook_entry_id
LEFT JOIN products AS p
    ON p.id = pbe.product2_id
LEFT JOIN pricebooks AS pb
    ON pb.id = pbe.pricebook2_id
LEFT JOIN users AS u
    ON u.id = o.owner_id
