/* @bruin

name: gold.product_pipeline_performance
type: sf.sql
description: Product and month rollup for credit union loan, card, deposit, business banking, and financial wellness pipeline.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - lending
  - gold
  - dashboard
domains:
  - crm
  - lending
meta:
  asset_grain: One row per close month, product family, product name, credit union tier, and Opportunity test tier.
  load_pattern: View over silver.salesforce_product_pipeline.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: view

depends:
  - silver.salesforce_product_pipeline

columns:
  - name: close_month
    type: DATE
    description: First day of the opportunity close month.
  - name: product_family
    type: VARCHAR
    description: Product family.
  - name: product_name
    type: VARCHAR
    description: Product name.
  - name: opportunity_test_tier
    type: VARCHAR
    description: Dashboard-compatible Opportunity tier that prefers Credit_Union_Tier__c and falls back to legacy Credit_Union_Agent_Test_Tier_June15__c when blank.
  - name: credit_union_tier
    type: VARCHAR
    description: Credit Union Tier from Salesforce Opportunity field Credit_Union_Tier__c, or Unspecified when the source field is blank.
  - name: line_item_count
    type: INTEGER
    description: Number of opportunity line items.
    checks:
      - name: non_negative
  - name: open_line_amount_usd
    type: DOUBLE
    description: Open pipeline line amount.
    checks:
      - name: non_negative
  - name: weighted_open_line_amount_usd
    type: DOUBLE
    description: Weighted open pipeline line amount.
    checks:
      - name: non_negative
  - name: won_line_amount_usd
    type: DOUBLE
    description: Closed-won line amount.
    checks:
      - name: non_negative
  - name: avg_unit_price_usd
    type: DOUBLE
    description: Average unit price.
    checks:
      - name: non_negative

@bruin */

SELECT
    close_month,
    product_family,
    product_name,
    opportunity_test_tier,
    COALESCE(credit_union_tier, 'Unspecified') AS credit_union_tier,
    COUNT(*) AS line_item_count,
    SUM(IFF(NOT is_closed, line_amount_usd, 0)) AS open_line_amount_usd,
    SUM(IFF(NOT is_closed, weighted_line_amount_usd, 0)) AS weighted_open_line_amount_usd,
    SUM(IFF(is_won, line_amount_usd, 0)) AS won_line_amount_usd,
    AVG(unit_price_usd) AS avg_unit_price_usd
FROM silver.salesforce_product_pipeline
GROUP BY 1, 2, 3, 4, 5
