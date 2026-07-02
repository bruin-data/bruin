/* @bruin

name: gold.pipeline_by_channel_monthly
type: sf.sql
description: |
  Monthly open and won pipeline by lead source, credit union tier, and
  Opportunity test tier for the Credit Union Salesforce demo dashboard.
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
  asset_grain: One row per close month, lead source, credit union tier, and Opportunity test tier.
  load_pattern: Full table rebuild over current silver opportunity mart.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: table
  strategy: create+replace

depends:
  - silver.salesforce_opportunity_pipeline

columns:
  - name: close_month
    type: DATE
    description: First day of the opportunity close month.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: lead_source
    type: VARCHAR
    description: Salesforce lead source.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: opportunity_test_tier
    type: VARCHAR
    description: Dashboard-compatible Opportunity tier that prefers Credit_Union_Tier__c and falls back to legacy Credit_Union_Agent_Test_Tier_June15__c when blank.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: credit_union_tier
    type: VARCHAR
    description: Credit Union Tier from Salesforce Opportunity field Credit_Union_Tier__c, or Unspecified when the source field is blank.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: open_amount_usd
    type: DOUBLE
    description: Non-closed pipeline amount in USD.
    checks:
      - name: non_negative
  - name: won_amount_usd
    type: DOUBLE
    description: Closed-won opportunity amount in USD.
    checks:
      - name: non_negative
  - name: opportunity_count
    type: INTEGER
    description: Number of opportunities.
    checks:
      - name: non_negative
  - name: source_system_modstamp
    type: TIMESTAMP
    description: Latest source Salesforce SystemModstamp among opportunities in the monthly channel bucket.
    checks:
      - name: not_null

@bruin */

SELECT
    close_month,
    lead_source,
    opportunity_test_tier,
    COALESCE(credit_union_tier, 'Unspecified') AS credit_union_tier,
    SUM(IFF(NOT is_closed, amount_usd, 0)) AS open_amount_usd,
    SUM(IFF(is_won, amount_usd, 0)) AS won_amount_usd,
    COUNT(*) AS opportunity_count,
    MAX(source_system_modstamp) AS source_system_modstamp
FROM silver.salesforce_opportunity_pipeline
WHERE close_month IS NOT NULL
GROUP BY 1, 2, 3, 4
