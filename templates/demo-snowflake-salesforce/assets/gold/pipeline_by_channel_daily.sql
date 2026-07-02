/* @bruin

name: gold.pipeline_by_channel_daily
type: sf.sql
description: |
  Daily open and won pipeline by close date, lead source, credit union tier,
  and Opportunity test tier. This asset is the pipeline's time-interval materialization example:
  normal runs refresh only close dates in the Bruin interval, while full-refresh
  runs rebuild all close dates.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - lending
  - gold
  - dashboard
  - time_interval
domains:
  - crm
  - lending
meta:
  asset_grain: One row per close date, lead source, credit union tier, and Opportunity test tier.
  load_pattern: Time-interval table refresh on close_date.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: table
  strategy: time_interval
  incremental_key: close_date
  time_granularity: date

depends:
  - silver.salesforce_opportunity_pipeline

columns:
  - name: close_date
    type: DATE
    description: Expected or actual opportunity close date.
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
    description: Non-closed pipeline amount in USD for the close date and lead source.
    checks:
      - name: non_negative
  - name: won_amount_usd
    type: DOUBLE
    description: Closed-won opportunity amount in USD for the close date and lead source.
    checks:
      - name: non_negative
  - name: opportunity_count
    type: INTEGER
    description: Number of opportunities for the close date and lead source.
    checks:
      - name: non_negative
  - name: source_system_modstamp
    type: TIMESTAMP
    description: Latest source Salesforce SystemModstamp among opportunities in the daily channel bucket.
    checks:
      - name: not_null

@bruin */

SELECT
    close_date,
    lead_source,
    opportunity_test_tier,
    COALESCE(credit_union_tier, 'Unspecified') AS credit_union_tier,
    SUM(IFF(NOT is_closed, amount_usd, 0)) AS open_amount_usd,
    SUM(IFF(is_won, amount_usd, 0)) AS won_amount_usd,
    COUNT(*) AS opportunity_count,
    MAX(source_system_modstamp) AS source_system_modstamp
FROM silver.salesforce_opportunity_pipeline
WHERE close_date IS NOT NULL
{% if not full_refresh %}
  AND close_date >= TO_DATE('{{ start_date }}')
  AND close_date < TO_DATE('{{ end_date }}')
{% endif %}
GROUP BY 1, 2, 3, 4
