/* @bruin

name: gold.campaign_conversion_funnel
type: sf.sql
description: Campaign attribution rollup for credit union member acquisition and community outreach demos.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - marketing
  - gold
  - dashboard
domains:
  - crm
  - marketing
meta:
  asset_grain: One row per campaign start month and campaign type.
  load_pattern: View over silver.salesforce_marketing_funnel.
  refresh_cadence: Daily batch pipeline.

materialization:
  type: view

depends:
  - silver.salesforce_marketing_funnel

columns:
  - name: campaign_start_month
    type: DATE
    description: First day of campaign start month.
  - name: campaign_type
    type: VARCHAR
    description: Campaign type.
  - name: campaign_count
    type: INTEGER
    description: Number of campaigns.
    checks:
      - name: non_negative
  - name: member_count
    type: INTEGER
    description: Number of campaign members.
    checks:
      - name: non_negative
  - name: lead_count
    type: INTEGER
    description: Number of lead campaign members.
    checks:
      - name: non_negative
  - name: contact_count
    type: INTEGER
    description: Number of contact campaign members.
    checks:
      - name: non_negative
  - name: response_rate
    type: DOUBLE
    description: Share of campaign members that responded.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1
  - name: converted_lead_count
    type: INTEGER
    description: Number of campaign leads marked converted.
    checks:
      - name: non_negative

@bruin */

SELECT
    campaign_start_month,
    campaign_type,
    COUNT(DISTINCT campaign_id) AS campaign_count,
    COUNT(*) AS member_count,
    COUNT_IF(member_type = 'Lead') AS lead_count,
    COUNT_IF(member_type = 'Contact') AS contact_count,
    COUNT_IF(responded_flag) / NULLIF(COUNT(*), 0) AS response_rate,
    COUNT_IF(converted_flag) AS converted_lead_count
FROM silver.salesforce_marketing_funnel
GROUP BY 1, 2
