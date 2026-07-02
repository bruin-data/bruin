/* @bruin

name: silver.salesforce_marketing_funnel
type: sf.sql
description: |
  Campaign-member grain mart combining Salesforce Campaigns, CampaignMembers,
  Leads, Contacts, and Accounts for credit union acquisition, community outreach,
  and member conversion analysis.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - salesforce
  - silver
  - marketing
domains:
  - crm
  - marketing
meta:
  asset_grain: One row per current Salesforce CampaignMember.
  load_pattern: Full rebuild over current bronze campaign, lead, contact, and account objects.
  refresh_cadence: Daily batch pipeline.
  source_system: Salesforce Sales Cloud via bronze ingestr assets.

materialization:
  type: table
  strategy: create+replace
  cluster_by:
    - campaign_start_month
    - member_type

depends:
  - bronze.salesforce_campaigns
  - bronze.salesforce_campaign_members
  - bronze.salesforce_leads
  - bronze.salesforce_contacts
  - bronze.salesforce_accounts

columns:
  - name: campaign_member_id
    type: VARCHAR
    description: Salesforce CampaignMember identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: campaign_id
    type: VARCHAR
    description: Salesforce Campaign identifier.
  - name: campaign_name
    type: VARCHAR
    description: Campaign name.
  - name: campaign_type
    type: VARCHAR
    description: Campaign type.
  - name: campaign_status
    type: VARCHAR
    description: Campaign status.
  - name: campaign_start_date
    type: DATE
    description: Campaign start date.
  - name: campaign_start_month
    type: DATE
    description: First day of campaign start month.
  - name: member_type
    type: VARCHAR
    description: Whether the campaign member is a Lead or Contact.
  - name: member_id
    type: VARCHAR
    description: Salesforce Lead or Contact identifier.
  - name: account_id
    type: VARCHAR
    description: Related Salesforce Account identifier for contact members.
  - name: member_name
    type: VARCHAR
    description: Lead or contact display name.
  - name: lead_source
    type: VARCHAR
    description: Lead source from the member record.
  - name: campaign_member_status
    type: VARCHAR
    description: Campaign member response status.
  - name: lead_status
    type: VARCHAR
    description: Lead status when the campaign member is a lead.
  - name: responded_flag
    type: BOOLEAN
    description: Whether the campaign member has responded.
  - name: converted_flag
    type: BOOLEAN
    description: Whether the lead status indicates conversion.
  - name: source_system_modstamp
    type: TIMESTAMP
    description: Latest Salesforce SystemModstamp across joined source rows.
    checks:
      - name: not_null

@bruin */

WITH campaigns AS (
    SELECT *
    FROM bronze.salesforce_campaigns
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

campaign_members AS (
    SELECT *
    FROM bronze.salesforce_campaign_members
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

leads AS (
    SELECT *
    FROM bronze.salesforce_leads
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

contacts AS (
    SELECT *
    FROM bronze.salesforce_contacts
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
),

accounts AS (
    SELECT *
    FROM bronze.salesforce_accounts
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY system_modstamp DESC) = 1
)

SELECT
    cm.id AS campaign_member_id,
    c.id AS campaign_id,
    c.name AS campaign_name,
    c.type AS campaign_type,
    c.status AS campaign_status,
    c.start_date::DATE AS campaign_start_date,
    DATE_TRUNC('MONTH', c.start_date::DATE)::DATE AS campaign_start_month,
    IFF(cm.lead_id IS NOT NULL, 'Lead', 'Contact') AS member_type,
    COALESCE(cm.lead_id, cm.contact_id) AS member_id,
    a.id AS account_id,
    TRIM(CONCAT(COALESCE(l.first_name, ct.first_name, ''), ' ', COALESCE(l.last_name, ct.last_name, ''))) AS member_name,
    COALESCE(l.lead_source, ct.lead_source, 'Unknown') AS lead_source,
    cm.status AS campaign_member_status,
    l.status AS lead_status,
    LOWER(COALESCE(cm.status, '')) IN ('responded', 'attended', 'registered') AS responded_flag,
    LOWER(COALESCE(l.status, '')) LIKE '%converted%' AS converted_flag,
    GREATEST(
        COALESCE(cm.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(c.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(l.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(ct.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01')),
        COALESCE(a.system_modstamp, TO_TIMESTAMP_NTZ('1970-01-01'))
    ) AS source_system_modstamp
FROM campaign_members AS cm
INNER JOIN campaigns AS c
    ON c.id = cm.campaign_id
LEFT JOIN leads AS l
    ON l.id = cm.lead_id
LEFT JOIN contacts AS ct
    ON ct.id = cm.contact_id
LEFT JOIN accounts AS a
    ON a.id = ct.account_id
