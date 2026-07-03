/* @bruin

name: bronze.salesforce_campaign_members
type: sf.sql
description: |
  Source-shaped empty CampaignMember bronze table. The current Salesforce org
  has CampaignMember query support but no seeded rows because Campaign is not
  createable for this user, and the ingestr connector fails on empty
  CampaignMember extracts with snake_case incremental-key casing.
connection: snowflake-default
tags:
  - finance
  - credit_union
  - crm
  - salesforce
  - bronze
  - source_shim
domains:
  - crm
  - marketing
meta:
  asset_grain: Empty source-shaped table for Salesforce CampaignMember.
  load_pattern: Create-or-replace empty table until CampaignMember rows are available in the org.
  source_object: CampaignMember
  source_system: Salesforce Sales Cloud

materialization:
  type: table
  strategy: create+replace

depends:
  - bronze.salesforce_campaigns

columns:
  - name: id
    type: VARCHAR
    description: Salesforce CampaignMember identifier.
    primary_key: true
  - name: campaign_id
    type: VARCHAR
    description: Related Campaign identifier.
  - name: contact_id
    type: VARCHAR
    description: Related Contact identifier when campaign member is a contact.
  - name: lead_id
    type: VARCHAR
    description: Related Lead identifier when campaign member is a lead.
  - name: status
    type: VARCHAR
    description: Campaign member response status.
  - name: created_date
    type: TIMESTAMP
    description: CampaignMember creation timestamp.
  - name: last_modified_date
    type: TIMESTAMP
    description: CampaignMember last modification timestamp.
  - name: system_modstamp
    type: TIMESTAMP
    description: Salesforce system modification timestamp.

@bruin */

SELECT
    NULL::VARCHAR AS id,
    NULL::VARCHAR AS campaign_id,
    NULL::VARCHAR AS contact_id,
    NULL::VARCHAR AS lead_id,
    NULL::VARCHAR AS status,
    NULL::TIMESTAMP_NTZ AS created_date,
    NULL::TIMESTAMP_NTZ AS last_modified_date,
    NULL::TIMESTAMP_NTZ AS system_modstamp
WHERE FALSE
