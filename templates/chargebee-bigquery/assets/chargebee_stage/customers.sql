/* @bruin
name: chargebee_stage.customers
type: bq.sql
description: >
  Conformed Chargebee billing customers. Unix timestamps are cast to TIMESTAMP,
  the acquisition channel and company are surfaced for segmentation, and a
  lower-cased email domain is derived so this table can later be joined to CRM
  accounts (HubSpot/Salesforce) and to Stripe customers on a shared account
  grain. `source_system` is stamped on every row so this model can be unioned
  with equivalent models from other billing systems. Deleted customers are
  filtered out. Amounts stay in native-currency minor units (cents).

materialization:
  type: table

depends:
  - chargebee_raw.customer

tags:
  - chargebee_stage
  - chargebee
  - billing
  - customers

columns:
  - name: chargebee_customer_id
    type: STRING
    description: Chargebee customer identifier and natural key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: source_system
    type: STRING
    description: Constant `chargebee`, so this model can be unioned across billing sources.
    checks:
      - name: not_null
  - name: customer_name
    type: STRING
    description: Full name (first + last) when present, otherwise the company name.
  - name: company_name
    type: STRING
    description: Company attached to the customer, a natural CRM account join key.
  - name: email
    type: STRING
    description: Billing email, kept for operational lookups and identity matching.
  - name: email_domain
    type: STRING
    description: >
      Lower-cased domain parsed from the email, used for account-level matching
      to CRM and other sources. Null when there is no email.
  - name: acquisition_channel
    type: STRING
    description: Channel the customer was acquired through, from Chargebee `channel`.
  - name: preferred_currency_code
    type: STRING
    description: Three-letter ISO currency the customer is billed in.
  - name: auto_collection
    type: STRING
    description: Whether payments are auto-collected, one of `on` or `off`.
  - name: net_term_days
    type: INT64
    description: Number of days within which an invoice must be paid.
  - name: customer_mrr_minor
    type: NUMERIC
    description: >
      Chargebee-computed MRR for the customer in native-currency minor units.
      It is a run rate, not recognized revenue.
  - name: customer_created_at
    type: TIMESTAMP
    description: When the customer was created in Chargebee.
  - name: customer_updated_at
    type: TIMESTAMP
    description: When the customer was last modified in Chargebee.
@bruin */

SELECT
  id AS chargebee_customer_id,
  'chargebee' AS source_system,
  COALESCE(
    NULLIF(TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))), ''),
    company
  ) AS customer_name,
  company AS company_name,
  email,
  LOWER(REGEXP_EXTRACT(email, r'@(.+)$')) AS email_domain,
  channel AS acquisition_channel,
  preferred_currency_code,
  auto_collection,
  SAFE_CAST(net_term_days AS INT64) AS net_term_days,
  SAFE_CAST(mrr AS NUMERIC) AS customer_mrr_minor,
  TIMESTAMP_SECONDS(SAFE_CAST(created_at AS INT64)) AS customer_created_at,
  TIMESTAMP_SECONDS(SAFE_CAST(updated_at AS INT64)) AS customer_updated_at
FROM chargebee_raw.customer
WHERE NOT COALESCE(deleted, FALSE);
