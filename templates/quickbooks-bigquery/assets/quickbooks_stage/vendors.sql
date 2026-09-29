/* @bruin
name: quickbooks_stage.vendors
type: bq.sql
description: >
  One row per QuickBooks vendor, with typed columns and flattened contact
  details. The open balance is the accounts payable balance QuickBooks
  reported at the last load.

materialization:
  type: table

depends:
  - quickbooks_raw.vendors

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - vendors

columns:
  - name: vendor_id
    type: STRING
    description: QuickBooks vendor identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: vendor_name
    type: STRING
    description: Vendor display name, unique within the company file.
    checks:
      - name: not_null
  - name: company_name
    type: STRING
    description: Legal or company name of the vendor.
  - name: email
    type: STRING
    description: Primary email address.
  - name: phone
    type: STRING
    description: Primary phone number as entered.
  - name: website
    type: STRING
    description: Vendor website.
  - name: account_number
    type: STRING
    description: Your account number with the vendor.
  - name: is_1099_vendor
    type: BOOL
    description: Whether the vendor is tracked for 1099 reporting.
  - name: payment_terms
    type: STRING
    description: Default payment terms for bills from the vendor.
  - name: currency
    type: STRING
    description: Vendor currency code, for example `USD`.
  - name: open_balance
    type: NUMERIC
    description: Open accounts payable balance owed to the vendor at the last load.
  - name: is_active
    type: BOOL
    description: Whether the vendor is active in QuickBooks.
  - name: created_at
    type: TIMESTAMP
    description: When the vendor was created in QuickBooks.
  - name: updated_at
    type: TIMESTAMP
    description: When the vendor was last modified in QuickBooks.
    checks:
      - name: not_null
@bruin */

SELECT
  id AS vendor_id,
  display_name AS vendor_name,
  company_name,
  LOWER(JSON_VALUE(primary_email_addr, '$.Address')) AS email,
  JSON_VALUE(primary_phone, '$.FreeFormNumber') AS phone,
  JSON_VALUE(web_addr, '$.URI') AS website,
  acct_num AS account_number,
  IFNULL(vendor1099, FALSE) AS is_1099_vendor,
  JSON_VALUE(term_ref, '$.name') AS payment_terms,
  JSON_VALUE(currency_ref, '$.value') AS currency,
  ROUND(CAST(balance AS NUMERIC), 2) AS open_balance,
  IFNULL(active, TRUE) AS is_active,
  TIMESTAMP(JSON_VALUE(meta_data, '$.CreateTime')) AS created_at,
  lastupdatedtime AS updated_at
FROM quickbooks_raw.vendors;
