/* @bruin
name: quickbooks_stage.customers
type: bq.sql
description: >
  One row per QuickBooks customer, with typed columns, flattened contact
  details, and the parent customer resolved for sub-customers (jobs). The
  open balance is the value QuickBooks reported at the last load.

materialization:
  type: table

depends:
  - quickbooks_raw.customers

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - customers

columns:
  - name: customer_id
    type: STRING
    description: QuickBooks customer identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: customer_name
    type: STRING
    description: Customer display name, unique within the company file.
    checks:
      - name: not_null
  - name: company_name
    type: STRING
    description: Legal or company name of the customer.
  - name: contact_name
    type: STRING
    description: Primary contact's given and family name.
  - name: email
    type: STRING
    description: Primary email address.
  - name: phone
    type: STRING
    description: Primary phone number as entered.
  - name: website
    type: STRING
    description: Customer website.
  - name: billing_city
    type: STRING
    description: City of the billing address.
  - name: billing_region
    type: STRING
    description: State, province, or region code of the billing address.
  - name: billing_country
    type: STRING
    description: Country of the billing address.
  - name: parent_customer_id
    type: STRING
    description: Parent customer of a sub-customer or job; null for top-level customers.
  - name: is_sub_customer
    type: BOOL
    description: Whether the customer is a sub-customer or job of another customer.
  - name: payment_terms
    type: STRING
    description: Default payment terms, for example `Net 30`.
  - name: currency
    type: STRING
    description: Customer currency code, for example `USD`.
  - name: open_balance
    type: NUMERIC
    description: >
      Open receivable balance for the customer, excluding sub-customers, at
      the last load.
  - name: open_balance_with_sub_customers
    type: NUMERIC
    description: Open receivable balance including sub-customers, at the last load.
  - name: is_active
    type: BOOL
    description: Whether the customer is active in QuickBooks.
  - name: is_taxable
    type: BOOL
    description: Whether sales to the customer are taxable by default.
  - name: notes
    type: STRING
    description: Free-form internal notes on the customer.
  - name: created_at
    type: TIMESTAMP
    description: When the customer was created in QuickBooks.
  - name: updated_at
    type: TIMESTAMP
    description: When the customer was last modified in QuickBooks.
    checks:
      - name: not_null
@bruin */

SELECT
  id AS customer_id,
  display_name AS customer_name,
  company_name,
  NULLIF(TRIM(CONCAT(IFNULL(given_name, ''), ' ', IFNULL(family_name, ''))), '') AS contact_name,
  LOWER(JSON_VALUE(primary_email_addr, '$.Address')) AS email,
  JSON_VALUE(primary_phone, '$.FreeFormNumber') AS phone,
  JSON_VALUE(web_addr, '$.URI') AS website,
  JSON_VALUE(bill_addr, '$.City') AS billing_city,
  JSON_VALUE(bill_addr, '$.CountrySubDivisionCode') AS billing_region,
  JSON_VALUE(bill_addr, '$.Country') AS billing_country,
  JSON_VALUE(parent_ref, '$.value') AS parent_customer_id,
  IFNULL(job, FALSE) AS is_sub_customer,
  JSON_VALUE(sales_term_ref, '$.name') AS payment_terms,
  JSON_VALUE(currency_ref, '$.value') AS currency,
  ROUND(CAST(balance AS NUMERIC), 2) AS open_balance,
  ROUND(CAST(balance_with_jobs AS NUMERIC), 2) AS open_balance_with_sub_customers,
  IFNULL(active, TRUE) AS is_active,
  IFNULL(taxable, FALSE) AS is_taxable,
  notes,
  TIMESTAMP(JSON_VALUE(meta_data, '$.CreateTime')) AS created_at,
  lastupdatedtime AS updated_at
FROM quickbooks_raw.customers;
